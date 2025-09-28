# process_queue.py
import re
import os
import time
import configparser
import requests
import hashlib
import sys
from datetime import datetime, timezone
from concurrent.futures import ProcessPoolExecutor

from bs4 import BeautifulSoup
from supabase import create_client, Client
from sudachipy import tokenizer, dictionary

# --- Globals for worker processes ---
_WORKER_TOKENIZER = None
_POS_CACHE = {}

# --- Helper Functions ---
def get_content_hash(content: bytes) -> str:
    # Calculates the SHA256 hash of the content.
    return hashlib.sha256(content).hexdigest()

def clean_text(text: str) -> str:
    # Cleans and normalizes whitespace in a string.
    if not text: return ""
    return re.sub(r'\s+', ' ', text).strip()

def get_text_from_html(content: bytes) -> str:
    # Extracts plain text from HTML content.
    try:
        soup = BeautifulSoup(content, 'html.parser')
        for s in soup(['script', 'style']): s.decompose()
        text = ' '.join(soup.stripped_strings)
        return clean_text(text)
    except Exception as e:
        raise RuntimeError(f"HTML parsing error: {e}")

def analyze_with_sudachi(text: str, tokenizer_obj, debug_mode: bool = False) -> list:
    # Analyzes text with SudachiPy to find Out-of-Vocabulary (OOV) nouns.
    if not text.strip() or not tokenizer_obj: return []
    
    chunk_size = 10000  # Safe character chunk size to not exceed byte limit.
    words = []

    try:
        for i in range(0, len(text), chunk_size):
            chunk = text[i:i + chunk_size]
            morphemes = tokenizer_obj.tokenize(chunk)
            
            for m in morphemes:
                pos_info = m.part_of_speech()
                pos_tuple = tuple(pos_info)
                pos_id = _POS_CACHE.get(pos_tuple)
                is_oov = m.is_oov()
                is_target_pos = pos_info[0] == "名詞" and pos_info[1] == "普通名詞"

                if debug_mode:
                    print(
                        f"[DEBUG] word: {m.surface():<15} | "
                        f"is_oov: {str(is_oov):<5} | "
                        f"is_target_pos: {str(is_target_pos):<5} | "
                        f"pos: {pos_info} | "
                        f"pos_id: {pos_id}",
                        file=sys.stderr
                    )

                if is_oov and is_target_pos and len(m.surface()) > 1:
                    if pos_id is not None:
                        words.append({"word": m.surface(), "pos_id": pos_id})
    except Exception as e:
        print(f"  [!] Sudachi analysis error: {e}", file=sys.stderr)

    return words

def worker_process_url(queue_item: dict, supabase_url: str, supabase_key: str, stop_words_set: set, request_timeout: int, config_path: str, pos_cache: dict, debug_mode: bool, request_delay: float):
    # Main worker function to process a single URL.
    global _WORKER_TOKENIZER, _POS_CACHE
    if _WORKER_TOKENIZER is None:
        # Initialize tokenizer and POS cache once per worker process.
        _WORKER_TOKENIZER = dictionary.Dictionary(config_path=config_path).create(mode=tokenizer.Tokenizer.SplitMode.C)
        _POS_CACHE = pos_cache

    url_id, url = queue_item['id'], queue_item['url']
    supabase: Client = create_client(supabase_url, supabase_key)

    try:
        # Add a polite delay before each request.
        time.sleep(request_delay)

        headers = {'User-Agent': 'Mozilla/5.0'}
        response = requests.get(url, timeout=request_timeout, headers=headers)
        response.raise_for_status()

        content_type = response.headers.get("content-type", "").lower()
        if "html" not in content_type:
            supabase.table("crawl_queue").update({"status": "completed", "processed_at": datetime.now(timezone.utc).isoformat()}).eq("id", url_id).execute()
            return True

        new_hash = get_content_hash(response.content)
        db_res = supabase.table("crawl_queue").select("content_hash").eq("id", url_id).single().execute()
        old_hash = db_res.data.get("content_hash") if db_res.data else None
        
        if old_hash == new_hash:
            supabase.table("crawl_queue").update({"status": "completed", "processed_at": datetime.now(timezone.utc).isoformat()}).eq("id", url_id).execute()
            return True
        
        text = get_text_from_html(response.content)
        
        if text:
            new_words = analyze_with_sudachi(text, _WORKER_TOKENIZER, debug_mode)
            filtered_words = [w for w in new_words if w["word"] not in stop_words_set]
            if filtered_words:
                # Delete old occurrences before inserting new ones.
                supabase.table("word_occurrences").delete().eq("source_url", url).execute()
                for word_data in filtered_words:
                    upsert_res = supabase.table("unique_words").upsert(
                        {"word": word_data["word"], "pos_id": word_data["pos_id"]},
                        on_conflict="word"
                    ).execute()
                    if upsert_res.data:
                        word_id = upsert_res.data[0]['id']
                        supabase.table("word_occurrences").insert({
                            "word_id": word_id, "source_url": url
                        }).execute()

        supabase.table("crawl_queue").update({
            "status": "completed", "content_hash": new_hash, "processed_at": datetime.now(timezone.utc).isoformat()
        }).eq("id", url_id).execute()
        return True

    except requests.exceptions.HTTPError as http_err:
        status_code = http_err.response.status_code
        print(f"  [!] HTTP Error: {url} - Status: {status_code}", file=sys.stderr)
        update_payload = {"processed_at": datetime.now(timezone.utc).isoformat(), "error_message": f"HTTP Error {status_code}"}
        # Permanent errors (4xx) are marked 'completed' to prevent retries.
        if 400 <= status_code < 500:
            update_payload["status"] = "completed"
        # Transient errors (5xx) are marked 'failed' for future retries.
        else:
            update_payload["status"] = "failed"
        supabase.table("crawl_queue").update(update_payload).eq("id", url_id).execute()
        return False

    except Exception as e:
        # Other errors (e.g., connection issues) are marked 'failed'.
        print(f"  [!] Unknown Error: {url} - {e}", file=sys.stderr)
        supabase.table("crawl_queue").update({
            "status": "failed",
            "processed_at": datetime.now(timezone.utc).isoformat(),
            "error_message": str(e)
        }).eq("id", url_id).execute()
        return False

def load_pos_master_to_cache(supabase_client: Client) -> dict:
    # Loads the entire POS master table into an in-memory dictionary for fast lookups.
    pos_cache = {}
    print("[*] Loading POS master data from DB...")
    try:
        response = supabase_client.table("pos_master").select("id,pos1,pos2,pos3,pos4,pos5,pos6").execute()
        for item in response.data:
            key = tuple(item.get(f'pos{i}', '*') for i in range(1, 7))
            pos_cache[key] = item['id']
        print(f"  [+] Cached {len(pos_cache)} POS entries.")
    except Exception as e:
        print(f"  [!] Failed to load POS master: {e}", file=sys.stderr)
    return pos_cache

def main():
    # --- Read Configuration ---
    config = configparser.ConfigParser()
    config.read('config.ini')
    max_workers = config.getint('Processor', 'MAX_WORKERS')
    process_batch_size = config.getint('Processor', 'PROCESS_BATCH_SIZE')
    request_timeout = config.getint('General', 'REQUEST_TIMEOUT')
    target_accesses_per_minute = config.getint('RateLimit', 'PROCESS_ACCESSES_PER_MINUTE')
    request_delay = config.getfloat('RateLimit', 'REQUEST_DELAY_SECONDS', fallback=1.0)
    debug_mode = config.getboolean('Debug', 'PROCESSOR_DEBUG', fallback=False)

    # --- Initialize Clients ---
    supabase_url, supabase_key = os.environ.get("SUPABASE_URL"), os.environ.get("SUPABASE_KEY")
    if not supabase_url or not supabase_key: raise ValueError("Supabase credentials not set in environment.")
    
    sudachi_config_path = os.environ.get("SUDACHI_CONFIG_PATH")
    supabase_main = create_client(supabase_url, supabase_key)
    print("--- Content Processor Started ---")

    # --- Prepare Resources ---
    pos_cache_for_workers = load_pos_master_to_cache(supabase_main)
    response = supabase_main.table("stop_words").select("word").execute()
    stop_words_set = {item['word'] for item in response.data}
    print(f"[*] Loaded {len(stop_words_set)} stop words.")

    # --- Main Processing Loop ---
    total_processed_count = 0
    total_to_process = 0
    try:
        count_res = supabase_main.table("crawl_queue").select("id", count='exact').in_("status", ["queued", "failed"]).execute()
        total_to_process = count_res.count
    except Exception:
        pass # Ignore if count fails, will proceed without percentage.
    
    if total_to_process == 0:
        print("[*] No URLs in queue to process. Exiting.")
        return
        
    print(f"[*] Found {total_to_process} URLs to process in queue.")
    
    while True:
        batch_start_time = time.time()
        response = supabase_main.table("crawl_queue").select("id, url").in_("status", ["queued", "failed"]).limit(process_batch_size).execute()
        urls_to_process = response.data
        if not urls_to_process:
            print("[*] Queue is empty. Finishing processing.")
            break
        
        processing_ids = [item['id'] for item in urls_to_process]
        try:
            supabase_main.table("crawl_queue").update({"status": "processing"}).in_("id", processing_ids).execute()
        except Exception as e:
            print(f"  [!] DB Error while locking URLs: {e}", file=sys.stderr)
            continue

        with ProcessPoolExecutor(max_workers=max_workers) as executor:
            futures = [executor.submit(worker_process_url, item, supabase_url, supabase_key, stop_words_set, request_timeout, sudachi_config_path, pos_cache_for_workers, debug_mode, request_delay) for item in urls_to_process]
            results = [f.result() for f in futures]
        
        success_count = sum(1 for r in results if r)
        batch_count = len(results)
        total_processed_count += batch_count
        progress_percent = (total_processed_count / total_to_process) * 100 if total_to_process > 0 else 0
        
        print(
            f"  [+] Batch complete (Success: {success_count}, Fail: {batch_count - success_count}) | "
            f"Total: {total_processed_count} / {total_to_process} ({progress_percent:.1f}%)"
        )
        
        elapsed_time = time.time() - batch_start_time
        if target_accesses_per_minute > 0:
            required_time = (60.0 / target_accesses_per_minute) * batch_count
            if elapsed_time < required_time:
                wait_time = required_time - elapsed_time
                print(f"  [*] Rate limiting: waiting for {wait_time:.2f} seconds.")
                time.sleep(wait_time)
    
    print(f"\n--- Content Processor Finished ---")
    print(f"Total URLs processed in this run: {total_processed_count}")

if __name__ == "__main__":
    main()
