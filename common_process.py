# common_process.py
import os, sys, re, time, configparser, hashlib
from datetime import datetime, timezone
from concurrent.futures import ProcessPoolExecutor
from supabase import create_client, Client
from postgrest.exceptions import APIError
import httpx

def worker_analyze_text(tool_name, analyze_fn, init_fn, text_item, supabase_url, supabase_key, stop_words_set, db_write_chunk_size):
    """
    Generic worker function. It takes the specific analysis and init functions as arguments.
    """
    init_fn() # Ensures the model is loaded in the worker process
    text_id, crawl_queue_id, text_to_analyze = text_item['id'], text_item['crawl_queue_id'], text_item['sentence_text']
    supabase = create_client(supabase_url, supabase_key)
    max_db_retries = 3
    for attempt in range(max_db_retries):
        try:
            url_res = supabase.table("crawl_queue").select("url").eq("id", crawl_queue_id).single().execute()
            source_url = url_res.data['url'] if url_res.data else f"unknown_url_for_crawl_id_{crawl_queue_id}"
            new_words = analyze_fn(text_to_analyze)
            if new_words:
                sanitized_words = [w for w in (d.update({'word': d['word'].replace('\x00', '')}) or d for d in new_words) if w['word'] and w['word'] not in stop_words_set]
                if sanitized_words:
                    supabase.table("word_occurrences").delete().eq("source_url", source_url).execute()
                    for i in range(0, len(sanitized_words), db_write_chunk_size):
                        chunk = sanitized_words[i:i + db_write_chunk_size]
                        upsert_res = supabase.table("unique_words").upsert(chunk, on_conflict="word, source_tool").execute()
                        if not upsert_res.data: continue
                        word_texts_chunk = [w['word'] for w in chunk]
                        select_res = supabase.table("unique_words").select("id, word").in_("word", word_texts_chunk).eq("source_tool", tool_name).execute()
                        word_to_id_map = {item['word']: item['id'] for item in select_res.data}
                        if word_to_id_map:
                            occurrences = [{"word_id": word_id, "source_url": source_url} for word, word_id in word_to_id_map.items() if word in word_to_id_map]
                            if occurrences: supabase.table("word_occurrences").upsert(occurrences, on_conflict="word_id, source_url").execute()
            supabase.table("sentence_queue").update({f"{tool_name}_status": "completed"}).eq("id", text_id).execute()
            return True
        except (APIError, httpx.ConnectError) as e:
            is_retryable = False
            if isinstance(e, APIError) and (e.code == '40P01' or (hasattr(e, 'code') and str(e.code).startswith('5'))): is_retryable = True
            elif isinstance(e, httpx.ConnectError): is_retryable = True
            if is_retryable and attempt < max_db_retries - 1:
                wait_time = (attempt + 1) * 2; print(f"  [!] DB/Network Error. Retrying in {wait_time}s... ({e})", file=sys.stderr); time.sleep(wait_time); supabase = create_client(supabase_url, supabase_key); continue
            else: print(f"  [!] Unretryable DB/Network error: {e}", file=sys.stderr); supabase.table("sentence_queue").update({f"{tool_name}_status": "failed"}).eq("id", text_id).execute(); return False
        except Exception as e:
            print(f"  [!] {tool_name.capitalize()} worker unexpected error: {e}", file=sys.stderr); supabase.table("sentence_queue").update({f"{tool_name}_status": "failed"}).eq("id", text_id).execute(); return False
    print(f"  [!!!] {tool_name.capitalize()} worker failed after {max_db_retries} retries.", file=sys.stderr); supabase.table("sentence_queue").update({f"{tool_name}_status": "failed"}).eq("id", text_id).execute(); return False

def run_processor(tool_name: str, analyze_fn, init_fn):
    """
    A generic main function to run any NLP processor.
    """
    config = configparser.ConfigParser(); config.read('config.ini')
    config_section = f"{tool_name.capitalize()}_Processor"
    max_workers = config.getint(config_section, 'MAX_WORKERS')
    batch_size = config.getint(config_section, 'BATCH_SIZE')
    db_write_chunk_size = config.getint(config_section, 'DB_WRITE_CHUNK_SIZE')
    supabase_url, supabase_key = os.environ.get("SUPABASE_URL"), os.environ.get("SUPABASE_KEY")
    supabase = create_client(supabase_url, supabase_key)
    print(f"--- {tool_name.capitalize()} Content Processor Started ---")
    try:
        response = supabase.table("stop_words").select("word").execute(); stop_words_set = {item['word'] for item in response.data}
        print(f"[*] Loaded {len(stop_words_set)} stop words.")
    except Exception as e: print(f"[!] Could not load stop words: {e}", file=sys.stderr); stop_words_set = set()
    max_retries = 3
    while True:
        res = None
        for attempt in range(max_retries):
            try:
                res = supabase.table("sentence_queue").select("id, sentence_text, crawl_queue_id").eq(f"{tool_name}_status", "queued").limit(batch_size).execute(); break
            except Exception as e:
                if attempt < max_retries - 1: time.sleep(5 * (attempt + 1))
                else: print(f"[!!!] DB query failed for {tool_name}. Exiting.", file=sys.stderr); return
        if not res or not res.data: print(f"[*] No texts in queue for {tool_name} to process. Exiting."); break
        ids = [item['id'] for item in res.data]
        try:
            supabase.table("sentence_queue").update({f"{tool_name}_status": "processing"}).in_("id", ids).execute()
            print(f"[*] Locked {len(res.data)} texts for {tool_name} processing.")
        except Exception as e: print(f"  [!] DB lock failed: {e}", file=sys.stderr); time.sleep(5); continue
        with ProcessPoolExecutor(max_workers=max_workers) as executor:
            futures = [executor.submit(worker_analyze_text, tool_name, analyze_fn, init_fn, item, supabase_url, supabase_key, stop_words_set, db_write_chunk_size) for item in res.data]
            results = [f.result() for f in futures]
            success_count = sum(1 for r in results if r)
            print(f"  [+] Batch complete (Success: {success_count}, Fail: {len(results) - success_count})")
    print(f"--- {tool_name.capitalize()} Content Processor Finished ---")