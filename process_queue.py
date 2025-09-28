# process_queue.py
import re
import os
import time
import configparser
import requests
import hashlib
import sys
from datetime import datetime, timezone

from bs4 import BeautifulSoup
from supabase import create_client, Client
from sudachipy import tokenizer, dictionary

# --- グローバル変数 ---
_POS_CACHE = {}

# --- ヘルパー関数群 ---
def get_content_hash(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()

def clean_text(text: str) -> str:
    if not text: return ""
    return re.sub(r'\s+', ' ', text).strip()

def get_text_from_html(content: bytes) -> str:
    try:
        soup = BeautifulSoup(content, 'html.parser')
        for s in soup(['script', 'style']): s.decompose()
        text = ' '.join(soup.stripped_strings)
        return clean_text(text)
    except Exception as e:
        raise RuntimeError(f"HTML解析エラー: {e}")

def analyze_with_sudachi(text: str, tokenizer_obj, debug_mode: bool = False) -> list:
    if not text.strip() or not tokenizer_obj: return []
    
    chunk_size = 10000
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
        print(f"  [!] Sudachi解析エラー: {e}", file=sys.stderr)
    return words

def process_single_url(queue_item: dict, supabase_client: Client, stop_words_set: set, request_timeout: int, tokenizer_obj, debug_mode: bool):
    """単一URLを処理する逐次実行用の関数"""
    url_id, url = queue_item['id'], queue_item['url']
    
    try:
        headers = {'User-Agent': 'Mozilla/5.0'}
        response = requests.get(url, timeout=request_timeout, headers=headers)
        response.raise_for_status()

        content_type = response.headers.get("content-type", "").lower()
        if "html" not in content_type:
            supabase_client.table("crawl_queue").update({"status": "completed", "processed_at": datetime.now(timezone.utc).isoformat()}).eq("id", url_id).execute()
            return True

        new_hash = get_content_hash(response.content)
        db_res = supabase_client.table("crawl_queue").select("content_hash").eq("id", url_id).single().execute()
        old_hash = db_res.data.get("content_hash") if db_res.data else None
        
        if old_hash == new_hash:
            supabase_client.table("crawl_queue").update({"status": "completed", "processed_at": datetime.now(timezone.utc).isoformat()}).eq("id", url_id).execute()
            return True
        
        text = get_text_from_html(response.content)
        
        if text:
            new_words = analyze_with_sudachi(text, tokenizer_obj, debug_mode)
            filtered_words = [w for w in new_words if w["word"] not in stop_words_set]
            if filtered_words:
                supabase_client.table("word_occurrences").delete().eq("source_url", url).execute()
                for word_data in filtered_words:
                    upsert_res = supabase_client.table("unique_words").upsert(
                        {"word": word_data["word"], "pos_id": word_data["pos_id"]},
                        on_conflict="word"
                    ).execute()
                    if upsert_res.data:
                        word_id = upsert_res.data[0]['id']
                        supabase_client.table("word_occurrences").insert({
                            "word_id": word_id, "source_url": url
                        }).execute()

        supabase_client.table("crawl_queue").update({
            "status": "completed", "content_hash": new_hash, "processed_at": datetime.now(timezone.utc).isoformat()
        }).eq("id", url_id).execute()
        return True

    except requests.exceptions.HTTPError as http_err:
        status_code = http_err.response.status_code
        print(f"  [!] HTTPエラー発生: {url} - Status: {status_code}", file=sys.stderr)
        update_payload = {"processed_at": datetime.now(timezone.utc).isoformat(), "error_message": f"HTTP Error {status_code}"}
        if 400 <= status_code < 500:
            update_payload["status"] = "completed"
        else:
            update_payload["status"] = "failed"
        supabase_client.table("crawl_queue").update(update_payload).eq("id", url_id).execute()
        return False
    except Exception as e:
        print(f"  [!] 不明なエラー発生: {url} - {e}", file=sys.stderr)
        supabase_client.table("crawl_queue").update({
            "status": "failed",
            "processed_at": datetime.now(timezone.utc).isoformat(),
            "error_message": str(e)
        }).eq("id", url_id).execute()
        return False

def load_pos_master_to_cache(supabase_client: Client):
    """DBからpos_masterを読み込み、高速検索用のキャッシュを作成する"""
    global _POS_CACHE
    if _POS_CACHE: return
    print("[*] 品詞マスターデータをDBから読み込んでいます...")
    try:
        response = supabase_client.table("pos_master").select("id,pos1,pos2,pos3,pos4,pos5,pos6").execute()
        for item in response.data:
            key = tuple(item.get(f'pos{i}', '*') for i in range(1, 7))
            _POS_CACHE[key] = item['id']
        print(f"  [+] {len(_POS_CACHE)}件の品詞データをキャッシュしました。")
    except Exception as e:
        print(f"  [!] 品詞マスターの読み込みに失敗: {e}", file=sys.stderr)

def main():
    config = configparser.ConfigParser()
    config.read('config.ini')
    
    process_batch_size = config.getint('Processor', 'PROCESS_BATCH_SIZE')
    request_timeout = config.getint('General', 'REQUEST_TIMEOUT')
    debug_mode = config.getboolean('Debug', 'PROCESSOR_DEBUG', fallback=False)

    supabase_url, supabase_key = os.environ.get("SUPABASE_URL"), os.environ.get("SUPABASE_KEY")
    if not supabase_url or not supabase_key: raise ValueError("環境変数を設定してください。")
    
    supabase_main = create_client(supabase_url, supabase_key)
    print("--- シングルコネクション・コンテンツ解析処理開始 ---")

    # Sudachiの初期化 (ユーザー辞書は使わない前提)
    print(f"[*] Sudachi Tokenizerを初期化します (dict: full)。")
    tokenizer_obj = dictionary.Dictionary(dict="full").create(mode=tokenizer.Tokenizer.SplitMode.C)

    load_pos_master_to_cache(supabase_main)
    response = supabase_main.table("stop_words").select("word").execute()
    stop_words_set = {item['word'] for item in response.data}
    print(f"[*] {len(stop_words_set)}件の除外ワードを読み込みました。")

    total_processed_count = 0
    
    while True:
        response = supabase_main.table("crawl_queue").select("id, url").in_("status", ["queued", "failed"]).limit(process_batch_size).execute()
        urls_to_process = response.data
        if not urls_to_process:
            print("[*] 処理対象のURLがキューにありません。終了します。")
            break

        processing_ids = [item['id'] for item in urls_to_process]
        supabase_main.table("crawl_queue").update({"status": "processing"}).in_("id", processing_ids).execute()
        print(f"[*] {len(urls_to_process)}件のURLをロックしました。")

        success_count = 0
        for item in urls_to_process:
            # サーバー負荷軽減のため、リクエストごとに遅延を入れる
            time.sleep(2) # 2秒に1アクセス
            result = process_single_url(item, supabase_main, stop_words_set, request_timeout, tokenizer_obj, debug_mode)
            if result:
                success_count += 1
        
        batch_count = len(urls_to_process)
        total_processed_count += batch_count
        print(f"  [+] 1バッチ処理完了 (成功: {success_count}, 失敗: {batch_count - success_count})")
    
    print(f"\n--- コンテンツ解析処理終了 ---")
    print(f"今回処理した合計URL数: {total_processed_count}")

if __name__ == "__main__":
    main()
