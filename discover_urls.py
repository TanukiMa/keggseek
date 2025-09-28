# discover_urls.py
import os
import sys
import requests
import configparser
import time
import random
from urllib.parse import urljoin, urlparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from bs4 import BeautifulSoup
from supabase import create_client, Client
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from url_normalize import url_normalize

def fetch_links_from_url(url: str, config, session) -> set:
    """単一のURLからリンクをすべて抽出し、正規化してセットとして返す"""
    target_domain = config.get('General', 'TARGET_DOMAIN')
    request_timeout = config.getint('General', 'REQUEST_TIMEOUT')
    found_links = set()
    
    try:
        headers = {'User-Agent': 'Mozilla/5.0'}
        response = session.get(url, timeout=request_timeout, headers=headers, allow_redirects=True)
        response.raise_for_status()
        
        content_type = response.headers.get("content-type", "").lower()

        if "html" in content_type:
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # ▼▼▼▼▼ リンク抽出条件を修正 ▼▼▼▼▼
            # 'href'属性に'japic_med?japic_code='という文字列が含まれる<a>タグをすべて探す
            for a_tag in soup.find_all('a', href=lambda href: href and 'japic_med?japic_code=' in href):
            # ▲▲▲▲▲ ここまで修正 ▲▲▲▲▲
                try:
                    link = urljoin(url, a_tag['href'])
                    normalized_link = url_normalize(link)
                    
                    if urlparse(normalized_link).netloc == target_domain:
                        found_links.add(normalized_link)
                except Exception:
                    pass
    except Exception as e:
        print(f"  [!] エラー: {url} - {e}", file=sys.stderr)
    
    return found_links

def main():
    config = configparser.ConfigParser()
    config.read('config.ini')
    
    start_url = config.get('General', 'START_URL')
    max_workers = config.getint('Discoverer', 'MAX_DISCOVER_WORKERS')
    discover_accesses_per_minute = config.getint('RateLimit', 'DISCOVER_ACCESSES_PER_MINUTE')

    supabase_url, supabase_key = os.environ.get("SUPABASE_URL"), os.environ.get("SUPABASE_KEY")
    if not supabase_url or not supabase_key: raise ValueError("環境変数を設定してください。")
    supabase = create_client(supabase_url, supabase_key)
    
    print("--- ページネーション・クロール (レートリミットモード) 開始 ---")

    session = requests.Session()
    retries = Retry(total=3, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
    session.mount('https://', HTTPAdapter(max_retries=retries))

    print(f"[*] 最初のページから最大ページ番号を取得します: {start_url}")
    pages_to_scrape = []
    try:
        response = session.get(start_url, timeout=30)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')
        
        page_numbers = [int(a['data-page']) for a in soup.find_all('a', attrs={'data-page': True})]
        last_page = max(page_numbers) if page_numbers else 1
        print(f"  [+] 最大ページ番号 {last_page} を特定しました。")

        base_url = start_url.split('?')[0]
        pages_to_scrape = [f"{base_url}?page={i}&display=med" for i in range(1, last_page + 1)]
        print(f"[*] {len(pages_to_scrape)}件のインデックスページをクロール対象とします。")

    except Exception as e:
        print(f"  [!] 最大ページ番号の取得に失敗しました: {e}", file=sys.stderr)
        pages_to_scrape.append(start_url)
    
    all_discovered_links = set()
    discover_batch_size = 20

    for i in range(0, len(pages_to_scrape), discover_batch_size):
        batch_start_time = time.time()
        batch = pages_to_scrape[i:i + discover_batch_size]
        print(f"\n[*] インデックスページ {i+1}～{i+len(batch)} の処理を開始します...")

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_url = {executor.submit(fetch_links_from_url, url, config, session): url for url in batch}
            for future in as_completed(future_to_url):
                try:
                    all_discovered_links.update(future.result())
                except Exception as exc:
                    print(f'[!] ワーカーで例外が発生しました: {exc}', file=sys.stderr)
        
        batch_end_time = time.time()
        elapsed_time = batch_end_time - batch_start_time
        
        if discover_accesses_per_minute > 0:
            required_time = (60.0 / discover_accesses_per_minute) * len(batch)
            if elapsed_time < required_time:
                wait_time = required_time - elapsed_time
                print(f"  [*] レートリミットのため {wait_time:.2f} 秒待機します。")
                time.sleep(wait_time)

    if not all_discovered_links:
        print("[*] 解析対象のURLは発見されませんでした。")
        return

    print(f"\n[*] 合計 {len(all_discovered_links)}件のユニークなURLを発見しました。キューに追加します...")
    
    try:
        chunk_size = 500
        links_list = list(all_discovered_links)
        for i in range(0, len(links_list), chunk_size):
            chunk = links_list[i:i + chunk_size]
            supabase.table("crawl_queue").upsert(
                [{"url": link, "status": "queued"} for link in chunk], 
                on_conflict="url"
            ).execute()
        print("  [+] キューへの追加が完了しました。")
    except Exception as e:
        print(f"  [!] キューへの一括書き込みでエラー: {e}", file=sys.stderr)

    print(f"\n--- URL発見処理終了 ---")

if __name__ == "__main__":
    main()
