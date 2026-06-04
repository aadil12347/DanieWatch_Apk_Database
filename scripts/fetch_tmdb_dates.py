#!/usr/bin/env python3
"""
Fetch TMDB release dates for all items in streaming_links/.
Uses ThreadPoolExecutor for concurrent requests + persistent JSON cache.

TMDB rate limit: ~40 requests per 10 seconds.
We use 8 concurrent workers with 0.3s delay = ~26 req/s target,
but the HTTP latency naturally throttles to ~8-12 req/s in practice.
"""

import json
import os
import sys
import glob
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

try:
    import requests
except ImportError:
    print("ERROR: pip install requests")
    sys.exit(1)

TMDB_API_KEY = os.environ.get('TMDB_API_KEY', 'fc6d85b3839330e3458701b975195487')
TMDB_BASE = 'https://api.themoviedb.org/3'

# Rate limiter: max 35 requests per 10 seconds
class RateLimiter:
    def __init__(self, max_calls=35, period=10):
        self.max_calls = max_calls
        self.period = period
        self.calls = []
        self.lock = threading.Lock()

    def wait(self):
        with self.lock:
            now = time.monotonic()
            # Remove old calls
            self.calls = [t for t in self.calls if now - t < self.period]
            if len(self.calls) >= self.max_calls:
                sleep_time = self.period - (now - self.calls[0]) + 0.1
                if sleep_time > 0:
                    time.sleep(sleep_time)
            self.calls.append(time.monotonic())


rate_limiter = RateLimiter(max_calls=35, period=10)


def fetch_one(tmdb_id, media_type, season_num=1, total_eps=999):
    """Fetch release date for one item. Returns (key, date_str) or (key, None)."""
    key = f"{tmdb_id}-{media_type}"
    rate_limiter.wait()

    try:
        if media_type == 'movie':
            resp = requests.get(
                f'{TMDB_BASE}/movie/{tmdb_id}',
                params={'api_key': TMDB_API_KEY},
                timeout=10,
            )
            if resp.status_code == 200:
                return key, resp.json().get('release_date') or None
            if resp.status_code == 429:
                time.sleep(3)
                resp = requests.get(
                    f'{TMDB_BASE}/movie/{tmdb_id}',
                    params={'api_key': TMDB_API_KEY},
                    timeout=10,
                )
                if resp.status_code == 200:
                    return key, resp.json().get('release_date') or None
        else:
            # TV show
            resp = requests.get(
                f'{TMDB_BASE}/tv/{tmdb_id}/season/{season_num}',
                params={'api_key': TMDB_API_KEY},
                timeout=10,
            )
            if resp.status_code == 200:
                data = resp.json()
                episodes = data.get('episodes', [])
                for ep in reversed(episodes):
                    ep_num = ep.get('episode_number', 0)
                    if ep_num <= total_eps and ep.get('air_date'):
                        return key, ep['air_date']
                return key, data.get('air_date') or None
            if resp.status_code == 404:
                # Try show-level
                rate_limiter.wait()
                resp2 = requests.get(
                    f'{TMDB_BASE}/tv/{tmdb_id}',
                    params={'api_key': TMDB_API_KEY},
                    timeout=10,
                )
                if resp2.status_code == 200:
                    return key, resp2.json().get('first_air_date') or None
            if resp.status_code == 429:
                time.sleep(3)
                resp = requests.get(
                    f'{TMDB_BASE}/tv/{tmdb_id}/season/{season_num}',
                    params={'api_key': TMDB_API_KEY},
                    timeout=10,
                )
                if resp.status_code == 200:
                    data = resp.json()
                    episodes = data.get('episodes', [])
                    for ep in reversed(episodes):
                        ep_num = ep.get('episode_number', 0)
                        if ep_num <= total_eps and ep.get('air_date'):
                            return key, ep['air_date']
                    return key, data.get('air_date') or None
    except Exception as e:
        pass

    return key, None


def main():
    repo_root = sys.argv[1] if len(sys.argv) > 1 else r'c:\Users\mdani\Desktop\DanieWatch_DB_temp'
    sl_dir = os.path.join(repo_root, 'streaming_links')
    cache_path = os.path.join(repo_root, 'release_dates_cache.json')

    # Load cache
    cache = {}
    if os.path.exists(cache_path):
        with open(cache_path, 'r', encoding='utf-8') as f:
            cache = json.load(f)
    print(f"Loaded cache: {len(cache)} entries")

    # Load all items
    items = []
    for filepath in glob.glob(os.path.join(sl_dir, '**', '*.json'), recursive=True):
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                item = json.load(f)
            if isinstance(item, dict) and item.get('id'):
                tmdb_id = str(item['id'])
                try:
                    int(tmdb_id)
                except ValueError:
                    continue
                media_type = item.get('type') or item.get('media_type', 'movie')
                key = f"{tmdb_id}-{media_type}"
                if key not in cache:
                    season = item.get('latest_uploaded_season', 1) or 1
                    eps = item.get('total_uploaded_episodes', 999) or 999
                    items.append((tmdb_id, media_type, int(season), int(eps), key))
        except Exception:
            pass

    print(f"Items to fetch: {len(items)} (skipping {len(cache)} cached)")

    if not items:
        print("Nothing to fetch!")
        return

    fetched = 0
    failed = 0
    start = time.time()

    # Use ThreadPoolExecutor for concurrent fetching
    with ThreadPoolExecutor(max_workers=6) as executor:
        futures = {}
        for tmdb_id, media_type, season, eps, key in items:
            fut = executor.submit(fetch_one, tmdb_id, media_type, season, eps)
            futures[fut] = key

        for i, fut in enumerate(as_completed(futures)):
            key, date = fut.result()
            if date:
                cache[key] = date
                fetched += 1
            else:
                failed += 1

            done = i + 1
            if done % 50 == 0 or done == len(items):
                elapsed = time.time() - start
                rate = done / elapsed if elapsed > 0 else 0
                eta = (len(items) - done) / rate if rate > 0 else 0
                print(f"  [{done}/{len(items)}] fetched={fetched} failed={failed} rate={rate:.1f}/s ETA={eta:.0f}s")

            # Save cache every 200 items
            if done % 200 == 0:
                with open(cache_path, 'w', encoding='utf-8') as f:
                    json.dump(cache, f, ensure_ascii=False, indent=2, sort_keys=True)

    # Final save
    with open(cache_path, 'w', encoding='utf-8') as f:
        json.dump(cache, f, ensure_ascii=False, indent=2, sort_keys=True)

    elapsed = time.time() - start
    print(f"\nDone! {fetched} fetched, {failed} failed in {elapsed:.0f}s")
    print(f"Cache now has {len(cache)} entries saved to {cache_path}")


if __name__ == '__main__':
    main()
