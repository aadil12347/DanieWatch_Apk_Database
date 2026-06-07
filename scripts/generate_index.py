#!/usr/bin/env python3
import os
import json
import re
import urllib.request
import urllib.error
from datetime import datetime

INDEX_FILE = 'index.json'
NO_SORTING_FILE = 'no_sorting.json'
STREAMING_LINKS_DIR = 'streaming_links'

def extract_year(title, filename=""):
    # Try finding (YYYY) in title
    match = re.search(r'\((\d{4})\)', title)
    if match:
        return int(match.group(1))
    # Try finding (YYYY) in filename
    match = re.search(r'\((\d{4})\)', filename)
    if match:
        return int(match.group(1))
    # Fallback to any 4-digit number between 1900 and 2100
    match = re.search(r'\b(19\d{2}|20\d{2})\b', title)
    if match:
        return int(match.group(1))
    match = re.search(r'\b(19\d{2}|20\d{2})\b', filename)
    if match:
        return int(match.group(1))
    return 0

def fetch_tmdb_release_date(tmdb_id, post_type, credential):
    if not credential:
        print(f"Skipping TMDB fetch for {tmdb_id} ({post_type}): No TMDB credential provided.")
        return None
    
    # TMDB uses 'tv' for series
    tmdb_type = 'tv' if post_type == 'series' else 'movie'
    
    if len(credential) > 50:
        # v4 Bearer Token
        url = f"https://api.themoviedb.org/3/{tmdb_type}/{tmdb_id}"
        headers = {
            'Authorization': f'Bearer {credential}',
            'User-Agent': 'Mozilla/5.0',
            'Content-Type': 'application/json;charset=utf-8'
        }
    else:
        # v3 API Key
        url = f"https://api.themoviedb.org/3/{tmdb_type}/{tmdb_id}?api_key={credential}"
        headers = {
            'User-Agent': 'Mozilla/5.0'
        }
        
    try:
        req = urllib.request.Request(url, headers=headers)
        with urllib.request.urlopen(req, timeout=10) as response:
            data = json.loads(response.read().decode('utf-8'))
            if tmdb_type == 'tv':
                return data.get('first_air_date')
            else:
                return data.get('release_date')
    except Exception as e:
        print(f"Error fetching TMDB ID {tmdb_id} ({post_type}): {e}")
        return None

def is_accurate_date(date_str):
    if not date_str:
        return False
    # Check if format is YYYY-MM-DD ...
    match = re.match(r'^(\d{4})-(\d{2})-(\d{2})', date_str)
    if not match:
        return False
    
    year, month, day = int(match.group(1)), int(match.group(2)), int(match.group(3))
    # Month must be 1-12 and day must be 1-31
    if 1 <= month <= 12 and 1 <= day <= 31:
        return True
    return False

def parse_date_to_timestamp(date_str):
    if not date_str:
        return 0
    # Try parsing full datetime first
    for fmt in ('%Y-%m-%d %H:%M:%S', '%Y-%m-%d %H:%M', '%Y-%m-%d'):
        try:
            dt = datetime.strptime(date_str.strip(), fmt)
            return int(dt.timestamp())
        except ValueError:
            pass
    # Fallback to parsing YYYY-MM-DD
    match = re.match(r'^(\d{4})-(\d{2})-(\d{2})', date_str)
    if match:
        try:
            y, m, d = int(match.group(1)), int(match.group(2)), int(match.group(3))
            if 1 <= m <= 12 and 1 <= d <= 31:
                return int(datetime(y, m, d).timestamp())
        except ValueError:
            pass
    return 0

def compute_sort_key(item):
    aired_date = item.get("aired_date") or ""
    accurate = is_accurate_date(aired_date)
    
    if accurate:
        # Extract year from the accurate date
        match = re.match(r'^(\d{4})', aired_date)
        year = int(match.group(1))
        timestamp = parse_date_to_timestamp(aired_date)
    else:
        # Extract year from the aired_date string if it's YYYY-00-00 or YYYY
        match_year = re.match(r'^(\d{4})', aired_date)
        if match_year:
            year = int(match_year.group(1))
        else:
            year = extract_year(item.get("title", ""))
        timestamp = 0
        
    tmdb_id = 0
    try:
        tmdb_id = int(item.get("tmdb_id", 0))
    except (ValueError, TypeError):
        pass

    # Sort rules:
    # 1. -year: latest years first (e.g. 2026 before 2025)
    # 2. accurate flag: 0 for accurate (comes first), 1 for non-accurate (comes last)
    # 3. -timestamp: latest times first
    # 4. -tmdb_id: fallback stable sort
    return (-year, 1 if not accurate else 0, -timestamp, -tmdb_id)

def main():
    # 1. Read existing index.json to preserve manual edits or existing dates
    existing_dates = {}
    if os.path.exists(INDEX_FILE):
        try:
            with open(INDEX_FILE, 'r', encoding='utf-8') as f:
                data = json.load(f)
                posts = data if isinstance(data, list) else data.get('posts', [])
                for post in posts:
                    tmdb_id = post.get('tmdb_id')
                    post_type = post.get('type')
                    if tmdb_id is not None and post_type is not None:
                        key = (int(tmdb_id), post_type)
                        existing_dates[key] = post.get('aired_date')
        except Exception as e:
            print(f"Warning: Could not parse existing {INDEX_FILE}: {e}")

    # 2. Scan streaming_links/ folder for JSON files
    if not os.path.isdir(STREAMING_LINKS_DIR):
        print(f"Error: Directory '{STREAMING_LINKS_DIR}' not found.")
        return

    # Load TMDB credential from environment
    tmdb_cred = os.environ.get('TMDB_API_KEY') or os.environ.get('TMDB_KEY')

    posts = []
    no_sorting_posts = []

    files = [f for f in os.listdir(STREAMING_LINKS_DIR) if f.endswith('.json')]
    print(f"Found {len(files)} files in '{STREAMING_LINKS_DIR}'.")

    for file in files:
        filepath = os.path.join(STREAMING_LINKS_DIR, file)
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                content = json.load(f)
        except Exception as e:
            print(f"Error reading {file}: {e}")
            continue

        # Extract fields
        title = content.get('post_title') or content.get('title')
        post_type = content.get('post_type') or content.get('type')
        tmdb_id = content.get('tmdb_id') or content.get('id')
        imdb_id = content.get('imdb_id') or ''
        languages = content.get('languages') or []

        if not title or tmdb_id is None or not post_type:
            print(f"Skipping {file}: missing title, tmdb_id, or post_type.")
            continue

        try:
            tmdb_id = int(tmdb_id)
        except ValueError:
            print(f"Skipping {file}: invalid tmdb_id '{tmdb_id}'.")
            continue

        # Check if we already have a date for this item in index.json
        key = (tmdb_id, post_type)
        aired_date = existing_dates.get(key)

        # If not already present or if we need to fetch
        if aired_date is None:
            print(f"Fetching TMDB release info for: {title} (ID: {tmdb_id}, Type: {post_type})...")
            fetched_date = fetch_tmdb_release_date(tmdb_id, post_type, tmdb_cred)
            if fetched_date:
                # Validate the fetched date format YYYY-MM-DD
                if is_accurate_date(fetched_date):
                    aired_date = fetched_date
                    print(f"  -> Found date: {aired_date}")
                else:
                    print(f"  -> Invalid/incomplete date from TMDB: {fetched_date}")
            else:
                print(f"  -> Could not fetch release date from TMDB.")
        
        # If still no valid aired_date
        if not aired_date or not is_accurate_date(aired_date):
            # Extract year from title/filename
            year = extract_year(title, file)
            # If we don't have accurate date, format as YYYY-00-00 or 0000-00-00
            if year > 0:
                aired_date = f"{year}-00-00"
            else:
                aired_date = "0000-00-00"

        # Create entry
        entry = {
            "title": title,
            "tmdb_id": tmdb_id,
            "imdb_id": imdb_id,
            "languages": languages,
            "type": post_type,
            "aired_date": aired_date
        }
        
        posts.append(entry)

        # If it doesn't have an accurate date, add to no_sorting
        if not is_accurate_date(aired_date):
            no_sorting_posts.append(entry)

    # 3. Sort posts using the custom sort key
    posts.sort(key=compute_sort_key)
    
    # 4. Sort no_sorting_posts as well
    no_sorting_posts.sort(key=compute_sort_key)

    # 5. Write index.json
    try:
        with open(INDEX_FILE, 'w', encoding='utf-8') as f:
            json.dump(posts, f, ensure_ascii=False, indent=2)
        print(f"Successfully wrote {len(posts)} posts to {INDEX_FILE}.")
    except Exception as e:
        print(f"Error writing {INDEX_FILE}: {e}")

    # 6. Write no_sorting.json
    try:
        with open(NO_SORTING_FILE, 'w', encoding='utf-8') as f:
            json.dump(no_sorting_posts, f, ensure_ascii=False, indent=2)
        print(f"Successfully wrote {len(no_sorting_posts)} items to {NO_SORTING_FILE}.")
    except Exception as e:
        print(f"Error writing {NO_SORTING_FILE}: {e}")

if __name__ == '__main__':
    main()
