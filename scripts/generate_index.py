#!/usr/bin/env python3
import os
import json
import re
import urllib.request
import urllib.error
from datetime import datetime

INDEX_FILE = 'index.json'
NO_SORTING_FILE = 'no_sorting.json'
WITHOUT_METADATA_FILE = 'without_metadata_posts.json'
STREAMING_LINKS_DIR = 'streaming_links'

# Language fallback mapping (dubbed language -> (lang_code, [country_code]))
LANGUAGE_FALLBACKS = {
    "turkish": ("tr", ["TR"]),
    "korean": ("ko", ["KR"]),
    "english": ("en", ["US"]),
    "spanish": ("es", ["ES"]),
    "german": ("de", ["DE"]),
    "punjabi": ("pa", ["IN"]),
    "japanese": ("ja", ["JP"]),
    "french": ("fr", ["FR"]),
    "italian": ("it", ["IT"]),
    "chinese": ("zh", ["CN"]),
    "hindi": ("hi", ["IN"]),
    "tamil": ("ta", ["IN"]),
    "telugu": ("te", ["IN"]),
    "malayalam": ("ml", ["IN"]),
    "kannada": ("kn", ["IN"]),
    "bengali": ("bn", ["IN"]),
    "marathi": ("mr", ["IN"]),
    "thai": ("th", ["TH"]),
    "russian": ("ru", ["RU"]),
    "portuguese": ("pt", ["PT"]),
    "arabic": ("ar", ["SA"]),
    "indonesian": ("id", ["ID"]),
}

def clean_title(title):
    if not title:
        return ""
    # Remove things like (Season 1), (Season 1 to 3), (Season 3 - 4), (Season 1 to 4)
    title = re.sub(r'\s*\(\s*Season\s+.*?\)', '', title, flags=re.IGNORECASE)
    # Remove things like (2026), (2025)
    title = re.sub(r'\s*\(\s*\d{4}\s*\)', '', title)
    return title.strip()

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

def get_fallback_lang_and_countries(languages):
    # Filter out "Hindi" (case-insensitive) if other languages exist
    filtered_langs = [l for l in languages if l.lower().strip() != 'hindi']
    if not filtered_langs and languages:
        filtered_langs = languages
        
    for lang_name in filtered_langs:
        key = lang_name.lower().strip()
        if key in LANGUAGE_FALLBACKS:
            return LANGUAGE_FALLBACKS[key]
            
    return "en", []

def fetch_tmdb_details_by_id(tmdb_id, tmdb_type, credential):
    url = f"https://api.themoviedb.org/3/{tmdb_type}/{tmdb_id}"
    if len(credential) > 50:
        headers = {
            'Authorization': f'Bearer {credential}',
            'User-Agent': 'Mozilla/5.0',
            'Content-Type': 'application/json;charset=utf-8'
        }
        req_url = url
    else:
        headers = {'User-Agent': 'Mozilla/5.0'}
        req_url = url + f"?api_key={credential}"
        
    try:
        req = urllib.request.Request(req_url, headers=headers)
        with urllib.request.urlopen(req, timeout=10) as response:
            data = json.loads(response.read().decode('utf-8'))
            
            title = data.get('title') if tmdb_type == 'movie' else data.get('name')
            release_date = data.get('release_date') if tmdb_type == 'movie' else data.get('first_air_date')
            original_language = data.get('original_language') or ''
            
            original_countries = []
            if 'origin_country' in data and isinstance(data['origin_country'], list):
                original_countries = [c for c in data['origin_country'] if c]
            elif 'production_countries' in data and isinstance(data['production_countries'], list):
                original_countries = [c.get('iso_3166_1') for c in data['production_countries'] if c.get('iso_3166_1')]
                
            genres = [g.get('name') for g in data.get('genres', []) if g.get('name')]
            imdb_id = data.get('imdb_id') or ''
            
            return {
                "tmdb_id": int(tmdb_id),
                "title": title,
                "release_date": release_date,
                "original_language": original_language,
                "original_countries": original_countries,
                "genres": genres,
                "imdb_id": imdb_id
            }
    except Exception as e:
        print(f"Error fetching TMDB details for ID {tmdb_id} ({tmdb_type}): {e}")
        return None

def fetch_tmdb_details(tmdb_id_or_imdb_id, post_type, credential):
    if not credential:
        return None
    
    is_imdb = isinstance(tmdb_id_or_imdb_id, str) and tmdb_id_or_imdb_id.startswith('tt')
    tmdb_type = 'tv' if post_type in ('series', 'tv') else 'movie'
    
    if is_imdb:
        url = f"https://api.themoviedb.org/3/find/{tmdb_id_or_imdb_id}"
        if len(credential) > 50:
            headers = {
                'Authorization': f'Bearer {credential}',
                'User-Agent': 'Mozilla/5.0',
                'Content-Type': 'application/json;charset=utf-8'
            }
            req_url = url + "?external_source=imdb_id"
        else:
            headers = {'User-Agent': 'Mozilla/5.0'}
            req_url = url + f"?api_key={credential}&external_source=imdb_id"
            
        try:
            req = urllib.request.Request(req_url, headers=headers)
            with urllib.request.urlopen(req, timeout=10) as response:
                find_data = json.loads(response.read().decode('utf-8'))
                results = find_data.get('movie_results') or find_data.get('tv_results')
                if results:
                    result = results[0]
                    tmdb_id = result.get('id')
                    return fetch_tmdb_details_by_id(tmdb_id, tmdb_type, credential)
        except Exception as e:
            print(f"Error finding TMDB entry by IMDB ID {tmdb_id_or_imdb_id}: {e}")
            return None
    else:
        return fetch_tmdb_details_by_id(tmdb_id_or_imdb_id, tmdb_type, credential)

def is_accurate_date(date_str):
    if not date_str:
        return False
    match = re.match(r'^(\d{4})-(\d{2})-(\d{2})', date_str)
    if not match:
        return False
    year, month, day = int(match.group(1)), int(match.group(2)), int(match.group(3))
    if 1 <= month <= 12 and 1 <= day <= 31:
        return True
    return False

def parse_date_to_timestamp(date_str):
    if not date_str:
        return 0
    for fmt in ('%Y-%m-%d %H:%M:%S', '%Y-%m-%d %H:%M', '%Y-%m-%d'):
        try:
            dt = datetime.strptime(date_str.strip(), fmt)
            return int(dt.timestamp())
        except ValueError:
            pass
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
<<<<<<< HEAD
    aired_date = item[8] or ""
=======
    if isinstance(item, list):
        aired_date = item[8] if len(item) > 8 else ""
        title = item[1] if len(item) > 1 else ""
        tmdb_id = item[0] if len(item) > 0 else 0
    else:
        aired_date = item.get("aired_date") or ""
        title = item.get("title") or ""
        tmdb_id = item.get("tmdb_id") or 0
        
>>>>>>> 75394528750b80c42ceb74d7118dfbbb53c7fe90
    accurate = is_accurate_date(aired_date)
    
    if accurate:
        match = re.match(r'^(\d{4})', aired_date)
        year = int(match.group(1))
        timestamp = parse_date_to_timestamp(aired_date)
    else:
        match_year = re.match(r'^(\d{4})', aired_date)
        if match_year:
            year = int(match_year.group(1))
        else:
<<<<<<< HEAD
            year = extract_year(item[1])
        timestamp = 0
        
    code = item[0]
    if isinstance(code, int):
        id_sort = -code
    elif isinstance(code, str) and code.startswith('tt'):
        try:
            id_sort = -int(code[2:])
        except ValueError:
            id_sort = 0
    else:
        id_sort = 0
=======
            year = extract_year(title)
        timestamp = 0
        
    tmdb_id = 0
    try:
        tmdb_id = int(tmdb_id)
    except (ValueError, TypeError):
        pass
>>>>>>> 75394528750b80c42ceb74d7118dfbbb53c7fe90

    return (-year, 1 if not accurate else 0, -timestamp, id_sort)

def main():
<<<<<<< HEAD
    # 1. Read existing index.json to preserve manual edits and aired dates
    existing_entries = {}
=======
    # 1. Read existing index.json to preserve manual edits or existing dates
    existing_dates = {}
    existing_posts = {}
>>>>>>> 75394528750b80c42ceb74d7118dfbbb53c7fe90
    if os.path.exists(INDEX_FILE):
        try:
            with open(INDEX_FILE, 'r', encoding='utf-8') as f:
                data = json.load(f)
                posts = data if isinstance(data, list) else data.get('posts', [])
                for post in posts:
<<<<<<< HEAD
                    if isinstance(post, dict):
                        # Old dict format
                        tmdb_id = post.get('tmdb_id')
                        imdb_id = post.get('imdb_id') or ''
                        post_type = post.get('type')
                        if post_type == 'series':
                            post_type = 'tv'
                        key = (tmdb_id, post_type) if tmdb_id else (imdb_id, post_type)
                        existing_entries[key] = {
                            "title": post.get('title'),
                            "tmdb_id": tmdb_id,
                            "imdb_id": imdb_id,
                            "languages": post.get('languages', []),
                            "type": post_type,
                            "aired_date": post.get('aired_date'),
                            "original_language": post.get('original_language', ''),
                            "original_countries": post.get('original_countries', []),
                            "genres": post.get('genres', [])
                        }
                    elif isinstance(post, list) and len(post) >= 9:
                        # New list format
                        tmdb_id = post[0]
                        imdb_id = post[7] or ''
                        post_type = post[2]
                        key = (tmdb_id, post_type) if tmdb_id else (imdb_id, post_type)
                        existing_entries[key] = {
                            "tmdb_id": tmdb_id,
                            "title": post[1],
                            "type": post_type,
                            "original_language": post[3],
                            "original_countries": post[4],
                            "languages": post[5],
                            "genres": post[6],
                            "imdb_id": imdb_id,
                            "aired_date": post[8]
                        }
=======
                    if isinstance(post, list):
                        tmdb_id = post[0]
                        post_type = post[2]
                        if tmdb_id is not None and post_type is not None:
                            key = (int(tmdb_id), post_type)
                            existing_posts[key] = post
                            existing_dates[key] = post[8] if len(post) > 8 else None
                    elif isinstance(post, dict):
                        tmdb_id = post.get('tmdb_id') or post.get('id')
                        post_type = post.get('type') or post.get('media_type')
                        if tmdb_id is not None and post_type is not None:
                            key = (int(tmdb_id), post_type)
                            existing_dates[key] = post.get('aired_date') or post.get('release_date')
>>>>>>> 75394528750b80c42ceb74d7118dfbbb53c7fe90
        except Exception as e:
            print(f"Warning: Could not parse existing {INDEX_FILE}: {e}")

    # 2. Scan streaming_links/ folder for JSON files
    if not os.path.isdir(STREAMING_LINKS_DIR):
        print(f"Error: Directory '{STREAMING_LINKS_DIR}' not found.")
        return

    # Load TMDB credential from environment (fallback to hardcoded app key if missing)
    tmdb_cred = os.environ.get('TMDB_API_KEY') or os.environ.get('TMDB_KEY') or 'fc6d85b3839330e3458701b975195487'

    posts = []
    no_sorting_posts = []
    without_metadata_posts = []

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

        # Extract fields from file
        title = content.get('post_title') or content.get('title')
        post_type = content.get('post_type') or content.get('type')
        if post_type == 'series':
            post_type = 'tv'
            
        raw_tmdb_id = content.get('tmdb_id') or content.get('id')
        imdb_id = content.get('imdb_id') or ''
        languages = content.get('languages') or []

        # Determine if raw_tmdb_id is an IMDB ID
        if isinstance(raw_tmdb_id, str) and raw_tmdb_id.startswith('tt'):
            imdb_id = raw_tmdb_id
            raw_tmdb_id = "NOT_FOUND"

        # Try parsing tmdb_id as integer
        tmdb_id = None
        try:
            if raw_tmdb_id is not None and str(raw_tmdb_id).strip().isdigit():
                tmdb_id = int(raw_tmdb_id)
        except ValueError:
            pass

        # Identify code to use as list element 0
        code = 0
        if tmdb_id is not None:
            code = tmdb_id
        elif imdb_id and imdb_id.startswith('tt'):
            code = imdb_id
        else:
            # Try to extract IMDB ID from filename
            match_imdb = re.search(r'\b(tt\d+)\b', file)
            if match_imdb:
                imdb_id = match_imdb.group(1)
                code = imdb_id

        # Determine the key to check in existing_entries
        lookup_key = (code, post_type)
        existing = existing_entries.get(lookup_key)

        # Initialize metadata variables
        orig_title = clean_title(title)
        orig_lang = ""
        orig_countries = []
        orig_genres = []
        aired_date = None

        if existing:
            # Preserve existing data fields strictly as requested by user
            orig_title = clean_title(existing.get("title")) or orig_title
            orig_lang = existing.get("original_language") or ""
            orig_countries = existing.get("original_countries") or []
            orig_genres = existing.get("genres") or []
            aired_date = existing.get("aired_date")
            imdb_id = existing.get("imdb_id") or imdb_id

        # If we need TMDB metadata (either not in existing or fields are empty)
        tmdb_fetched = False
        if not orig_lang or not orig_countries or not orig_genres or not aired_date:
            # Attempt TMDB fetch
            query_id = tmdb_id if tmdb_id else imdb_id
            if query_id and query_id != "NOT_FOUND" and query_id != 0:
                print(f"Fetching TMDB metadata for {orig_title} (ID: {query_id}, Type: {post_type})...")
                details = fetch_tmdb_details(query_id, post_type, tmdb_cred)
                if details:
                    orig_title = clean_title(details.get("title")) or orig_title
                    orig_lang = details.get("original_language") or orig_lang
                    orig_countries = details.get("original_countries") or orig_countries
                    orig_genres = details.get("genres") or orig_genres
                    imdb_id = details.get("imdb_id") or imdb_id
                    if not aired_date and details.get("release_date"):
                        # Format date with time suffix for sorting
                        fetched_date = details.get("release_date")
                        if is_accurate_date(fetched_date):
                            current_time = datetime.now().strftime('%H:%M:%S')
                            aired_date = f"{fetched_date} {current_time}"
                    tmdb_fetched = True

        # Fallbacks if TMDB lookup fails/is incomplete
        if not orig_lang or not orig_countries:
            fallback_lang, fallback_countries = get_fallback_lang_and_countries(languages)
            if not orig_lang:
                orig_lang = fallback_lang
            if not orig_countries:
                orig_countries = fallback_countries

        if not aired_date:
            year = extract_year(title, file)
            if year > 0:
                aired_date = f"{year}-00-00"
            else:
                aired_date = "0000-00-00"

<<<<<<< HEAD
        # Log to without_metadata_posts if we couldn't resolve TMDB metadata
        if not tmdb_fetched and (not tmdb_id or tmdb_id == 0 or tmdb_id == "NOT_FOUND"):
            without_metadata_posts.append({
                "filename": file,
                "title": orig_title,
                "imdb_id": imdb_id,
                "type": post_type,
                "languages": languages
            })

        # Create entry list in correct format:
        # [tmdb_id_or_imdb_id, title, type, original_language, original_countries, languages, genres, imdb_id, aired_date]
        entry = [
            code,
            orig_title,
            post_type,
            orig_lang,
            orig_countries,
            languages,
            orig_genres,
            imdb_id,
            aired_date
        ]
=======
        # Create entry
        cached_post = existing_posts.get(key)
        
        # Determine highest season and total episodes for series
        highest_uploaded_season = 1
        total_uploaded_episodes = 0
        if post_type in ('tv', 'series'):
            seasons_data = content.get('seasons')
            if isinstance(seasons_data, list):
                for s in seasons_data:
                    s_num = int(s.get('season_number', 1))
                    if s_num > highest_uploaded_season:
                        highest_uploaded_season = s_num
                    total_uploaded_episodes += len(s.get('episodes', []))
            elif isinstance(seasons_data, dict):
                for s_str in seasons_data.keys():
                    try:
                        s_num = int(s_str)
                        if s_num > highest_uploaded_season:
                            highest_uploaded_season = s_num
                        season_data = seasons_data[s_str]
                        episodes_set = set()
                        for qual in season_data.keys():
                            ep_list = season_data[qual]
                            if isinstance(ep_list, list):
                                for ep in ep_list:
                                    if ep.get('episode_title'):
                                        episodes_set.add(ep.get('episode_title'))
                        total_uploaded_episodes += len(episodes_set)
                    except ValueError:
                        pass

        # If cached, preserve existing fields but update title, languages, release date, and tv details
        if cached_post and isinstance(cached_post, list):
            entry = list(cached_post)
            entry[1] = title
            entry[2] = 'tv' if post_type == 'series' else post_type
            entry[5] = languages
            entry[7] = imdb_id or (entry[7] if len(entry) > 7 else "")
            entry[8] = aired_date
            if post_type in ('tv', 'series'):
                while len(entry) < 11:
                    entry.append(None)
                entry[9] = highest_uploaded_season
                entry[10] = total_uploaded_episodes
        else:
            # Create a new positional array
            entry = [
                tmdb_id,                                      # 0: id
                title,                                        # 1: title
                'tv' if post_type == 'series' else post_type, # 2: type
                "en",                                         # 3: original_language
                [],                                           # 4: country
                languages,                                    # 5: language
                [],                                           # 6: genres
                imdb_id,                                      # 7: imdb_id
                aired_date,                                   # 8: release_date
            ]
            if post_type in ('tv', 'series'):
                entry.append(highest_uploaded_season)       # 9: latest_uploaded_season
                entry.append(total_uploaded_episodes)       # 10: total_uploaded_episodes
>>>>>>> 75394528750b80c42ceb74d7118dfbbb53c7fe90

        posts.append(entry)

        # Add to no_sorting if aired_date is not accurate
        if not is_accurate_date(aired_date):
            no_sorting_posts.append(entry)

    # 3. Sort posts
    posts.sort(key=compute_sort_key)
    no_sorting_posts.sort(key=compute_sort_key)

<<<<<<< HEAD
    # 4. Write index.json
=======
    # 5. Write index.json with one post array per line
>>>>>>> 75394528750b80c42ceb74d7118dfbbb53c7fe90
    try:
        with open(INDEX_FILE, 'w', encoding='utf-8') as f:
            lines = [json.dumps(post, ensure_ascii=False) for post in posts]
            json_str = "[\n  " + ",\n  ".join(lines) + "\n]"
            f.write(json_str)
        print(f"Successfully wrote {len(posts)} posts to {INDEX_FILE}.")
    except Exception as e:
        print(f"Error writing {INDEX_FILE}: {e}")

<<<<<<< HEAD
    # 5. Write no_sorting.json
=======
    # 6. Write no_sorting.json with one post array per line
>>>>>>> 75394528750b80c42ceb74d7118dfbbb53c7fe90
    try:
        with open(NO_SORTING_FILE, 'w', encoding='utf-8') as f:
            lines = [json.dumps(post, ensure_ascii=False) for post in no_sorting_posts]
            json_str = "[\n  " + ",\n  ".join(lines) + "\n]"
            f.write(json_str)
        print(f"Successfully wrote {len(no_sorting_posts)} items to {NO_SORTING_FILE}.")
    except Exception as e:
        print(f"Error writing {NO_SORTING_FILE}: {e}")

    # 6. Write without_metadata_posts.json
    try:
        with open(WITHOUT_METADATA_FILE, 'w', encoding='utf-8') as f:
            json.dump(without_metadata_posts, f, ensure_ascii=False, indent=2)
        print(f"Successfully wrote {len(without_metadata_posts)} items to {WITHOUT_METADATA_FILE}.")
    except Exception as e:
        print(f"Error writing {WITHOUT_METADATA_FILE}: {e}")

if __name__ == '__main__':
    main()
