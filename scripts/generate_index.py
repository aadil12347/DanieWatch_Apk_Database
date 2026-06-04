#!/usr/bin/env python3
"""
Generate Flat Index + Incremental Updates from streaming_links (DanieWatch format).

Pipeline:
  1. Scan streaming_links/ folder (sole source of truth)
  2. Generate base_index.json (flat, unsorted, NO watch links)
  3. Generate daily incremental updates (diff against previous base)
  4. Categorize using ONLY origin_country + original_language (NOT dubbing language)
  5. Generate index/home/sections.json (carousel + sections)
  6. Duplicate detection

Watch links are NOT included in the index — they are fetched on-demand from
the streaming_links/ folder when the detail page is opened.

Output:
  base_index.json              — all posts, flat list (bundled in APK assets)
  updates/
    update_manifest.json       — list of daily incremental files
    index_N_YYYY-MM-DD.json    — daily diff files
  index/
    meta.json                  — version + total counts
    home/sections.json         — pre-built home screen data

Usage:
    python generate_index.py [--repo-root .]
"""

import json
import os
import sys
import glob
import time
from datetime import datetime, timezone
from typing import Any

# Category matching rules — uses ONLY origin_country + original_language
# NEVER matches on the 'language' field (that's dubbing/audio availability)
CATEGORIES = {
    'indian': {
        'countries': ['IN'],
        'languages': ['hi', 'hindi', 'ur', 'urdu', 'pa', 'punjabi', 'ta', 'tamil',
                       'te', 'telugu', 'ml', 'malayalam', 'kn', 'kannada',
                       'bn', 'bengali', 'mr', 'marathi', 'gu', 'gujarati'],
    },
    'korean': {
        'countries': ['KR'],
        'languages': ['ko', 'korean'],
    },
    'anime': {
        'countries': ['JP'],
        'languages': ['ja', 'japanese'],
        'genres': ['Animation'],
    },
    'chinese': {
        'countries': ['CN', 'HK', 'TW'],
        'languages': ['zh', 'cn', 'chinese', 'mandarin', 'cantonese'],
    },
    'punjabi': {
        'countries': [],
        'languages': ['pa', 'punjabi'],
    },
    'pakistani': {
        'countries': ['PK'],
        'languages': ['ur', 'urdu'],
    },
    # Hollywood = EVERYTHING ELSE (catch-all, not defined here)
}

HOME_SECTIONS = [
    {'title': 'Trending Now', 'filter': 'trending', 'limit': 20},
    {'title': 'Top 10 Today', 'filter': 'top10', 'limit': 10, 'is_ranked': True},
    {'title': 'Indian', 'filter': 'indian', 'limit': 20},
    {'title': 'Korean', 'filter': 'korean', 'limit': 20},
    {'title': 'Anime', 'filter': 'anime', 'limit': 20},
    {'title': 'Hollywood', 'filter': 'hollywood', 'limit': 20},
    {'title': 'Top Rated', 'filter': 'top_rated', 'limit': 20},
    {'title': 'Chinese', 'filter': 'chinese', 'limit': 20},
    {'title': 'Punjabi', 'filter': 'punjabi', 'limit': 20},
    {'title': 'Pakistani', 'filter': 'pakistani', 'limit': 20},
]


# ═══════════════════════════════════════════════════════════════════════════════
# Utility Functions
# ═══════════════════════════════════════════════════════════════════════════════

def safe_int(val) -> int:
    """Parse a value to int, return 0 if not parseable."""
    if val is None:
        return 0
    if isinstance(val, int):
        return val
    try:
        return int(str(val))
    except (ValueError, TypeError):
        return 0


def item_key(item: dict) -> str:
    """Unique key for an item: 'id-type'."""
    tmdb_id = item.get('id') or ''
    media_type = item.get('type') or item.get('media_type', 'movie')
    return f"{tmdb_id}-{media_type}"


def strip_watch_link(item: dict) -> dict:
    """Return a copy of the item with the 'watch' field removed.
    
    Watch links are fetched on-demand from streaming_links/ when the 
    detail page is opened, so they don't belong in the index.
    """
    cleaned = {k: v for k, v in item.items() if k != 'watch'}
    return cleaned


def get_sort_key(item: dict) -> tuple:
    """Generate sort key: year DESC → release_date DESC → id DESC."""
    year = safe_int(item.get('year') or item.get('release_year') or 0)
    
    date_str = item.get('release_date') or item.get('added_date') or item.get('sort_date') or ''
    date_sortable = 0
    if date_str and len(date_str) >= 10:
        try:
            date_sortable = int(date_str[:10].replace('-', ''))
        except ValueError:
            pass
    
    item_id = safe_int(item.get('id') or 0)
    return (-year, -date_sortable, -item_id)


def write_json(path: str, data: Any):
    """Write JSON file, creating dirs as needed."""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, separators=(',', ':'))


# ═══════════════════════════════════════════════════════════════════════════════
# Data Loading
# ═══════════════════════════════════════════════════════════════════════════════

def load_streaming_links(repo_root: str) -> list[dict]:
    """Scan streaming_links/ folder — each JSON file is one movie/show."""
    sl_dir = os.path.join(repo_root, 'streaming_links')
    if not os.path.isdir(sl_dir):
        print(f'  WARNING: streaming_links/ directory not found at {sl_dir}')
        return []
    
    items = []
    errors = 0
    for filepath in glob.glob(os.path.join(sl_dir, '**', '*.json'), recursive=True):
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                item = json.load(f)
                if isinstance(item, dict) and item.get('id'):
                    items.append(item)
                elif isinstance(item, list):
                    for sub_item in item:
                        if isinstance(sub_item, dict) and sub_item.get('id'):
                            items.append(sub_item)
        except (json.JSONDecodeError, IOError) as e:
            errors += 1
    
    if errors > 0:
        print(f'  WARNING: {errors} files failed to parse in streaming_links/')
    return items


def load_top_content(repo_root: str, folder: str) -> list[dict]:
    """Load Top 5 or Top 10 items."""
    top_dir = os.path.join(repo_root, folder)
    if not os.path.isdir(top_dir):
        return []
    items = []
    for filepath in sorted(glob.glob(os.path.join(top_dir, '*.json'))):
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                items.append(json.load(f))
        except (json.JSONDecodeError, IOError):
            pass
    return items


def load_previous_base_index(repo_root: str) -> dict:
    """Load previous base_index.json for diff computation."""
    path = os.path.join(repo_root, 'base_index.json')
    if not os.path.exists(path):
        return {}
    try:
        with open(path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        # Build lookup by item key
        lookup = {}
        for post in data.get('posts', []):
            key = item_key(post)
            lookup[key] = post
        return lookup
    except (json.JSONDecodeError, IOError):
        return {}


def load_update_manifest(repo_root: str) -> dict:
    """Load existing update_manifest.json."""
    path = os.path.join(repo_root, 'updates', 'update_manifest.json')
    if not os.path.exists(path):
        return {'latest_version': '', 'updates': []}
    try:
        with open(path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except (json.JSONDecodeError, IOError):
        return {'latest_version': '', 'updates': []}


# ═══════════════════════════════════════════════════════════════════════════════
# Category Matching (same logic as old generate_catalog.py)
# ═══════════════════════════════════════════════════════════════════════════════

def matches_category(item: dict, cat_config: dict) -> bool:
    """Check if item belongs to a category using origin_country + original_language."""
    countries = [c.upper() for c in cat_config.get('countries', [])]
    languages = [l.lower() for l in cat_config.get('languages', [])]
    genre_names = [g.lower() for g in cat_config.get('genres', [])]

    item_countries = item.get('country') or item.get('origin_country') or []
    if isinstance(item_countries, str):
        item_countries = [item_countries]
    item_countries = [c.upper() for c in item_countries]

    item_orig_lang = (item.get('original_language') or '').lower().strip()

    # Country match
    if countries and any(c in countries for c in item_countries):
        return True

    # Original language match
    if languages and item_orig_lang and item_orig_lang in languages:
        return True

    # Fallback: use 'language' field when no origin metadata
    if not item_orig_lang and not item_countries:
        item_lang = item.get('language') or []
        if isinstance(item_lang, str):
            item_lang = [item_lang]
        item_lang_lower = [l.lower().strip() for l in item_lang if isinstance(l, str)]
        if languages and any(l in languages for l in item_lang_lower):
            return True

    # Genre match (for anime)
    if genre_names:
        item_genres = item.get('genres') or []
        if isinstance(item_genres, str):
            item_genres = [item_genres]
        item_genres_lower = [g.lower().strip() for g in item_genres]
        if any(g in genre_names for g in item_genres_lower):
            if 'japanese' in languages or 'ja' in languages:
                if item_orig_lang in ('ja', 'japanese'):
                    return True
                item_lang = item.get('language') or []
                if isinstance(item_lang, str):
                    item_lang = [item_lang]
                item_lang_lower = [l.lower().strip() for l in item_lang]
                return any(l in ('ja', 'japanese') for l in item_lang_lower)
            return True

    return False


def is_hollywood(item: dict) -> bool:
    """Hollywood = catch-all: everything NOT in any other named category."""
    for cat_config in CATEGORIES.values():
        if matches_category(item, cat_config):
            return False
    return True


# ═══════════════════════════════════════════════════════════════════════════════
# Duplicate Detection
# ═══════════════════════════════════════════════════════════════════════════════

def detect_duplicates(items: list[dict], repo_root: str):
    """Detect duplicate TMDB IDs (admin + normal versions)."""
    by_tmdb_id: dict[str, list[dict]] = {}
    for item in items:
        tmdb_id = str(safe_int(item.get('id')))
        if tmdb_id == '0':
            continue
        media_type = item.get('type') or 'movie'
        key = f"{tmdb_id}-{media_type}"
        by_tmdb_id.setdefault(key, []).append(item)
    
    duplicates = {k: v for k, v in by_tmdb_id.items() if len(v) > 1}
    
    if duplicates:
        lines = ['# Duplicate Posts\n\n']
        lines.append(f'Found {len(duplicates)} duplicate TMDB IDs:\n\n')
        for key, dups in sorted(duplicates.items()):
            lines.append(f'## {key}\n')
            for d in dups:
                lines.append(f'- **{d.get("title", "?")}** (result: {d.get("result", "?")})\n')
            lines.append('\n')
        
        dup_path = os.path.join(repo_root, 'duplicate_posts.md')
        with open(dup_path, 'w', encoding='utf-8') as f:
            f.writelines(lines)
        print(f'  Found {len(duplicates)} duplicate TMDB IDs -> duplicate_posts.md')
    else:
        print('  No duplicate TMDB IDs found')


# ═══════════════════════════════════════════════════════════════════════════════
# Main Index Generation
# ═══════════════════════════════════════════════════════════════════════════════

def generate_index(repo_root: str):
    """Generate the full index system."""
    
    now = datetime.now(timezone.utc)
    version = now.strftime('%Y%m%dT%H%M%SZ')
    today = now.strftime('%Y-%m-%d')
    timestamp = int(now.timestamp())

    # ─── Step 1: Load items from streaming_links/ ─────────────────────────
    print('Step 1: Loading items from streaming_links/...')
    items = load_streaming_links(repo_root)
    print(f'  Found {len(items)} items')

    if not items:
        print('ERROR: No items found!', file=sys.stderr)
        sys.exit(1)

    # Filter out non-numeric IDs
    valid_items = [item for item in items if safe_int(item.get('id')) > 0]
    print(f'  {len(valid_items)} items with valid numeric IDs (skipped {len(items) - len(valid_items)})')
    items = valid_items

    # ─── Step 2: Generate base_index.json (flat, NO watch links) ──────────
    print('\nStep 2: Generating base_index.json...')
    
    # Strip watch links and sort for consistent output
    sorted_items = sorted(items, key=get_sort_key)
    index_posts = [strip_watch_link(item) for item in sorted_items]
    
    base_index = {
        'version': version,
        'total': len(index_posts),
        'posts': index_posts,
    }
    
    write_json(os.path.join(repo_root, 'base_index.json'), base_index)
    print(f'  Generated base_index.json ({len(index_posts)} posts, version: {version})')

    # ─── Step 3: Generate daily incremental update ────────────────────────
    print('\nStep 3: Generating incremental updates...')
    
    previous_lookup = load_previous_base_index(repo_root)
    current_lookup = {item_key(strip_watch_link(item)): strip_watch_link(item) for item in sorted_items}
    
    # Detect changes
    prev_keys = set(previous_lookup.keys())
    curr_keys = set(current_lookup.keys())
    
    added_keys = curr_keys - prev_keys
    removed_keys = prev_keys - curr_keys
    
    # Check for modified items (same key, different content — ignoring watch field)
    modified_keys = set()
    for key in curr_keys & prev_keys:
        curr_item = current_lookup[key]
        prev_item = previous_lookup[key]
        # Compare without watch field (already stripped)
        if json.dumps(curr_item, sort_keys=True) != json.dumps(prev_item, sort_keys=True):
            modified_keys.add(key)
    
    has_changes = added_keys or removed_keys or modified_keys
    
    if has_changes:
        # Load existing manifest
        manifest = load_update_manifest(repo_root)
        existing_updates = manifest.get('updates', [])
        
        # Determine next index number
        next_index = 1
        if existing_updates:
            next_index = max(u.get('index', 0) for u in existing_updates) + 1
        
        # Check if there's already an update for today — replace it
        today_update = None
        for u in existing_updates:
            if u.get('date') == today:
                today_update = u
                next_index = u['index']  # Reuse same index
                break
        
        # Build the daily index file
        additions = []
        for key in added_keys | modified_keys:
            additions.append(current_lookup[key])
        
        removals = []
        for key in removed_keys | modified_keys:
            # For modified items, we treat as remove + re-add
            tmdb_id, media_type = key.rsplit('-', 1)
            removals.append({'id': tmdb_id, 'type': media_type})
        
        daily_file_name = f'index_{next_index}_{today}.json'
        daily_data = {
            'index': next_index,
            'date': today,
            'timestamp': timestamp,
            'items': additions,
            'removals': removals,
            'total_after': len(current_lookup),
        }
        
        write_json(os.path.join(repo_root, 'updates', daily_file_name), daily_data)
        
        # Update manifest
        if today_update:
            today_update['timestamp'] = timestamp
            today_update['file'] = daily_file_name
        else:
            existing_updates.append({
                'index': next_index,
                'date': today,
                'file': daily_file_name,
                'timestamp': timestamp,
            })
        
        manifest['latest_version'] = version
        manifest['updates'] = existing_updates
        write_json(os.path.join(repo_root, 'updates', 'update_manifest.json'), manifest)
        
        print(f'  Added: {len(added_keys)}, Removed: {len(removed_keys)}, Modified: {len(modified_keys)}')
        print(f'  Daily file: updates/{daily_file_name}')
    else:
        # Still write manifest even if no changes (ensure it exists)
        manifest = load_update_manifest(repo_root)
        manifest['latest_version'] = version
        os.makedirs(os.path.join(repo_root, 'updates'), exist_ok=True)
        write_json(os.path.join(repo_root, 'updates', 'update_manifest.json'), manifest)
        print('  No changes detected since last run')

    # ─── Step 4: Categorization summary ───────────────────────────────────
    print('\nStep 4: Category summary (Hollywood = catch-all)...')
    cat_counts = {}
    for cat_name, cat_config in CATEGORIES.items():
        cat_items = [item for item in sorted_items if matches_category(item, cat_config)]
        cat_counts[cat_name] = len(cat_items)
        print(f'  {cat_name:15s}: {len(cat_items):5d} items')
    
    hollywood_items = [item for item in sorted_items if is_hollywood(item)]
    cat_counts['hollywood'] = len(hollywood_items)
    print(f'  {"hollywood":15s}: {len(hollywood_items):5d} items (catch-all)')

    # ─── Step 5: Generate index/home/sections.json ────────────────────────
    print('\nStep 5: Generating home sections...')
    top5 = load_top_content(repo_root, 'Top 5')
    top10 = load_top_content(repo_root, 'Top 10')
    
    # Strip watch links from top content too
    top5 = [strip_watch_link(t) for t in top5]
    top10 = [strip_watch_link(t) for t in top10]

    # Carousel: top5 if available, otherwise first 5
    carousel = (top5 if top5 else [strip_watch_link(i) for i in sorted_items])[:5]

    sections = []
    for sec in HOME_SECTIONS:
        title = sec['title']
        filt = sec['filter']
        limit = sec.get('limit', 20)
        is_ranked = sec.get('is_ranked', False)

        if filt == 'trending':
            sec_items = [strip_watch_link(i) for i in sorted_items[:limit]]
        elif filt == 'top10':
            sec_items = (top10 if top10 else [strip_watch_link(i) for i in sorted_items])[:limit]
        elif filt == 'top_rated':
            rated = sorted(sorted_items, key=lambda x: -(x.get('vote_average') or 0))
            sec_items = [strip_watch_link(i) for i in rated[:limit]]
        elif filt == 'hollywood':
            hw_items = [item for item in sorted_items if is_hollywood(item)]
            sec_items = [strip_watch_link(i) for i in hw_items[:limit]]
        elif filt in CATEGORIES:
            cat_items = [item for item in sorted_items if matches_category(item, CATEGORIES[filt])]
            sec_items = [strip_watch_link(i) for i in cat_items[:limit]]
        else:
            sec_items = [strip_watch_link(i) for i in sorted_items[:limit]]

        if sec_items:
            sections.append({
                'title': title,
                'items': sec_items,
                'is_ranked': is_ranked,
            })

    write_json(os.path.join(repo_root, 'index', 'home', 'sections.json'), {
        'carousel': carousel,
        'sections': sections,
    })
    print(f'  Generated index/home/sections.json ({len(sections)} sections, {len(carousel)} carousel)')

    # ─── Step 5b: Generate index/meta.json ────────────────────────────────
    meta = {
        'version': version,
        'total_items': len(sorted_items),
        'categories': cat_counts,
    }
    write_json(os.path.join(repo_root, 'index', 'meta.json'), meta)
    print(f'  Generated index/meta.json')

    # ─── Step 6: Duplicate detection ──────────────────────────────────────
    print('\nStep 6: Checking for duplicates...')
    detect_duplicates(items, repo_root)

    # ─── Summary ──────────────────────────────────────────────────────────
    print(f'\n{"="*60}')
    print(f'DONE! Index generated successfully.')
    print(f'  Total items: {len(sorted_items)}')
    print(f'  Version: {version}')
    print(f'  Files: base_index.json, updates/, index/')
    print(f'{"="*60}')


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='Generate flat index + incremental updates for DanieWatch')
    parser.add_argument('--repo-root', default='.', help='Root of the database repository')
    args = parser.parse_args()
    generate_index(args.repo_root)
