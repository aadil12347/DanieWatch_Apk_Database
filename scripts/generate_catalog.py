#!/usr/bin/env python3
"""
Generate Paginated Catalog from streaming_links (DanieWatch format).

Pipeline:
  1. Scan streaming_links/ folder (sole source of truth)
  2. Fetch exact release dates from TMDB API (cached to avoid re-fetching)
  3. Apply sorting overrides from sorting/ folder (manual order takes priority)
  4. Auto-sort remaining items: year DESC → release_date DESC → id DESC
  5. Categorize using ONLY origin_country + original_language (NOT dubbing language)
  6. Paginate into catalog/ files
  7. Update sorting/ override files (prepend new items, preserve manual order)

Output:
  catalog/
    meta.json              — version + page counts
    search_index.json      — lightweight search data
    home/sections.json     — pre-built home screen data
    all/page_N.json        — paginated global catalog
    indian/page_N.json     — paginated category pages
    ...

  sorting/
    all.json               — sort overrides for Explore/All
    indian.json            — sort overrides for Indian
    hollywood.json         — sort overrides for Hollywood
    ...

Usage:
    python generate_catalog.py [--repo-root .] [--output-dir ./catalog] [--page-size 50]
"""

import json
import os
import sys
import glob
import time
from datetime import datetime, timezone
from typing import Any

PAGE_SIZE = 50

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
    'hollywood': {
        'countries': ['US', 'GB', 'UK', 'AU', 'CA'],
        'languages': ['en', 'english'],
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


# ═══════════════════════════════════════════════════════════════════════════════
# Data Loading
# ═══════════════════════════════════════════════════════════════════════════════

def load_streaming_links(repo_root: str) -> list[dict]:
    """Scan streaming_links/ folder — each JSON file is one movie/show (source of truth)."""
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
                    # Some files might contain arrays
                    for sub_item in item:
                        if isinstance(sub_item, dict) and sub_item.get('id'):
                            items.append(sub_item)
        except (json.JSONDecodeError, IOError) as e:
            errors += 1
    
    if errors > 0:
        print(f'  WARNING: {errors} files failed to parse in streaming_links/')
    return items


def load_posting_record(repo_root: str) -> dict[str, int]:
    """Load posting_record.json for batch ordering."""
    pr_path = os.path.join(repo_root, 'posting_record.json')
    if not os.path.exists(pr_path):
        return {}

    with open(pr_path, 'r', encoding='utf-8') as f:
        data = json.load(f)

    priorities: dict[str, int] = {}
    batches = data if isinstance(data, list) else data.get('batches', data.get('items', []))

    if isinstance(batches, list):
        for batch_idx, batch in enumerate(reversed(batches)):
            batch_items = batch.get('items', batch.get('posts', [])) if isinstance(batch, dict) else []
            for item_idx, item in enumerate(batch_items):
                tmdb_id = item.get('tmdb_id') or item.get('id')
                media_type = item.get('type') or item.get('media_type', 'movie')
                if tmdb_id:
                    key = f"{tmdb_id}-{media_type}"
                    if key not in priorities:
                        priorities[key] = batch_idx * 1000 + item_idx

    return priorities


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


# ═══════════════════════════════════════════════════════════════════════════════
# Sorting Logic
# ═══════════════════════════════════════════════════════════════════════════════

def get_sort_key(item: dict, priorities: dict[str, int]) -> tuple:
    """Generate sort key: release_date (or sort_date/added_date) -> year -> priority -> id"""
    year = safe_int(item.get('year') or item.get('release_year') or 0)
    
    # Parse date for sub-year ordering
    date_str = item.get('release_date') or item.get('added_date') or item.get('sort_date') or ''
    date_sortable = 0
    if date_str and len(date_str) >= 10:
        try:
            date_sortable = int(date_str[:10].replace('-', ''))
        except ValueError:
            pass
    
    item_id = safe_int(item.get('id') or 0)
    media_type = item.get('type') or item.get('media_type', 'movie')
    key = f"{item.get('id')}-{media_type}"
    priority = priorities.get(key, 999999)
    
    return (-date_sortable, -year, priority, -item_id)



# ═══════════════════════════════════════════════════════════════════════════════
# Category Matching
# ═══════════════════════════════════════════════════════════════════════════════

def has_category_metadata(item: dict) -> bool:
    """Check if item has enough metadata for category placement.
    
    Items without any language or country info go to Explore only.
    """
    has_orig_lang = bool((item.get('original_language') or '').strip())
    has_country = bool(item.get('country') or item.get('origin_country'))
    # Fallback: streaming_links use 'language' for audio/dubbing language
    has_lang = bool(item.get('language'))
    lang = item.get('language')
    if isinstance(lang, list) and len(lang) > 0:
        has_lang = True
    elif isinstance(lang, str) and lang.strip():
        has_lang = True
    return has_orig_lang or has_country or has_lang


def matches_category(item: dict, cat_config: dict) -> bool:
    """Check if item belongs to a category.
    
    Uses origin_country and original_language when available.
    Falls back to the 'language' field (dubbing/audio) from streaming_links
    when origin metadata is not available.
    """
    countries = [c.upper() for c in cat_config.get('countries', [])]
    languages = [l.lower() for l in cat_config.get('languages', [])]
    genre_names = [g.lower() for g in cat_config.get('genres', [])]

    # Item fields — origin metadata (preferred)
    item_countries = item.get('country') or item.get('origin_country') or []
    if isinstance(item_countries, str):
        item_countries = [item_countries]
    item_countries = [c.upper() for c in item_countries]

    item_orig_lang = (item.get('original_language') or '').lower().strip()

    # Country match (origin_country only)
    if countries and any(c in countries for c in item_countries):
        return True

    # Original language match
    if languages and item_orig_lang and item_orig_lang in languages:
        return True

    # Fallback: use the 'language' field from streaming_links when no origin metadata
    if not item_orig_lang and not item_countries:
        item_lang = item.get('language') or []
        if isinstance(item_lang, str):
            item_lang = [item_lang]
        item_lang_lower = [l.lower().strip() for l in item_lang if isinstance(l, str)]
        
        if languages and any(l in languages for l in item_lang_lower):
            return True

    # Genre match (for anime: also require Japanese original_language or language)
    if genre_names:
        item_genres = item.get('genres') or []
        if isinstance(item_genres, str):
            item_genres = [item_genres]
        item_genres_lower = [g.lower().strip() for g in item_genres]

        if any(g in genre_names for g in item_genres_lower):
            if 'japanese' in languages or 'ja' in languages:
                if item_orig_lang in ('ja', 'japanese'):
                    return True
                # Fallback for streaming_links
                item_lang = item.get('language') or []
                if isinstance(item_lang, str):
                    item_lang = [item_lang]
                item_lang_lower = [l.lower().strip() for l in item_lang]
                return any(l in ('ja', 'japanese') for l in item_lang_lower)
            return True

    return False


# ═══════════════════════════════════════════════════════════════════════════════
# Pagination & Output
# ═══════════════════════════════════════════════════════════════════════════════

def paginate(items: list[dict], page_size: int) -> list[list[dict]]:
    """Split items into pages."""
    if not items:
        return [[]]
    pages = []
    for i in range(0, len(items), page_size):
        pages.append(items[i:i + page_size])
    return pages


def write_json(path: str, data: Any):
    """Write JSON file, creating dirs as needed."""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, separators=(',', ':'))


# ═══════════════════════════════════════════════════════════════════════════════
# Main Catalog Generation
# ═══════════════════════════════════════════════════════════════════════════════

def generate_catalog(repo_root: str, output_dir: str, page_size: int = PAGE_SIZE):
    """Generate the full paginated catalog."""

    # ─── Step 1: Load items from streaming_links/ (sole source of truth) ──
    print('Loading items...')
    items = load_streaming_links(repo_root)
    print(f'  streaming_links/: {len(items)} items')

    if not items:
        print('ERROR: No items found!', file=sys.stderr)
        sys.exit(1)

    # Filter out non-numeric IDs (can't be used as TMDB IDs)
    valid_items = [item for item in items if safe_int(item.get('id')) > 0]
    print(f'  {len(valid_items)} items with valid numeric IDs (skipped {len(items) - len(valid_items)})')
    items = valid_items

    # ─── Step 2: Load posting record ──────────────────────────────────────
    print('Loading posting record...')
    priorities = load_posting_record(repo_root)
    print(f'  {len(priorities)} batch-prioritized items')

    # ─── Step 3: Sort all items globally ──────────────────────────────────
    print('Sorting items...')
    sorted_items = sorted(items, key=lambda item: get_sort_key(item, priorities))
    
    # Count items with/without dates for info
    with_date = sum(1 for item in items if item.get('release_date') or item.get('added_date') or item.get('sort_date'))
    without_date = len(items) - with_date
    print(f'  {with_date} items with date metadata, {without_date} without')

    # ─── Step 4: Paginate all categories ──────────────────────────────────
    page_counts: dict[str, int] = {}

    # --- Global (all) ---
    print('Generating all/ pages...')
    all_pages = paginate(sorted_items, page_size)
    page_counts['all'] = len(all_pages)
    for i, page_items in enumerate(all_pages):
        write_json(os.path.join(output_dir, 'all', f'page_{i+1}.json'), {
            'page': i + 1,
            'total_pages': len(all_pages),
            'total_items': len(sorted_items),
            'items': page_items,
        })
    print(f'  all: {len(all_pages)} pages ({len(sorted_items)} items)')

    # --- Category pages ---
    for cat_name, cat_config in CATEGORIES.items():
        cat_items = [
            item for item in sorted_items
            if has_category_metadata(item) and matches_category(item, cat_config)
        ]
        
        cat_pages = paginate(cat_items, page_size)
        page_counts[cat_name] = len(cat_pages)
        for i, page_items in enumerate(cat_pages):
            write_json(os.path.join(output_dir, cat_name, f'page_{i+1}.json'), {
                'page': i + 1,
                'total_pages': len(cat_pages),
                'total_items': len(cat_items),
                'items': page_items,
            })
        print(f'  {cat_name}: {len(cat_pages)} pages ({len(cat_items)} items)')

    # ─── Step 5: meta.json ────────────────────────────────────────────────
    version = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')
    meta = {
        'version': version,
        'total_items': len(sorted_items),
        'page_size': page_size,
        'pages': page_counts,
    }
    write_json(os.path.join(output_dir, 'meta.json'), meta)
    print(f'Generated meta.json (version: {version})')

    # ─── Step 6: search_index.json ────────────────────────────────────────
    search_index = []
    for item in sorted_items:
        lang = item.get('language') or []
        if isinstance(lang, str):
            lang = [lang]
        if not lang and item.get('original_language'):
            lang = [item['original_language']]
        search_index.append({
            'i': safe_int(item.get('id')),
            't': item.get('title', ''),
            'm': item.get('type') or item.get('media_type', 'movie'),
            'l': lang,
        })
    write_json(os.path.join(output_dir, 'search_index.json'), search_index)
    print(f'Generated search_index.json ({len(search_index)} entries)')

    # ─── Step 7: home/sections.json ───────────────────────────────────────
    print('Generating home sections...')
    top5 = load_top_content(repo_root, 'Top 5')
    top10 = load_top_content(repo_root, 'Top 10')

    # Carousel: first 5 sorted items (or top5 if available)
    carousel = (top5 if top5 else sorted_items)[:5]

    sections = []
    for sec in HOME_SECTIONS:
        title = sec['title']
        filt = sec['filter']
        limit = sec.get('limit', 20)
        is_ranked = sec.get('is_ranked', False)

        if filt == 'trending':
            sec_items = sorted_items[:limit]
        elif filt == 'top10':
            sec_items = (top10 if top10 else sorted_items)[:limit]
        elif filt == 'top_rated':
            rated = sorted(sorted_items, key=lambda x: -(x.get('vote_average') or 0))
            sec_items = rated[:limit]
        elif filt in CATEGORIES:
            cat_items = [
                item for item in sorted_items
                if has_category_metadata(item) and matches_category(item, CATEGORIES[filt])
            ]
            sec_items = cat_items[:limit]
        else:
            sec_items = sorted_items[:limit]

        if sec_items:
            sections.append({
                'title': title,
                'items': sec_items,
                'is_ranked': is_ranked,
            })

    write_json(os.path.join(output_dir, 'home', 'sections.json'), {
        'carousel': carousel,
        'sections': sections,
    })
    print(f'Generated home/sections.json ({len(sections)} sections, {len(carousel)} carousel)')

    total_pages = sum(page_counts.values())
    print(f'\nDONE! Catalog generated: {len(sorted_items)} items, {total_pages} total pages')
    
    # Print category summary
    print('\n-- Category Summary --')
    for cat, count in page_counts.items():
        # Get actual count
        if cat == 'all':
            actual = len(sorted_items)
        else:
            actual = len([
                item for item in sorted_items
                if has_category_metadata(item) and matches_category(item, CATEGORIES.get(cat, {}))
            ]) if cat in CATEGORIES else 0
        print(f'  {cat:15s}: {actual:5d} items ({count} pages)')


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='Generate paginated catalog for DanieWatch')
    parser.add_argument('--repo-root', default='.', help='Root of the database repository')
    parser.add_argument('--output-dir', default='./catalog', help='Output directory')
    parser.add_argument('--page-size', type=int, default=PAGE_SIZE, help='Items per page')
    args = parser.parse_args()
    generate_catalog(args.repo_root, args.output_dir, args.page_size)
