const fs = require('fs');
const path = require('path');

// TMDB API Setup
const TMDB_API_KEY = process.env.TMDB_API_KEY || 'fc6d85b3839330e3458701b975195487';
const dir = './streaming_links';
const outputFile = './index.json';
const CONCURRENCY_LIMIT = 15; // Batch size for parallel fetching

const REVERSE_GENRE_MAP = {
  'action': 28, 'adventure': 12, 'animation': 16, 'comedy': 35,
  'crime': 80, 'documentary': 99, 'drama': 18, 'family': 10751,
  'fantasy': 14, 'history': 36, 'horror': 27, 'music': 10402,
  'mystery': 9648, 'romance': 10749, 'science fiction': 878, 'sci-fi': 878,
  'thriller': 53, 'war': 10752, 'western': 37,
  'action & adventure': 10759, 'kids': 10762, 'news': 10763,
  'reality': 10764, 'sci-fi & fantasy': 10765, 'soap': 10766,
  'talk': 10767, 'war & politics': 10768
};

// Fetch metadata from TMDB
async function getTMDBMetadata(id, type) {
    if (!id || !TMDB_API_KEY) return null;
    try {
        const url = `https://api.themoviedb.org/3/${type === 'tv' ? 'tv' : 'movie'}/${id}?api_key=${TMDB_API_KEY}&append_to_response=external_ids`;
        const response = await fetch(url);
        if (!response.ok) return null;
        const data = await response.json();
        
        return {
            country: type === 'tv' ? (data.origin_country || []) : (data.production_countries?.map(c => c.iso_3166_1) || []),
            original_language: data.original_language || "en",
            genres: data.genres?.map(g => g.id) || [],
            imdb_id: data.external_ids?.imdb_id || data.imdb_id || "",
            seasons: data.seasons || [],
            release_date: type === 'tv' ? (data.first_air_date || "") : (data.release_date || "")
        };
    } catch (err) {
        console.error(`Error fetching TMDB for ${id}:`, err.message);
        return null;
    }
}

// Fetch the latest episode air date of the specified season from TMDB (episodes <= today)
async function getTMDBSeasonAirDate(tvId, seasonNumber) {
    if (!tvId || !TMDB_API_KEY) return null;
    try {
        const url = `https://api.themoviedb.org/3/tv/${tvId}/season/${seasonNumber}?api_key=${TMDB_API_KEY}`;
        const response = await fetch(url);
        if (!response.ok) return null;
        const data = await response.json();
        
        if (data && data.episodes && Array.isArray(data.episodes)) {
            const todayStr = new Date().toISOString().split('T')[0]; // YYYY-MM-DD
            let latestAirDate = null;
            for (const ep of data.episodes) {
                if (ep.air_date) {
                    if (ep.air_date <= todayStr) {
                        if (!latestAirDate || ep.air_date > latestAirDate) {
                            latestAirDate = ep.air_date;
                        }
                    }
                }
            }
            if (latestAirDate) return latestAirDate;
        }
        return data.air_date || null;
    } catch (err) {
        console.error(`Error fetching TMDB season ${seasonNumber} for TV ${tvId}:`, err.message);
        return null;
    }
}

// Helper functions for sorting matching generate_index.py logic
function isAccurateDate(dateStr) {
    if (!dateStr) return false;
    const match = dateStr.match(/^(\d{4})-(\d{2})-(\d{2})/);
    if (!match) return false;
    const month = parseInt(match[2], 10);
    const day = parseInt(match[3], 10);
    return month >= 1 && month <= 12 && day >= 1 && day <= 31;
}

function parseDateToTimestamp(dateStr) {
    if (!dateStr) return 0;
    const cleaned = dateStr.trim();
    let d = new Date(cleaned);
    if (!isNaN(d.getTime())) {
        return Math.floor(d.getTime() / 1000);
    }
    const match = cleaned.match(/^(\d{4})-(\d{2})-(\d{2})/);
    if (match) {
        const y = parseInt(match[1], 10);
        const m = parseInt(match[2], 10) - 1;
        const day = parseInt(match[3], 10);
        d = new Date(y, m, day);
        if (!isNaN(d.getTime())) {
            return Math.floor(d.getTime() / 1000);
        }
    }
    return 0;
}

function extractYear(title, filename) {
    let match = (title || "").match(/\((\d{4})\)/);
    if (match) return parseInt(match[1], 10);
    if (filename) {
        match = filename.match(/\((\d{4})\)/);
        if (match) return parseInt(match[1], 10);
    }
    match = (title || "").match(/\b(19\d{2}|20\d{2})\b/);
    if (match) return parseInt(match[1], 10);
    if (filename) {
        match = filename.match(/\b(19\d{2}|20\d{2})\b/);
        if (match) return parseInt(match[1], 10);
    }
    return 0;
}

function getSortKey(item) {
    const airedDate = item[8] || "";
    const accurate = isAccurateDate(airedDate);
    let year = 0;
    let timestamp = 0;
    if (accurate) {
        const match = airedDate.match(/^(\d{4})/);
        year = parseInt(match[1], 10);
        timestamp = parseDateToTimestamp(airedDate);
    } else {
        const matchYear = airedDate.match(/^(\d{4})/);
        if (matchYear) {
            year = parseInt(matchYear[1], 10);
        } else {
            year = extractYear(item[1]);
        }
        timestamp = 0;
    }
    
    const code = item[0];
    let idSort = 0;
    if (typeof code === 'number') {
        idSort = -code;
    } else if (typeof code === 'string' && code.startsWith('tt')) {
        const parsed = parseInt(code.substring(2), 10);
        if (!isNaN(parsed)) {
            idSort = -parsed;
        }
    }
    return {
        year: year,
        accurate: accurate ? 0 : 1,
        timestamp: timestamp,
        idSort: idSort
    };
}


// Convert a cached positional array back into a metadata object for easy merging/checking
function getMetaFromCached(p) {
    if (!p || !Array.isArray(p)) return null;
    return {
        id: p[0],
        title: p[1],
        type: p[2],
        original_language: p[3],
        country: p[4],
        language: p[5],
        genres: p[6],
        imdb_id: p[7],
        release_date: p[8],
        latest_uploaded_season: p[9],
        total_uploaded_episodes: p[10]
    };
}

async function run() {
    const posts = [];
    
    // Load existing data for caching
    let cache = new Map();
    if (fs.existsSync(outputFile)) {
        try {
            const existing = JSON.parse(fs.readFileSync(outputFile, 'utf8'));
            if (Array.isArray(existing)) {
                if (existing.length > 0 && !Array.isArray(existing[0])) {
                    // Handle raw list of objects format (like on remote branch)
                    existing.forEach(p => {
                        const genreIds = (p.genres || p.genre_ids || []).map(g => {
                            if (typeof g === 'number') return g;
                            if (typeof g === 'string') {
                                return REVERSE_GENRE_MAP[g.toLowerCase().trim()] || null;
                            }
                            return null;
                        }).filter(g => g !== null);
                        
                        const idVal = p.id || p.tmdb_id;
                        const typeVal = p.type || p.media_type || 'movie';
                        const releaseDateVal = p.release_date || p.aired_date || (p.year ? `${p.year}-01-01` : '');

                        const arr = [
                            idVal,
                            p.title,
                            typeVal,
                            p.original_language || 'en',
                            p.country || p.origin_country || [],
                            p.language || p.languages || ['Hindi'],
                            genreIds,
                            p.imdb_id || '',
                            releaseDateVal,
                        ];
                        if (typeVal === 'tv' || typeVal === 'series') {
                            arr[9] = p.latest_uploaded_season || 1;
                            arr[10] = p.total_uploaded_episodes || 0;
                        }
                        cache.set(String(idVal), arr);
                    });
                } else {
                    // Positional array format
                    existing.forEach(p => {
                        if (Array.isArray(p) && p.length > 0) {
                            cache.set(String(p[0]), p);
                        }
                    });
                }
            } else if (existing && existing.posts) {
                // Handle old wrapped object format if present — convert string genres to IDs
                existing.posts.forEach(p => {
                    const genreIds = (p.genres || []).map(g => {
                        if (typeof g === 'number') return g;
                        if (typeof g === 'string') {
                            return REVERSE_GENRE_MAP[g.toLowerCase().trim()] || null;
                        }
                        return null;
                    }).filter(g => g !== null);
                    
                    const arr = [
                        p.id,
                        p.title,
                        p.type || 'movie',
                        p.original_language || 'en',
                        p.country || [],
                        p.language || ['Hindi'],
                        genreIds,
                        p.imdb_id || '',
                        p.release_date || (p.year ? `${p.year}-01-01` : ''),
                    ];
                    if (p.type === 'tv') {
                        arr[9] = p.latest_uploaded_season || 1;
                        arr[10] = p.total_uploaded_episodes || 0;
                    }
                    cache.set(String(p.id), arr);
                });
            }
        } catch (e) {
            console.log("Starting with fresh index...");
        }
    }

    if (!fs.existsSync(dir)) {
        console.error(`Directory ${dir} not found!`);
        return;
    }

    const files = fs.readdirSync(dir).filter(f => f.endsWith('.json'));
    console.log(`Scanning ${files.length} files...`);

    const results = new Array(files.length);
    
    // Process files in batches to respect rate limits and speed up execution
    for (let i = 0; i < files.length; i += CONCURRENCY_LIMIT) {
        const batch = files.slice(i, i + CONCURRENCY_LIMIT);
        console.log(`Processing batch ${Math.floor(i/CONCURRENCY_LIMIT) + 1}/${Math.ceil(files.length/CONCURRENCY_LIMIT)}...`);
        
        const batchPromises = batch.map(async (file, indexInBatch) => {
            const globalIndex = i + indexInBatch;
            const filePath = path.join(dir, file);
            try {
                const content = JSON.parse(fs.readFileSync(filePath, 'utf8'));
                
                // Support both old and new schemas
                const idVal = content.id || content.tmdb_id;
                const tmdbId = parseInt(idVal, 10);
                if (isNaN(tmdbId)) {
                    console.log(`⚠️ Skipping ${file}: invalid TMDB ID '${idVal}'`);
                    return;
                }

                const title = content.title || content.post_title || "";
                const type = content.type || content.post_type || "movie";
                const languages = content.language || content.languages || ["Hindi"];

                const cachedArray = cache.get(String(tmdbId));
                let meta = getMetaFromCached(cachedArray);

                // --- Calculate uploaded seasons and episodes ---
                let highestUploadedSeason = 1;
                let totalUploadedEpisodes = 0;
                
                if (type === 'tv' || type === 'series') {
                    if (content.seasons) {
                        if (Array.isArray(content.seasons)) {
                            // Old format: seasons is a List of objects
                            content.seasons.forEach(s => {
                                const sNum = parseInt(s.season_number, 10);
                                if (!isNaN(sNum) && sNum > highestUploadedSeason) {
                                    highestUploadedSeason = sNum;
                                }
                                totalUploadedEpisodes += s.episodes ? s.episodes.length : 0;
                            });
                        } else if (typeof content.seasons === 'object') {
                            // New format: seasons is a Map where keys are season numbers ("01", "02")
                            Object.keys(content.seasons).forEach(sStr => {
                                const sNum = parseInt(sStr, 10);
                                if (!isNaN(sNum) && sNum > highestUploadedSeason) {
                                    highestUploadedSeason = sNum;
                                }
                                const seasonData = content.seasons[sStr];
                                if (seasonData) {
                                    // Episodes can be under quality folders like "480p", "720p", "1080p"
                                    const episodeTitles = new Set();
                                    Object.keys(seasonData).forEach(quality => {
                                        const episodes = seasonData[quality];
                                        if (Array.isArray(episodes)) {
                                            episodes.forEach(ep => {
                                                if (ep.episode_title) {
                                                    episodeTitles.add(ep.episode_title);
                                                }
                                            });
                                        }
                                    });
                                    totalUploadedEpisodes += episodeTitles.size;
                                }
                            });
                        }
                    }
                }

                // Check if we need to force a TMDB update because a new season or episode was added, or if cached metadata is incomplete
                let needsTMDBUpdate = false;
                if (!meta || !meta.country || !meta.genres || meta.genres.length === 0 || meta.imdb_id === undefined || !meta.release_date) {
                    needsTMDBUpdate = true;
                } else if (type === 'tv' || type === 'series') {
                    if (meta.latest_uploaded_season !== highestUploadedSeason || meta.total_uploaded_episodes !== totalUploadedEpisodes) {
                        needsTMDBUpdate = true;
                    }
                }

                // Re-fetch if necessary fields are missing OR if a new season/episode was added
                if (needsTMDBUpdate) {
                    const tmdbData = await getTMDBMetadata(tmdbId, type);
                    if (tmdbData) {
                        meta = { ...meta, ...tmdbData }; // Merge to keep existing meta fields while updating
                    }
                }

                // Calculate correct year/date for TV Shows and Movies
                let finalReleaseDate = meta?.release_date || "";
                if ((type === 'tv' || type === 'series') && meta) {
                    if (needsTMDBUpdate) {
                        // Fetch the latest episode air date for the highest uploaded season
                        const seasonAirDate = await getTMDBSeasonAirDate(tmdbId, highestUploadedSeason);
                        const currentTime = new Date().toTimeString().split(' ')[0]; // HH:MM:SS
                        if (seasonAirDate) {
                            finalReleaseDate = `${seasonAirDate} ${currentTime}`;
                        } else {
                            // Fallback to the season's air_date from TV show details
                            const tmdbSeason = meta.seasons ? meta.seasons.find(s => s.season_number === highestUploadedSeason) : null;
                            if (tmdbSeason && tmdbSeason.air_date) {
                                finalReleaseDate = `${tmdbSeason.air_date} ${currentTime}`;
                            } else if (meta.release_date) {
                                finalReleaseDate = `${meta.release_date.split(' ')[0]} ${currentTime}`;
                            }
                        }
                    } else {
                        // Preserve the cached release date exactly (with its original date & time)
                        finalReleaseDate = meta.release_date || "";
                    }
                } else {
                    // For movies
                    if (needsTMDBUpdate) {
                        if (meta && meta.release_date) {
                            const currentTime = new Date().toTimeString().split(' ')[0];
                            finalReleaseDate = `${meta.release_date.split(' ')[0]} ${currentTime}`;
                        } else if (content.year) {
                            finalReleaseDate = `${content.year}-01-01 12:00:00`;
                        }
                    } else {
                        finalReleaseDate = meta?.release_date || "";
                    }
                }

                // Construct positional array
                const entry = [
                    tmdbId,                                                 // 0: id
                    title,                                                  // 1: title
                    (type === 'series' ? 'tv' : type),                      // 2: type
                    meta?.original_language || "en",                        // 3: original_language
                    meta?.country || [],                                    // 4: country
                    languages,                                              // 5: language
                    meta?.genres || [],                                     // 6: genres (ids)
                    meta?.imdb_id || content.imdb_id || "",                 // 7: imdb_id
                    finalReleaseDate,                                       // 8: release_date
                ];

                if (type === 'tv' || type === 'series') {
                    entry[9] = highestUploadedSeason;                       // 9: latest_uploaded_season
                    entry[10] = totalUploadedEpisodes;                      // 10: total_uploaded_episodes
                }

                results[globalIndex] = entry;
            } catch (err) {
                console.error(`Error processing ${file}:`, err);
            }
        });

        await Promise.all(batchPromises);
    }

    // Filter out any failed entries
    const finalPosts = results.filter(p => p !== undefined);

    // Sort finalPosts using the same custom sorting logic as the Python script
    finalPosts.sort((a, b) => {
        const keyA = getSortKey(a);
        const keyB = getSortKey(b);
        
        if (keyA.year !== keyB.year) {
            return keyB.year - keyA.year; // year descending
        }
        if (keyA.accurate !== keyB.accurate) {
            return keyA.accurate - keyB.accurate; // accurate first (0 < 1)
        }
        if (keyA.timestamp !== keyB.timestamp) {
            return keyB.timestamp - keyA.timestamp; // timestamp descending
        }
        return keyB.idSort - keyA.idSort; // code descending
    });

    // Write index.json with one post array per line
    const lines = finalPosts.map(post => JSON.stringify(post));
    const jsonString = "[\n  " + lines.join(",\n  ") + "\n]";
    fs.writeFileSync(outputFile, jsonString, 'utf8');
    console.log(`✅ index.json successfully updated! Total posts: ${finalPosts.length}`);

    // Copy to local Flutter app assets folder if it exists
    const localFlutterAssetPath = 'C:\\Users\\mdani\\Desktop\\DanieWatch Apk VidEasy\\assets\\base_index.json';
    if (fs.existsSync(path.dirname(localFlutterAssetPath))) {
        try {
            fs.copyFileSync(outputFile, localFlutterAssetPath);
            console.log(`📋 Successfully copied index.json to Flutter app assets: ${localFlutterAssetPath}`);
        } catch (copyErr) {
            console.log(`⚠️ Note: Could not copy to Flutter app assets: ${copyErr.message}`);
        }
    }
}

run();
