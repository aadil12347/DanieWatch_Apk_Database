const fs = require('fs');
const path = require('path');

// TMDB API Setup
const TMDB_API_KEY = process.env.TMDB_API_KEY || 'fc6d85b3839330e3458701b975195487';
const dir = './streaming_links';
const outputFile = './index.json';
const CONCURRENCY_LIMIT = 10; // Batch size for parallel fetching

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
            genres: data.genres?.map(g => g.name) || [],
            imdb_id: data.external_ids?.imdb_id || data.imdb_id || "",
            seasons: data.seasons || []
        };
    } catch (err) {
        console.error(`Error fetching TMDB for ${id}:`, err.message);
        return null;
    }
}

async function run() {
    const posts = [];
    
    // Load existing data for caching
    let cache = new Map();
    if (fs.existsSync(outputFile)) {
        try {
            const existing = JSON.parse(fs.readFileSync(outputFile, 'utf8'));
            if (existing.posts) {
                existing.posts.forEach(p => cache.set(p.id, p));
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
                let meta = cache.get(content.id);

                // --- NEW LOGIC: Calculate uploaded seasons and episodes ---
                let highestUploadedSeason = 1;
                let totalUploadedEpisodes = 0;
                
                if (content.type === 'tv' && content.seasons) {
                    content.seasons.forEach(s => {
                        if (s.season_number > highestUploadedSeason) {
                            highestUploadedSeason = s.season_number;
                        }
                        totalUploadedEpisodes += s.episodes ? s.episodes.length : 0;
                    });
                }

                // Check if we need to force a TMDB update because a new season was added
                let needsSeasonUpdate = false;
                if (content.type === 'tv' && (!meta || meta.latest_uploaded_season !== highestUploadedSeason)) {
                    needsSeasonUpdate = true;
                }

                // Re-fetch if necessary fields are missing OR if a new season was added
                if (!meta || !meta.country || !meta.genres || meta.genres.length === 0 || meta.imdb_id === undefined || needsSeasonUpdate) {
                    const tmdbData = await getTMDBMetadata(content.id, content.type);
                    if (tmdbData) {
                        meta = { ...meta, ...tmdbData }; // Merge to keep existing meta fields while updating
                    }
                }

                // --- Calculate correct year for TV Shows ---
                let finalYear = content.year || (meta ? meta.year : "");
                
                if (content.type === 'tv' && meta && meta.seasons) {
                    // Try to find the air_date of the highest uploaded season
                    const tmdbSeason = meta.seasons.find(s => s.season_number === highestUploadedSeason);
                    if (tmdbSeason && tmdbSeason.air_date) {
                        finalYear = tmdbSeason.air_date.substring(0, 4);
                    }
                } else if (meta && meta.year) {
                    finalYear = meta.year;
                }

                const entry = {
                    id: content.id,
                    type: content.type,
                    title: content.title || "",
                    poster: content.poster || "",
                    year: finalYear,
                    result: content.result || "HD",
                    language: content.language || ["Hindi"],
                    country: meta?.country || [],
                    original_language: meta?.original_language || "en",
                    genres: meta?.genres || [],
                    imdb_id: meta?.imdb_id || ""
                };

                if (content.type === 'tv') {
                    entry.latest_uploaded_season = highestUploadedSeason;
                    entry.total_uploaded_episodes = totalUploadedEpisodes;
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

    const finalData = {
        last_updated: new Date().toISOString(),
        total: finalPosts.length,
        posts: finalPosts
    };

    fs.writeFileSync(outputFile, JSON.stringify(finalData, null, 2));
    console.log(`✅ index.json successfully updated! Total posts: ${finalPosts.length}`);
}

run();
