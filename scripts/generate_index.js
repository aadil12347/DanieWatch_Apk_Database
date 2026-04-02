const fs = require('fs');
const path = require('path');

// TMDB API Setup
const TMDB_API_KEY = process.env.TMDB_API_KEY || 'fc6d85b3839330e3458701b975195487';
const dir = './streaming_links';
const outputFile = './index.json';

// Fetch metadata from TMDB
async function getTMDBMetadata(id, type) {
    if (!id || !TMDB_API_KEY) return null;
    try {
        const url = `https://api.themoviedb.org/3/${type === 'tv' ? 'tv' : 'movie'}/${id}?api_key=${TMDB_API_KEY}`;
        const response = await fetch(url);
        if (!response.ok) return null;
        const data = await response.json();
        
        return {
            country: type === 'tv' ? (data.origin_country || []) : (data.production_countries?.map(c => c.iso_3166_1) || []),
            original_language: data.original_language || "en",
            genres: data.genres?.map(g => g.name) || []
        };
    } catch (err) {
        console.error(`Error fetching TMDB for ${id}:`, err.message);
        return null;
    }
}

async function run() {
    const posts = [];
    
    // Purana data load karo to avoid unnecessary API calls (Cache mechanism)
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

    if (fs.existsSync(dir)) {
        const files = fs.readdirSync(dir);
        console.log(`Scanning ${files.length} files...`);

        for (let i = 0; i < files.length; i++) {
            const file = files[i];
            if (file.endsWith('.json')) {
                const filePath = path.join(dir, file);
                try {
                    const content = JSON.parse(fs.readFileSync(filePath, 'utf8'));
                    
                    let meta = cache.get(content.id);
                    // Agar meta nahi hai ya genres missing hain, to fetch karo
                    if (!meta || !meta.country || !meta.genres || meta.genres.length === 0) {
                        console.log(`[${i + 1}/${files.length}] Fetching metadata: ${content.title}`);
                        const tmdbData = await getTMDBMetadata(content.id, content.type);
                        if (tmdbData) {
                            meta = { ...tmdbData };
                        }
                    }

                    posts.push({
                        id: content.id,
                        type: content.type,
                        title: content.title || "",
                        poster: content.poster || "",
                        year: content.year || "",
                        result: content.result || "HD",
                        language: content.language || ["Hindi"],
                        country: meta?.country || [],
                        original_language: meta?.original_language || "en",
                        genres: meta?.genres || []
                    });
                } catch (err) {
                    console.error(`Error processing ${file}:`, err);
                }
            }
        }
    }

    const finalData = {
        last_updated: new Date().toISOString(),
        total: posts.length,
        posts: posts
    };

    fs.writeFileSync(outputFile, JSON.stringify(finalData, null, 2));
    console.log('✅ index.json successfully updated with country, language, and genres!');
}

run();
