const fs = require('fs');
const path = require('path');

const streamingLinksDir = './streaming_links';
const recordFile = './posting_record.json';

// How many days to keep batches before auto-pruning
const MAX_BATCH_AGE_DAYS = 7;

/**
 * Scan streaming_links/ folder and build a map of all current posts.
 * Returns a Map<key, { title, id, type, year }> where key = "type_id"
 */
function scanStreamingLinks() {
    if (!fs.existsSync(streamingLinksDir)) {
        console.error(`❌ ${streamingLinksDir}/ directory not found!`);
        return new Map();
    }

    const files = fs.readdirSync(streamingLinksDir).filter(f => f.endsWith('.json'));
    const postMap = new Map();

    for (const file of files) {
        try {
            const filePath = path.join(streamingLinksDir, file);
            const content = JSON.parse(fs.readFileSync(filePath, 'utf8'));

            if (!content.id || !content.type) continue;

            const key = `${content.type}_${content.id}`;
            postMap.set(key, {
                title: content.title || 'Unknown Title',
                tmdb_id: String(content.id),
                imdb_id: '', // Will be filled if available
                type: content.type,
                year: content.year || '',
            });
        } catch (err) {
            // Skip malformed files silently
        }
    }

    return postMap;
}

/**
 * Load existing posting_record.json.
 */
function loadExistingRecord() {
    if (!fs.existsSync(recordFile)) {
        return null;
    }

    try {
        return JSON.parse(fs.readFileSync(recordFile, 'utf8'));
    } catch (err) {
        console.log('⚠️ Could not parse existing posting_record.json, creating fresh.');
        return null;
    }
}

/**
 * Get the persistent set of all known item keys.
 * This set survives batch pruning — items are never re-detected as "new".
 * 
 * Reads from record.all_known_keys (persistent set) AND all batch posts
 * to build the complete set.
 */
function getKnownKeys(record) {
    const keys = new Set();
    if (!record) return keys;

    // 1. From persistent all_known_keys (survives pruning)
    if (Array.isArray(record.all_known_keys)) {
        for (const key of record.all_known_keys) {
            keys.add(key);
        }
    }

    // 2. From all batch posts (redundant but safe)
    if (record.batches) {
        for (const batch of record.batches) {
            for (const post of (batch.posts || [])) {
                const key = `${post.type}_${post.tmdb_id}`;
                keys.add(key);
            }
        }
    }

    return keys;
}

/**
 * Auto-prune batches older than MAX_BATCH_AGE_DAYS.
 * Returns the number of pruned batches.
 */
function pruneBatches(record) {
    if (!record || !record.batches) return 0;

    const cutoff = new Date();
    cutoff.setDate(cutoff.getDate() - MAX_BATCH_AGE_DAYS);
    const cutoffStr = cutoff.toISOString().split('T')[0]; // "YYYY-MM-DD"

    const before = record.batches.length;
    record.batches = record.batches.filter(b => {
        return b.date_key >= cutoffStr;
    });
    const pruned = before - record.batches.length;

    if (pruned > 0) {
        console.log(`🧹 Auto-pruned ${pruned} batch(es) older than ${MAX_BATCH_AGE_DAYS} days.`);
    }

    return pruned;
}

function run() {
    // ─── Step 1: Scan streaming_links/ for current posts ───
    console.log(`Scanning ${streamingLinksDir}/ for current posts...`);
    const currentPosts = scanStreamingLinks();
    console.log(`  Found ${currentPosts.size} posts in streaming_links/`);

    if (currentPosts.size === 0) {
        console.error('❌ No posts found in streaming_links/. Aborting.');
        return;
    }

    // ─── Step 2: Load existing posting_record.json ───
    let record = loadExistingRecord();

    if (!record) {
        // First run ever — create initial record
        // All current posts are treated as "already known" — no batch created
        const allKeys = Array.from(currentPosts.keys());
        record = {
            last_updated: new Date().toISOString(),
            total_posts: currentPosts.size,
            total_batches: 0,
            all_known_keys: allKeys,
            batches: [],
        };
        fs.writeFileSync(recordFile, JSON.stringify(record, null, 2));
        console.log(`✅ posting_record.json initialized (0 batches, ${currentPosts.size} existing posts tracked).`);
        return;
    }

    // ─── Step 3: Get ALL known keys BEFORE pruning ───
    // This prevents pruned-batch items from re-appearing as "new"
    const knownKeys = getKnownKeys(record);

    // If migrating from old format (no all_known_keys), seed from batches
    if (!record.all_known_keys) {
        console.log('ℹ️ Migrating: seeding all_known_keys from existing batches + current streaming_links...');
        // On migration, treat ALL current items as known (not just batch items)
        // This prevents the entire library from appearing as "new"
        for (const key of currentPosts.keys()) {
            knownKeys.add(key);
        }
    }

    // ─── Step 4: Auto-prune old batches (>7 days) ───
    pruneBatches(record);

    // ─── Step 5: Compute DIFF — find ADDED and REMOVED posts ───
    const currentKeys = new Set(currentPosts.keys());

    // New posts: in streaming_links but not in known keys
    const addedPosts = [];
    for (const [key, post] of currentPosts) {
        if (!knownKeys.has(key)) {
            addedPosts.push(post);
        }
    }

    // Removed posts: in existing batches but no longer in streaming_links
    const removedKeys = new Set();
    if (record.batches) {
        for (const batch of record.batches) {
            for (const post of (batch.posts || [])) {
                const key = `${post.type}_${post.tmdb_id}`;
                if (!currentKeys.has(key)) {
                    removedKeys.add(key);
                }
            }
        }
    }

    // ─── Step 6: Purge removed posts from all batches ───
    if (removedKeys.size > 0) {
        let totalRemoved = 0;
        record.batches.forEach(batch => {
            const before = batch.posts.length;
            batch.posts = batch.posts.filter(p => {
                const key = `${p.type}_${p.tmdb_id}`;
                return !removedKeys.has(key);
            });
            const removed = before - batch.posts.length;
            totalRemoved += removed;
            batch.total_in_batch = batch.posts.length;
        });

        // Remove batches that became empty after purging
        record.batches = record.batches.filter(b => b.posts.length > 0);

        if (totalRemoved > 0) {
            console.log(`🗑️ Purged ${totalRemoved} removed post(s) from posting record.`);
        }
    }

    // ─── Step 7: Update persistent known keys ───
    // Add all current items to the known set (so they never re-appear as new)
    for (const key of currentKeys) {
        knownKeys.add(key);
    }
    // Remove keys that no longer exist in streaming_links
    for (const key of removedKeys) {
        knownKeys.delete(key);
    }

    if (addedPosts.length === 0 && removedKeys.size === 0) {
        console.log('ℹ️ No new or removed posts detected. Posting record unchanged.');
        // Still save in case pruning or migration happened
        record.total_batches = record.batches.length;
        record.total_posts = currentPosts.size;
        record.all_known_keys = Array.from(knownKeys);
        record.last_updated = new Date().toISOString();
        fs.writeFileSync(recordFile, JSON.stringify(record, null, 2));
        return;
    }

    if (addedPosts.length === 0) {
        // Only removals, no new posts — save and exit
        record.total_batches = record.batches.length;
        record.total_posts = currentPosts.size;
        record.all_known_keys = Array.from(knownKeys);
        record.last_updated = new Date().toISOString();
        fs.writeFileSync(recordFile, JSON.stringify(record, null, 2));
        console.log(`   📊 Total batches: ${record.total_batches}`);
        console.log(`   📁 Total posts in database: ${record.total_posts}`);
        return;
    }

    // ─── Step 8: Sort new posts alphabetically (A → Z) by title ───
    addedPosts.sort((a, b) => {
        const titleA = a.title.toLowerCase();
        const titleB = b.title.toLowerCase();
        if (titleA < titleB) return -1;
        if (titleA > titleB) return 1;
        return 0;
    });

    // ─── Step 9: Create or merge into today's batch (1 batch per day) ───
    const now = new Date();
    const todayDate = now.toISOString().split('T')[0]; // "YYYY-MM-DD"
    const todayReadable = now.toLocaleDateString('en-US', {
        year: 'numeric',
        month: 'long',
        day: 'numeric',
    });

    // Check if a batch for today already exists
    const existingBatchIndex = record.batches.findIndex(b => b.date_key === todayDate);

    if (existingBatchIndex !== -1) {
        // Merge new posts into today's existing batch
        const existingBatch = record.batches[existingBatchIndex];

        // Build a set of already-tracked post keys to avoid duplicates
        const existingKeys = new Set(
            existingBatch.posts.map(p => `${p.type}_${p.tmdb_id}`)
        );

        // Filter to only truly new posts (not already in batch)
        const trulyNew = addedPosts.filter(p => {
            const key = `${p.type}_${p.tmdb_id}`;
            return !existingKeys.has(key);
        });

        // PREPEND new posts ABOVE existing ones (don't rearrange existing order)
        existingBatch.posts = [...trulyNew, ...existingBatch.posts];
        existingBatch.total_in_batch = existingBatch.posts.length;
        existingBatch.last_modified = now.toISOString();

        console.log(`✅ Posting record updated! (merged into today's batch)`);
        console.log(`   📦 Batch "${todayReadable}": now ${existingBatch.total_in_batch} post(s) total`);
    } else {
        // Create a new batch for today
        const nextBatchId = (record.batches.length > 0)
            ? Math.max(...record.batches.map(b => b.batch_id || 0)) + 1
            : 1;

        const newBatch = {
            batch_id: nextBatchId,
            date_key: todayDate,
            date: todayReadable,
            timestamp: now.toISOString(),
            total_in_batch: addedPosts.length,
            posts: addedPosts,
        };

        record.batches.unshift(newBatch);
        console.log(`✅ Posting record updated!`);
        console.log(`   📦 Batch "${todayReadable}": ${addedPosts.length} new post(s)`);
    }

    // ─── Step 10: Save ───
    record.total_batches = record.batches.length;
    record.total_posts = currentPosts.size;
    record.all_known_keys = Array.from(knownKeys);
    record.last_updated = now.toISOString();

    fs.writeFileSync(recordFile, JSON.stringify(record, null, 2));

    console.log(`   📊 Total batches: ${record.total_batches}`);
    console.log(`   📁 Total posts in database: ${record.total_posts}`);

    // Print the new posts
    console.log(`\n   New posts added (A→Z):`);
    addedPosts.forEach((p, i) => {
        console.log(`   ${i + 1}. ${p.title} [${p.type.toUpperCase()}] (TMDB: ${p.tmdb_id}, Year: ${p.year || 'N/A'})`);
    });
}

run();
