const fs = require('fs');
const { exec } = require('child_process');
const path = require('path');

const watchDir = './streaming_links';
const scriptPath = './scripts/generate_index.js';

let debounceTimer;

console.log(`👀 Watching for changes in ${watchDir}...`);

// Function to trigger the index generation
function triggerUpdate(eventType, filename) {
    if (!filename || !filename.endsWith('.json')) return;

    // Debounce the update to avoid multiple runs when multiple files change at once
    clearTimeout(debounceTimer);
    debounceTimer = setTimeout(() => {
        console.log(`\n🔔 Change detected: ${filename} (${eventType})`);
        console.log(`🚀 Starting update pipeline...`);

        // 1. Create old index snapshot
        const indexFile = './index.json';
        const snapshotFile = './old_index_snapshot.json';
        if (fs.existsSync(indexFile)) {
            fs.copyFileSync(indexFile, snapshotFile);
        } else {
            fs.writeFileSync(snapshotFile, '[]', 'utf8');
        }

        // 2. Generate New Index
        console.log(`👉 Running generate_index.js...`);
        exec(`node ./scripts/generate_index.js`, (error, stdout, stderr) => {
            if (error) {
                console.error(`❌ Error updating index: ${error.message}`);
                cleanupSnapshot();
                return;
            }
            console.log(stdout.trim());

            // 3. Generate Posting Record
            console.log(`👉 Running generate_posting_record.js...`);
            exec(`node ./scripts/generate_posting_record.js`, (errorPR, stdoutPR, stderrPR) => {
                if (errorPR) {
                    console.error(`❌ Error updating posting record: ${errorPR.message}`);
                } else {
                    console.log(stdoutPR.trim());
                }

                // 4. Find Duplicates
                console.log(`👉 Running find_duplicates.js...`);
                exec(`node ./scripts/find_duplicates.js`, (errorDup, stdoutDup, stderrDup) => {
                    if (errorDup) {
                        console.error(`❌ Error finding duplicates: ${errorDup.message}`);
                    } else {
                        console.log(stdoutDup.trim());
                    }

                    // 5. Cleanup
                    cleanupSnapshot();
                });
            });
        });
    }, 500); // Wait 500ms for more changes before running
}

function cleanupSnapshot() {
    const snapshotFile = './old_index_snapshot.json';
    if (fs.existsSync(snapshotFile)) {
        try {
            fs.unlinkSync(snapshotFile);
            console.log(`🧹 Cleaned up old_index_snapshot.json`);
        } catch (e) {
            console.error(`⚠️ Could not remove old_index_snapshot.json: ${e.message}`);
        }
    }
}

// Watch the directory
// Note: recursive watch is available on Windows and macOS
try {
    fs.watch(watchDir, { recursive: false }, (eventType, filename) => {
        triggerUpdate(eventType, filename);
    });
} catch (err) {
    console.error(`❌ Watcher failed: ${err.message}`);
}
