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
        console.log(`🚀 Updating index.json...`);
        
        exec(`node ${scriptPath}`, (error, stdout, stderr) => {
            if (error) {
                console.error(`❌ Error updating index: ${error.message}`);
                return;
            }
            if (stderr) {
                console.error(`⚠️ Script status: ${stderr}`);
            }
            console.log(stdout.trim());
        });
    }, 500); // Wait 500ms for more changes before running
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
