const fs = require('fs');
const path = require('path');

const dir = './streaming_links';
const outputFile = './index.json';

const posts = [];

// Folder scan karo
if (fs.existsSync(dir)) {
    const files = fs.readdirSync(dir);
    
    files.forEach(file => {
        if (file.endsWith('.json')) {
            const filePath = path.join(dir, file);
            try {
                const content = JSON.parse(fs.readFileSync(filePath, 'utf8'));
                // Index file ko chota rakhne ke liye sirf zaroori data lo
                posts.push({
                    id: content.id,
                    type: content.type,
                    title: content.title || "",
                    poster: content.poster || "",
                    year: content.year || "",
                    result: content.result || "HD",
                    language: content.language || ["Hindi"]
                });
            } catch (err) {
                console.error(`Error parsing ${file}:`, err);
            }
        }
    });
}

// Latest entries ko upar rakhne ke liye (Optional: agar date ho)
// posts.reverse(); 

const finalData = {
    last_updated: new Date().toISOString(),
    total: posts.length,
    posts: posts
};

fs.writeFileSync(outputFile, JSON.stringify(finalData, null, 2));
console.log('✅ index.json successfully updated!');
