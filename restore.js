#!/usr/bin/env node

const readline = require('node:readline');
const fs = require('node:fs');
const zlib = require('node:zlib');

// Parse command-line arguments
const args = process.argv.slice(2);

if (args.length < 2) {
  console.error('Usage: node script.js <tsvArchive> <eventServer>');
  process.exit(1);
}

const [tsvArchive, eventServer] = args;

if (!fs.existsSync(tsvArchive)) {
  console.error(`Error: TSV archive file "${tsvArchive}" does not exist.`);
  process.exit(1);
}

async function run() {
  const rl = readline.createInterface({
    input: fs.createReadStream(tsvArchive).pipe(zlib.createGunzip()),
    crlfDelay: Infinity,
  });

  let eventsSent = 0;
  for await (const line of rl) {
    const [_id, timestamp, path, payload] = line.split('\t');
    while (true) {
      try {
        const res = await fetch(eventServer + path, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json', 'X-Original-Timestamp': timestamp, },
          body: payload,
        });
        if (res.status !== 200) {
          console.error(`Failed to POST event - tsv line number: ${eventsSent}, code ${res.status}, ${path} - ${payload.slice(0, 100)}...`);
          throw new Error(`Failed to POST event: code ${res.status}`);
        } else {
          break;
        }
      } catch (error) {
        console.error(`Retrying in 2 seconds..`);
        await new Promise(resolve => setTimeout(resolve, 2000));
      }
    }
    
    eventsSent++;
    // Log progress every 100 events
    if (eventsSent % 100 === 0) {
      console.log(`Sent ${eventsSent} events`);
    }
  }
  rl.close();
}

run().then(() => {
  console.log('Completed');
  process.exit(0);
}).catch(error => {
  console.error(error);
  process.exit(1);
});
