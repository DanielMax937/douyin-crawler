#!/usr/bin/env node

/**
 * Open browser only — for login or profile setup.
 * Uses the same profile as douyin-scraper.js so logins/settings are reused.
 *
 * Usage: node open-browser.js
 *
 * Environment Variables:
 * - BROWSER_USER_DATA_DIR: Chrome profile directory (default: OS temp/douyin-scraper-user-data)
 */

const { chromium } = require('patchright');
const path = require('path');
const os = require('os');
const fs = require('fs');
const readline = require('readline');

const USER_DATA_DIR = process.env.BROWSER_USER_DATA_DIR || path.join(os.tmpdir(), 'douyin-scraper-user-data');

async function main() {
  console.log('🌐 Opening browser (same profile as scraper)...');
  console.log(`   📂 Profile: ${USER_DATA_DIR}`);
  console.log('   Log in or change settings, then press Enter in this terminal to close.\n');

  if (!fs.existsSync(USER_DATA_DIR)) {
    fs.mkdirSync(USER_DATA_DIR, { recursive: true });
  }

  const context = await chromium.launchPersistentContext(USER_DATA_DIR, {
    channel: 'chrome',
    headless: false,
    viewport: null,
  });

  try {
    await context.grantPermissions(['clipboard-read', 'clipboard-write']);
  } catch (e) {
    // ignore
  }

  const pages = context.pages();
  if (pages.length === 0) {
    await context.newPage();
  }

  const rl = readline.createInterface({ input: process.stdin, output: process.stdout });
  await new Promise((resolve) => {
    rl.question('Press Enter to close browser... ', () => {
      rl.close();
      resolve();
    });
  });

  await context.close();
  console.log('✅ Browser closed.');
}

main().catch((err) => {
  console.error('❌', err.message);
  process.exit(1);
});
