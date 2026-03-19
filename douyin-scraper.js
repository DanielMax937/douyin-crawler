#!/usr/bin/env node

/**
 * Douyin Video Scraper
 *
 * Scrapes video information from Douyin including:
 * - Video title
 * - Like, comment, favorite, share counts
 * - Comments with like counts
 * - Share link
 *
 * Usage:
 *   node douyin-scraper.js [videoCount]
 *   node douyin-scraper.js --url <douyin_video_url>
 * Example:
 *   node douyin-scraper.js 3
 *   node douyin-scraper.js --url https://www.douyin.com/video/1234567890
 *
 * Environment Variables:
 * - SAVE_TO_FILE: Set to 'true' to save JSON/Markdown files (default: false, database only)
 * - BROWSER_USER_DATA_DIR: Chrome profile directory (default: OS temp/douyin-scraper-user-data)
 * - PGHOST, PGPORT, PGDATABASE, PGUSER, PGPASSWORD: PostgreSQL connection settings
 * - MIN_COMMENTS_THRESHOLD: Minimum comments threshold (default: 5000)
 */

const { chromium } = require('patchright');
const path = require('path');
const os = require('os');
const fs = require('fs');
const { Pool } = require('pg');

// Configuration
const CONFIG = {
  DOUYIN_URL: 'https://www.douyin.com/?recommend=1',
  USER_DATA_DIR: process.env.BROWSER_USER_DATA_DIR || path.join(os.tmpdir(), 'douyin-scraper-user-data'),
  OUTPUT_DIR: path.join(process.cwd(), 'output'),
  SAVE_TO_FILE: process.env.SAVE_TO_FILE === 'true', // Default: false (database only)
  WAIT_TIMEOUT: 5000,
  MAX_COMMENTS: 10,
  // Keep legacy default behavior for feed scraping.
  MIN_COMMENTS_THRESHOLD: Math.max(0, parseInt(process.env.MIN_COMMENTS_THRESHOLD || '5000', 10) || 5000),
  // Human-like behavior settings
  HUMAN_DELAY: {
    MIN_WAIT: 800,      // Minimum wait time in ms
    MAX_WAIT: 2500,     // Maximum wait time in ms
    CLICK_MIN: 50,      // Min delay before click
    CLICK_MAX: 150,     // Max delay before click
    SCROLL_MIN: 1500,   // Min wait after scroll
    SCROLL_MAX: 3500,   // Max wait after scroll
    TYPE_MIN: 30,       // Min delay between keystrokes
    TYPE_MAX: 120,      // Max delay between keystrokes
  },
  // PostgreSQL configuration
  POSTGRES: {
    host: process.env.PGHOST || 'localhost',
    port: parseInt(process.env.PGPORT || '5432', 10),
    database: process.env.PGDATABASE || 'douyin',
    user: process.env.PGUSER || 'postgres',
    password: process.env.PGPASSWORD || 'postgres',
  },
};

// PostgreSQL connection pool
let pgPool = null;

/**
 * Initialize PostgreSQL connection and create tables if needed
 */
async function initPostgres() {
  try {
    pgPool = new Pool(CONFIG.POSTGRES);

    // Test connection
    const client = await pgPool.connect();
    console.log('🐘 Connected to PostgreSQL');

    // Create tables if not exist
    await client.query(`
      CREATE TABLE IF NOT EXISTS douyin_videos (
        id SERIAL PRIMARY KEY,
        video_id VARCHAR(64) UNIQUE,
        title TEXT,
        author VARCHAR(255),
        likes INTEGER DEFAULT 0,
        likes_display VARCHAR(32),
        comments_count INTEGER DEFAULT 0,
        comments_display VARCHAR(32),
        favorites INTEGER DEFAULT 0,
        favorites_display VARCHAR(32),
        shares INTEGER DEFAULT 0,
        shares_display VARCHAR(32),
        share_link TEXT,
        short_link TEXT,
        raw_data JSONB,
        scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      );

      CREATE TABLE IF NOT EXISTS douyin_comments (
        id SERIAL PRIMARY KEY,
        video_id VARCHAR(64) REFERENCES douyin_videos(video_id) ON DELETE CASCADE,
        username VARCHAR(255),
        content TEXT,
        time VARCHAR(64),
        location VARCHAR(64),
        likes INTEGER DEFAULT 0,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      );

      CREATE INDEX IF NOT EXISTS idx_videos_video_id ON douyin_videos(video_id);
      CREATE INDEX IF NOT EXISTS idx_videos_author ON douyin_videos(author);
      CREATE INDEX IF NOT EXISTS idx_videos_scraped_at ON douyin_videos(scraped_at);
      CREATE INDEX IF NOT EXISTS idx_comments_video_id ON douyin_comments(video_id);
    `);

    client.release();
    console.log('🐘 PostgreSQL tables ready');
    return true;
  } catch (error) {
    console.log(`⚠️  PostgreSQL not available: ${error.message}`);
    console.log('   Videos will only be saved to files.');
    pgPool = null;
    return false;
  }
}

/**
 * Save video data to PostgreSQL
 */
async function saveToPostgres(video) {
  if (!pgPool) return false;

  const client = await pgPool.connect();
  try {
    await client.query('BEGIN');

    // Upsert video
    const videoResult = await client.query(`
      INSERT INTO douyin_videos (
        video_id, title, author,
        likes, likes_display,
        comments_count, comments_display,
        favorites, favorites_display,
        shares, shares_display,
        share_link, short_link,
        raw_data, scraped_at
      ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
      ON CONFLICT (video_id) DO UPDATE SET
        title = EXCLUDED.title,
        author = EXCLUDED.author,
        likes = EXCLUDED.likes,
        likes_display = EXCLUDED.likes_display,
        comments_count = EXCLUDED.comments_count,
        comments_display = EXCLUDED.comments_display,
        favorites = EXCLUDED.favorites,
        favorites_display = EXCLUDED.favorites_display,
        shares = EXCLUDED.shares,
        shares_display = EXCLUDED.shares_display,
        share_link = EXCLUDED.share_link,
        short_link = EXCLUDED.short_link,
        raw_data = EXCLUDED.raw_data,
        scraped_at = EXCLUDED.scraped_at,
        updated_at = CURRENT_TIMESTAMP
      RETURNING id
    `, [
      video.videoId || `temp-${Date.now()}`,
      video.stats.title,
      video.stats.author,
      video.stats.likes,
      video.stats.likesDisplay,
      video.stats.comments,
      video.stats.commentsDisplay,
      video.stats.favorites,
      video.stats.favoritesDisplay,
      video.stats.shares,
      video.stats.sharesDisplay,
      video.shareLink,
      video.shortLink,
      JSON.stringify(video),
      video.timestamp,
    ]);

    // Delete old comments for this video (to avoid duplicates on re-scrape)
    if (video.videoId) {
      await client.query('DELETE FROM douyin_comments WHERE video_id = $1', [video.videoId]);
    }

    // Insert comments
    for (const comment of video.comments) {
      await client.query(`
        INSERT INTO douyin_comments (video_id, username, content, time, location, likes)
        VALUES ($1, $2, $3, $4, $5, $6)
      `, [
        video.videoId || `temp-${Date.now()}`,
        comment.username,
        comment.content,
        comment.time,
        comment.location,
        comment.likes,
      ]);
    }

    await client.query('COMMIT');
    return true;
  } catch (error) {
    await client.query('ROLLBACK');
    console.log(`  ⚠️  PostgreSQL save failed: ${error.message}`);
    return false;
  } finally {
    client.release();
  }
}

/**
 * Close PostgreSQL connection
 */
async function closePostgres() {
  if (pgPool) {
    await pgPool.end();
    console.log('🐘 PostgreSQL connection closed');
  }
}

// Selectors based on DOM detection
const SELECTORS = {
  // Video info
  videoTitle: '[data-e2e="video-desc"]',
  authorName: '[data-e2e="feed-video-nickname"]',

  // Stats
  likeCount: '[data-e2e="video-player-digg"]',
  commentCount: '[data-e2e="feed-comment-icon"]',
  favoriteCount: '[data-e2e="video-player-collect"]',
  shareCount: '[data-e2e="video-player-share"]',

  // Interactive elements
  commentButton: '[data-e2e="feed-comment-icon"]',
  shareButton: '[data-e2e="video-player-share"]',

  // Comment panel
  commentList: '[data-e2e="comment-list"]',
  commentItem: '[data-e2e="comment-item"]',

  // Share panel
  shareContainer: '[data-e2e="video-share-container"]',
  copyLinkButton: 'button:has-text("复制链接")',

  // Navigation
  nextVideo: '[data-e2e="video-switch-next-arrow"]',
  activeVideo: '[data-e2e="feed-active-video"]',
};

// ============================================
// Human-like behavior utilities
// ============================================

/**
 * Generate a random number between min and max (inclusive)
 */
function randomInt(min, max) {
  return Math.floor(Math.random() * (max - min + 1)) + min;
}

/**
 * Generate a random delay within a range
 */
function randomDelay(min, max) {
  return randomInt(min, max);
}

/**
 * Sleep for a random duration (human-like wait)
 */
async function humanWait(minMs = CONFIG.HUMAN_DELAY.MIN_WAIT, maxMs = CONFIG.HUMAN_DELAY.MAX_WAIT) {
  const delay = randomDelay(minMs, maxMs);
  await new Promise(resolve => setTimeout(resolve, delay));
  return delay;
}

/**
 * Human-like click with random offset and delay
 */
async function humanClick(locator, options = {}) {
  const {
    minDelay = CONFIG.HUMAN_DELAY.CLICK_MIN,
    maxDelay = CONFIG.HUMAN_DELAY.CLICK_MAX,
    timeout = 5000,
    force = false,
  } = options;

  // Wait a bit before clicking (like a human would)
  await humanWait(minDelay, maxDelay);

  // Click with random delay between mousedown and mouseup
  await locator.click({
    delay: randomInt(50, 150),
    timeout,
    force, // Force click if element is not stable (e.g., video playing)
  });

  // Small wait after click
  await humanWait(100, 300);
}

/**
 * Human-like mouse move to element before clicking (with fallback)
 */
async function humanHoverAndClick(page, locator, options = {}) {
  const { timeout = 5000 } = options;

  try {
    // Try to hover first (but with short timeout)
    await locator.hover({ timeout: 1500, force: true });
    await humanWait(150, 400);
  } catch (e) {
    // Hover failed, just proceed to click
  }

  // Then click with force to bypass stability checks
  await humanClick(locator, { ...options, timeout, force: true });
}

/**
 * Human-like scroll using mouse wheel
 */
async function humanScroll(page, direction = 'down', options = {}) {
  const {
    minWait = CONFIG.HUMAN_DELAY.SCROLL_MIN,
    maxWait = CONFIG.HUMAN_DELAY.SCROLL_MAX,
    smooth = true,
  } = options;

  // Random scroll amount (like different mouse wheel speeds)
  const scrollAmount = randomInt(300, 600);
  const deltaY = direction === 'down' ? scrollAmount : -scrollAmount;

  if (smooth) {
    // Simulate smooth scrolling with multiple small steps
    const steps = randomInt(3, 6);
    const stepAmount = deltaY / steps;

    for (let i = 0; i < steps; i++) {
      await page.mouse.wheel(0, stepAmount);
      await humanWait(30, 80);
    }
  } else {
    await page.mouse.wheel(0, deltaY);
  }

  // Wait after scrolling
  const waitTime = await humanWait(minWait, maxWait);
  return waitTime;
}

/**
 * Human-like keyboard press with random timing
 */
async function humanKeyPress(page, key, options = {}) {
  const {
    minDelay = 50,
    maxDelay = 150,
  } = options;

  await humanWait(minDelay, maxDelay);
  await page.keyboard.press(key);
  await humanWait(100, 250);
}

/**
 * Simulate reading/viewing content (random pause)
 */
async function simulateReading(minMs = 1000, maxMs = 3000) {
  const readTime = await humanWait(minMs, maxMs);
  return readTime;
}

/**
 * Parse Chinese number notation (e.g., "485.2万" -> 4852000)
 */
function parseChineseNumber(text) {
  if (!text) return 0;

  const cleanText = text.trim();
  if (!cleanText) return 0;

  // Handle "万" (10,000)
  if (cleanText.includes('万')) {
    const num = parseFloat(cleanText.replace('万', ''));
    if (isNaN(num)) return 0;
    return Math.round(num * 10000);
  }

  // Handle "亿" (100,000,000)
  if (cleanText.includes('亿')) {
    const num = parseFloat(cleanText.replace('亿', ''));
    if (isNaN(num)) return 0;
    return Math.round(num * 100000000);
  }

  // Regular number
  return parseInt(cleanText.replace(/[^\d]/g, ''), 10) || 0;
}

/**
 * Format number for display
 */
function formatNumber(num) {
  if (num >= 100000000) {
    return (num / 100000000).toFixed(1) + '亿';
  }
  if (num >= 10000) {
    return (num / 10000).toFixed(1) + '万';
  }
  return num.toString();
}

function sanitizeAuthorName(raw) {
  const text = (raw || '').trim();
  if (!text) return '';
  const blocked = new Set([
    '我的', '推荐', '关注', '朋友', '直播', '放映厅', '短剧', '搜索', '客户端', '通知', '私信', '投稿',
  ]);
  if (blocked.has(text)) return '';
  return text.replace(/^@+/, '').trim();
}

/**
 * Capture the currently active video so later actions stay bound to one card.
 */
async function getActiveVideoSnapshot(activeVideo) {
  return await activeVideo.evaluate((root, selectors) => {
    const getText = (selector) => {
      const el = root.querySelector(selector);
      return el ? el.textContent?.trim() || '' : '';
    };

    const className = root.className || '';
    const classVideoIdMatch = className.match(/video_(\d+)/);

    const videoEl = root.querySelector('video');
    const sourceUrl =
      videoEl?.currentSrc ||
      videoEl?.src ||
      root.querySelector('source')?.src ||
      '';
    const srcVideoIdMatch = sourceUrl.match(/video\/(\d+)/);

    const linkVideoIdMatch = Array.from(root.querySelectorAll('a[href]'))
      .map((link) => link.getAttribute('href') || '')
      .map((href) => href.match(/video\/(\d+)/))
      .find(Boolean);

    const videoId = classVideoIdMatch?.[1] || srcVideoIdMatch?.[1] || linkVideoIdMatch?.[1] || null;

    return {
      title: getText(selectors.videoTitle),
      author: getText(selectors.authorName),
      likes: getText(selectors.likeCount),
      comments: getText(selectors.commentCount),
      favorites: getText(selectors.favoriteCount),
      shares: getText(selectors.shareCount),
      videoId,
    };
  }, SELECTORS);
}

/**
 * Pause the currently active video so Douyin does not advance while panels are open.
 */
async function pauseActiveVideo(activeVideo) {
  return await activeVideo.evaluate((root) => {
    const videoEl = root.querySelector('video');
    if (!videoEl) {
      return { found: false, paused: false };
    }

    try {
      videoEl.pause();
    } catch (e) {
      return { found: true, paused: videoEl.paused, error: e.message };
    }

    return { found: true, paused: videoEl.paused };
  });
}

function buildVideoSignature(snapshot) {
  return [snapshot.videoId || '', snapshot.author || '', snapshot.title || ''].join('||');
}

/**
 * Ensure Douyin is still focused on the same video we started scraping.
 */
async function assertSameActiveVideo(page, expectedSignature, stage) {
  const activeVideo = page.locator(SELECTORS.activeVideo).first();
  const currentSnapshot = await getActiveVideoSnapshot(activeVideo);
  const currentSignature = buildVideoSignature(currentSnapshot);

  if (currentSignature !== expectedSignature) {
    throw new Error(
      `${stage}: active video changed from "${expectedSignature}" to "${currentSignature}"`
    );
  }

  return currentSnapshot;
}

/**
 * Extract comments from the comment panel
 */
async function extractComments(page, maxComments = CONFIG.MAX_COMMENTS) {
  return await page.evaluate((args) => {
    const { selector, max } = args;
    const comments = [];
    const items = document.querySelectorAll(selector);

    items.forEach((item, idx) => {
      if (idx >= max) return;

      const text = item.textContent || '';

      // Parse comment structure: "username...content time·location likes分享回复"
      // Example: "起个破名真费劲...毛主席我们想念你啦！3周前·辽宁18分享回复"

      // Extract username (before ...)
      const usernameMatch = text.match(/^([^.]+)\.\.\./);
      const username = usernameMatch ? usernameMatch[1] : '';

      // Extract likes (number before 分享回复)
      const likesMatch = text.match(/(\d+)分享回复/);
      const likes = likesMatch ? parseInt(likesMatch[1], 10) : 0;

      // Extract time and location
      const timeLocationMatch = text.match(/(\d+[周天月年小时分钟]+前)·?([^\d]*)/);
      const time = timeLocationMatch ? timeLocationMatch[1] : '';
      const location = timeLocationMatch ? timeLocationMatch[2].replace(/\d+分享回复.*$/, '').trim() : '';

      // Extract content (between ... and time)
      let content = text;
      if (usernameMatch) {
        content = text.substring(usernameMatch[0].length);
      }
      if (timeLocationMatch) {
        content = content.substring(0, content.indexOf(timeLocationMatch[0]));
      }

      comments.push({
        username,
        content: content.trim(),
        time,
        location,
        likes,
      });
    });

    return comments;
  }, { selector: SELECTORS.commentItem, max: maxComments });
}

/**
 * Resolve short URL to get the true video link
 */
async function resolveShortLink(page, shortUrl) {
  if (!shortUrl) return null;

  try {
    // If it's already a full douyin.com/video URL, extract video ID
    const videoIdMatch = shortUrl.match(/douyin\.com\/video\/(\d+)/);
    if (videoIdMatch) {
      return {
        shortLink: shortUrl,
        videoId: videoIdMatch[1],
        fullLink: `https://www.douyin.com/video/${videoIdMatch[1]}`,
      };
    }

    // If it's a v.douyin.com short link, we need to follow the redirect
    if (shortUrl.includes('v.douyin.com')) {
      // Open the short link in a new tab to get the redirect
      const newPage = await page.context().newPage();

      try {
        // Navigate and wait for redirect
        await newPage.goto(shortUrl, {
          waitUntil: 'domcontentloaded',
          timeout: 15000,
        });

        // Wait a bit for any JS redirects
        await humanWait(1000, 2000);

        // Get the final URL
        const finalUrl = newPage.url();

        // Extract video ID from final URL
        const finalVideoIdMatch = finalUrl.match(/video\/(\d+)/);
        const videoId = finalVideoIdMatch ? finalVideoIdMatch[1] : null;

        await newPage.close();

        return {
          shortLink: shortUrl,
          videoId,
          fullLink: videoId ? `https://www.douyin.com/video/${videoId}` : finalUrl,
        };
      } catch (err) {
        await newPage.close().catch(() => {});
        console.log(`    ⚠️ Could not resolve short link: ${err.message}`);
        return {
          shortLink: shortUrl,
          videoId: null,
          fullLink: shortUrl,
        };
      }
    }

    return {
      shortLink: shortUrl,
      videoId: null,
      fullLink: shortUrl,
    };
  } catch (error) {
    console.log(`    ⚠️ Error resolving link: ${error.message}`);
    return {
      shortLink: shortUrl,
      videoId: null,
      fullLink: shortUrl,
    };
  }
}

/**
 * Get share link by clicking copy button
 */
async function getShareLink(page) {
  try {
    // Grant clipboard permissions
    const context = page.context();
    await context.grantPermissions(['clipboard-read', 'clipboard-write']);

    // Clear clipboard first
    await page.evaluate(async () => {
      try {
        await navigator.clipboard.writeText('');
      } catch (e) {}
    });

    // Click copy link button with human-like behavior
    const copyButton = page.locator(SELECTORS.copyLinkButton).first();
    if (await copyButton.count() > 0) {
      await humanClick(copyButton, { force: true });
      await humanWait(500, 1000);

      // Try to read from clipboard
      const clipboardText = await page.evaluate(async () => {
        try {
          return await navigator.clipboard.readText();
        } catch (e) {
          return null;
        }
      });

      if (clipboardText && clipboardText.includes('douyin.com')) {
        // Extract just the URL from the clipboard text
        const urlMatch = clipboardText.match(/https:\/\/v\.douyin\.com\/[^\s]+/);
        if (urlMatch) {
          return urlMatch[0];
        }
        // Try alternate URL format
        const altUrlMatch = clipboardText.match(/https:\/\/www\.douyin\.com\/video\/\d+/);
        if (altUrlMatch) {
          return altUrlMatch[0];
        }
        return clipboardText;
      }
    }

    // Fallback: try to get video ID from URL or page
    const videoId = await page.evaluate(() => {
      // Try to find video ID in the active video class
      const activeVideo = document.querySelector('[data-e2e="feed-active-video"]');
      if (activeVideo) {
        const classList = activeVideo.className;
        const match = classList.match(/video_(\d+)/);
        if (match) {
          return match[1];
        }
      }
      return null;
    });

    if (videoId) {
      return `https://www.douyin.com/video/${videoId}`;
    }

    return null;
  } catch (error) {
    console.error('Error getting share link:', error.message);
    return null;
  }
}

/**
 * Scrape a single video
 */
async function scrapeVideo(page, videoIndex, options = {}) {
  const { targetUrl = '' } = options;
  const targetUrlText = String(targetUrl || '');
  const targetVideoId = (targetUrlText.match(/video\/(\d+)/) || [null, null])[1];
  console.log(`\n📹 Scraping video #${videoIndex + 1}...`);

  const video = {
    index: videoIndex + 1,
    timestamp: new Date().toISOString(),
    stats: {},
    comments: [],
    shareLink: null,
  };

  // Wait for video to load (human-like random wait)
  await simulateReading(1500, 3000);
  let activeVideo = page.locator(SELECTORS.activeVideo).first();
  let usePageScope = false;
  try {
    await page.waitForSelector(SELECTORS.activeVideo, { timeout: 15000 });
  } catch (_err) {
    if (!targetUrl) {
      // In legacy feed mode, keep old strict behavior to avoid silent behavior changes.
      throw _err;
    }
    // Target URL pages may not have feed-active-video.
    usePageScope = true;
    activeVideo = page.locator('body').first();
    // On detail pages the <video> element may exist but not be "visible" to Playwright.
    // Continue as long as core metadata UI is present.
    await page.waitForSelector(SELECTORS.videoTitle, { timeout: 15000, state: 'attached' }).catch(async () => {
      await page.waitForSelector(SELECTORS.commentButton, { timeout: 15000, state: 'attached' });
    });
    console.log('  ℹ️ feed-active-video not found, fallback to page-level selectors');
  }

  console.log('  ⏸️  Pausing current video...');
  const pauseResult = await pauseActiveVideo(activeVideo);
  if (pauseResult.found) {
    console.log(`  ${pauseResult.paused ? '✅' : '⚠️'} Video ${pauseResult.paused ? 'paused' : 'is still playing'}`);
  } else {
    console.log('  ⚠️  Video element not found; continuing');
  }

  // Extract basic stats
  console.log('  📊 Extracting video stats...');
  const stats = await getActiveVideoSnapshot(activeVideo);
  video.stats = {
    title: stats.title,
    author: sanitizeAuthorName(stats.author),
    likes: parseChineseNumber(stats.likes),
    likesDisplay: stats.likes,
    comments: parseChineseNumber(stats.comments),
    commentsDisplay: stats.comments,
    favorites: parseChineseNumber(stats.favorites),
    favoritesDisplay: stats.favorites,
    shares: parseChineseNumber(stats.shares),
    sharesDisplay: stats.shares,
  };

  // Extra metadata from detail/feed page text and video element.
  const extraMeta = await page.evaluate(() => {
    const bodyText = document.body?.innerText || '';
    const publishMatch = bodyText.match(/发布时间[：:]\s*([0-9]{4}-[0-9]{2}-[0-9]{2}\s*[0-9]{2}:[0-9]{2})/);
    const videoEl = document.querySelector('video');
    let duration = 0;
    if (videoEl && Number.isFinite(videoEl.duration) && videoEl.duration > 0) {
      duration = Math.round(videoEl.duration);
    }
    return {
      publishTime: publishMatch ? publishMatch[1] : '',
      durationSeconds: duration,
    };
  });
  video.stats.publishTime = extraMeta.publishTime || '';
  video.stats.durationSeconds = extraMeta.durationSeconds || 0;

  // Detail-page fallback: sometimes selector-based title/author are empty.
  if (!video.stats.title || !video.stats.author) {
    const detailMeta = await page.evaluate(() => {
      const titleFromDom =
        document.querySelector('[data-e2e="video-desc"]')?.textContent?.trim() ||
        document.querySelector('h1')?.textContent?.trim() ||
        '';
      const titleFromDoc = (document.title || '').replace(/\s*-\s*抖音\s*$/, '').trim();
      const author =
        document.querySelector('[data-e2e="feed-video-nickname"]')?.textContent?.trim() ||
        document.querySelector('[data-e2e="video-author-name"]')?.textContent?.trim() ||
        document.querySelector('a[href*="/user/"] span')?.textContent?.trim() ||
        '';
      return { title: titleFromDom || titleFromDoc || '', author };
    });
    if (!video.stats.title && detailMeta.title) {
      video.stats.title = detailMeta.title;
    }
    if (!video.stats.author && detailMeta.author) {
      video.stats.author = detailMeta.author;
    }
  }
  video.stats.author = sanitizeAuthorName(video.stats.author);

  console.log(`  📝 Title: ${video.stats.title?.substring(0, 50)}...`);
  console.log(`  👤 Author: ${video.stats.author}`);
  console.log(`  ❤️  Likes: ${video.stats.likesDisplay}`);
  console.log(`  💬 Comments: ${video.stats.commentsDisplay}`);
  if (video.stats.publishTime) {
    console.log(`  🕒 Publish time: ${video.stats.publishTime}`);
  }
  if (video.stats.durationSeconds > 0) {
    console.log(`  ⏱️  Duration: ${video.stats.durationSeconds}s`);
  }

  const expectedVideoSignature = buildVideoSignature(stats);
  video.videoId = stats.videoId || null;
  if (targetVideoId) {
    if (video.videoId && video.videoId !== targetVideoId) {
      console.log(`  ⚠️  Detected video_id ${video.videoId}, overriding with target ${targetVideoId}`);
    }
    video.videoId = targetVideoId;
  }
  if (video.videoId) {
    console.log(`  🆔 Video ID: ${video.videoId}`);
  }

  // Optional threshold gate (disabled by default).
  if (!targetUrlText && CONFIG.MIN_COMMENTS_THRESHOLD > 0 && video.stats.comments < CONFIG.MIN_COMMENTS_THRESHOLD) {
    console.log(`  ⏭️  Skipping: comments (${video.stats.comments}) < ${CONFIG.MIN_COMMENTS_THRESHOLD}`);
    return null;
  }

  // Stay on current page: get comments and share link only, then return (main loop will swipe to next after).
  await simulateReading(800, 1500);

  // Comments (stay on current video, no swipe)
  console.log('  💬 Opening comments...');
  try {
    if (!usePageScope) {
      await assertSameActiveVideo(page, expectedVideoSignature, 'before opening comments');
    }
    const commentButton = activeVideo.locator(SELECTORS.commentButton).first();
    if (await commentButton.count() > 0) {
      await humanHoverAndClick(page, commentButton, { timeout: 5000 });
      await humanWait(1500, 2500);
      if (!usePageScope) {
        await assertSameActiveVideo(page, expectedVideoSignature, 'after opening comments');
      }

      await page.waitForSelector(SELECTORS.commentList, { timeout: 5000 }).catch(() => {});
      await simulateReading(600, 1200);
      if (!usePageScope) {
        await assertSameActiveVideo(page, expectedVideoSignature, 'before extracting comments');
      }

      video.comments = await extractComments(page);
      console.log(`  📝 Found ${video.comments.length} comments`);

      await humanKeyPress(page, 'Escape');
      await humanWait(300, 600);
    }
  } catch (err) {
    console.log(`  ⚠️  Could not open comments: ${err.message}`);
    if (usePageScope) {
      // Detail page often renders comments directly without opening panel.
      try {
        const directComments = await extractComments(page);
        if (directComments.length > 0) {
          video.comments = directComments;
          console.log(`  📝 Fallback comments from detail page: ${video.comments.length}`);
        }
      } catch (_e) {
        // ignore
      }
    }
  }

  await humanWait(400, 900);

  // Share link (still on current video, no swipe)
  console.log('  🔗 Getting share link...');
  try {
    if (!usePageScope) {
      await assertSameActiveVideo(page, expectedVideoSignature, 'before opening share panel');
    }
    const shareButton = activeVideo.locator(SELECTORS.shareButton).first();
    if (await shareButton.count() > 0) {
      await humanHoverAndClick(page, shareButton, { timeout: 5000 });
      await humanWait(1200, 2000);
      if (!usePageScope) {
        await assertSameActiveVideo(page, expectedVideoSignature, 'after opening share panel');
      }

      const rawShareLink = await getShareLink(page);

      if (rawShareLink) {
        console.log(`  🔗 Raw link: ${rawShareLink}`);
        const resolvedLink = await resolveShortLink(page, rawShareLink);
        video.shareLink = resolvedLink.fullLink;
        video.shortLink = resolvedLink.shortLink;
        video.videoId = resolvedLink.videoId;
        console.log(`  🔗 True link: ${video.shareLink}`);
        if (video.videoId) {
          console.log(`  🆔 Video ID: ${video.videoId}`);
        }
      } else {
        console.log(`  🔗 Share link: Not found`);
        if (video.videoId) {
          video.shareLink = `https://www.douyin.com/video/${video.videoId}`;
          video.shortLink = video.shareLink;
          console.log(`  🔗 Fallback link: ${video.shareLink}`);
        }
      }

      await humanKeyPress(page, 'Escape');
      await humanWait(300, 600);
    }
  } catch (err) {
    console.log(`  ⚠️  Could not get share link: ${err.message}`);
    if (targetUrl) {
      video.shareLink = targetUrl;
      video.shortLink = targetUrl;
      console.log(`  🔗 Fallback link after share failure: ${video.shareLink}`);
    } else if (video.videoId) {
      video.shareLink = `https://www.douyin.com/video/${video.videoId}`;
      video.shortLink = video.shareLink;
      console.log(`  🔗 Fallback link after share failure: ${video.shareLink}`);
    }
  }

  return video;
}

/**
 * Generate markdown content for a video
 */
function generateMarkdown(video) {
  const lines = [];

  lines.push(`# ${video.stats.title || 'Untitled Video'}`);
  lines.push('');
  lines.push(`**Scraped at:** ${video.timestamp}`);
  lines.push('');

  // Video info
  lines.push('## Video Information');
  lines.push('');
  lines.push(`- **Title:** ${video.stats.title || 'N/A'}`);
  lines.push(`- **Author:** ${video.stats.author || 'N/A'}`);
  lines.push(`- **Video ID:** ${video.videoId || 'N/A'}`);
  lines.push(`- **Full Link:** ${video.shareLink || 'N/A'}`);
  if (video.shortLink && video.shortLink !== video.shareLink) {
    lines.push(`- **Short Link:** ${video.shortLink}`);
  }
  lines.push('');

  // Stats
  lines.push('## Statistics');
  lines.push('');
  lines.push('| Metric | Count |');
  lines.push('|--------|-------|');
  lines.push(`| ❤️ Likes | ${video.stats.likesDisplay} (${video.stats.likes.toLocaleString()}) |`);
  lines.push(`| 💬 Comments | ${video.stats.commentsDisplay} (${video.stats.comments.toLocaleString()}) |`);
  lines.push(`| ⭐ Favorites | ${video.stats.favoritesDisplay} (${video.stats.favorites.toLocaleString()}) |`);
  lines.push(`| 🔗 Shares | ${video.stats.sharesDisplay} (${video.stats.shares.toLocaleString()}) |`);
  lines.push('');

  // Comments
  if (video.comments.length > 0) {
    lines.push('## Top Comments');
    lines.push('');
    lines.push('| User | Comment | Time | Location | Likes |');
    lines.push('|------|---------|------|----------|-------|');

    video.comments.forEach(comment => {
      const escapedContent = comment.content.replace(/\|/g, '\\|').substring(0, 50);
      lines.push(`| ${comment.username} | ${escapedContent} | ${comment.time} | ${comment.location} | ${comment.likes} |`);
    });
    lines.push('');
  }

  return lines.join('\n');
}

/**
 * Save a single video to separate files and PostgreSQL
 */
async function saveVideoFiles(video, outputDir) {
  let jsonPath = null;
  let mdPath = null;

  // Save to files only if SAVE_TO_FILE is enabled
  if (CONFIG.SAVE_TO_FILE) {
    // Generate filename from video ID or timestamp
    const fileId = video.videoId || `video-${video.index}-${Date.now()}`;
    const safeFileId = fileId.replace(/[^a-zA-Z0-9-_]/g, '_');

    // Save JSON
    jsonPath = path.join(outputDir, `${safeFileId}.json`);
    fs.writeFileSync(jsonPath, JSON.stringify(video, null, 2));

    // Save Markdown
    mdPath = path.join(outputDir, `${safeFileId}.md`);
    fs.writeFileSync(mdPath, generateMarkdown(video));
  }

  // Save to PostgreSQL
  const pgSaved = await saveToPostgres(video);

  return { jsonPath, mdPath, pgSaved };
}

/**
 * Main scraper function
 */
async function main() {
  const args = process.argv.slice(2);
  let targetUrl = null;
  let videoCount = 1;

  for (let i = 0; i < args.length; i++) {
    const arg = args[i];
    if (arg === '--url' && i + 1 < args.length) {
      targetUrl = args[i + 1];
      i += 1;
      continue;
    }
    if (/^https?:\/\//.test(arg) && !targetUrl) {
      targetUrl = arg;
      continue;
    }
    const maybeCount = parseInt(arg, 10);
    if (!Number.isNaN(maybeCount) && maybeCount > 0) {
      videoCount = maybeCount;
    }
  }

  console.log('🎬 Douyin Video Scraper');
  if (targetUrl) {
    console.log(`🎯 Target URL mode: ${targetUrl}`);
  } else {
    if (CONFIG.MIN_COMMENTS_THRESHOLD > 0) {
      console.log(`📊 Will scrape ${videoCount} video(s) with >= ${CONFIG.MIN_COMMENTS_THRESHOLD} comments`);
    } else {
      console.log(`📊 Will scrape ${videoCount} video(s) (no comments threshold)`);
    }
  }
  console.log('');

  // Ensure output directory exists (only if saving to files)
  if (CONFIG.SAVE_TO_FILE && !fs.existsSync(CONFIG.OUTPUT_DIR)) {
    fs.mkdirSync(CONFIG.OUTPUT_DIR, { recursive: true });
  }

  // Initialize PostgreSQL
  const pgEnabled = await initPostgres();

  let context;
  try {
    // Launch system Chrome via Playwright (no CDP; uses patchright + channel: "chrome")
    console.log('🌐 Launching system Chrome (patchright)...');
    console.log(`   📂 UserDataDir: ${CONFIG.USER_DATA_DIR}`);

    if (!fs.existsSync(CONFIG.USER_DATA_DIR)) {
      fs.mkdirSync(CONFIG.USER_DATA_DIR, { recursive: true });
    }

    const launchOptions = {
      channel: 'chrome',
      headless: false,
      viewport: null,
    };

    context = await chromium.launchPersistentContext(CONFIG.USER_DATA_DIR, launchOptions);

    // Grant clipboard permissions for share/copy link
    try {
      await context.grantPermissions(['clipboard-read', 'clipboard-write']);
    } catch (permError) {
      console.log('   ⚠️ Clipboard permission skipped:', permError.message);
    }

    // Find or create Douyin tab
    const pages = context.pages();
    let page;
    
    // Check if there's already a Douyin page open
    const douyinPage = pages.find(p => p.url().includes('douyin.com'));
    if (douyinPage) {
      page = douyinPage;
      console.log('📱 Found existing Douyin tab');
    } else {
      // Open Douyin if not already open
      console.log('📱 Opening Douyin...');
      page = pages.length > 0 ? pages[0] : await context.newPage();
      await page.goto('https://www.douyin.com', { waitUntil: 'domcontentloaded', timeout: 60000 });
      await humanWait(2000, 3000);
    }

    if (targetUrl) {
      console.log('🔗 Opening target video URL...');
      await page.goto(targetUrl, { waitUntil: 'domcontentloaded', timeout: 60000 });
      await humanWait(2000, 3500);
      console.log(`✅ Navigated to: ${page.url()}`);
    } else {
      // Check current URL
      const currentUrl = page.url();
      console.log(`📍 Current URL: ${currentUrl}`);

      // Navigate to recommendation feed by clicking the button if not already there
      if (!currentUrl.includes('recommend')) {
        console.log('🔍 Looking for 推荐 button...');
        try {
          // Wait for the navigation menu to load
          await page.waitForSelector('a:has-text("推荐")', { timeout: 10000 });
          
          // Find and click the 推荐 button
          const recommendButton = page.locator('a:has-text("推荐")').first();
          if (await recommendButton.count() > 0) {
            console.log('🖱️  Clicking 推荐 button...');
            await humanHoverAndClick(page, recommendButton, { timeout: 5000 });
            
            // Wait for navigation
            await humanWait(2000, 3000);
            console.log(`✅ Navigated to: ${page.url()}`);
          } else {
            console.log('⚠️  推荐 button not found, trying direct navigation...');
            await page.goto(CONFIG.DOUYIN_URL, { waitUntil: 'domcontentloaded', timeout: 60000 });
            await humanWait(CONFIG.WAIT_TIMEOUT, CONFIG.WAIT_TIMEOUT + 2000);
          }
        } catch (err) {
          console.log(`⚠️  Could not click 推荐 button: ${err.message}`);
          console.log('   Trying direct navigation...');
          await page.goto(CONFIG.DOUYIN_URL, { waitUntil: 'domcontentloaded', timeout: 60000 });
          await humanWait(CONFIG.WAIT_TIMEOUT, CONFIG.WAIT_TIMEOUT + 2000);
        }
      } else {
        console.log('✅ Already on recommendation feed');
        // Small pause before starting
        await humanWait(500, 1500);
      }
    }

    // Scrape videos
    const videos = [];
    let videoIndex = 0;

    while (videos.length < videoCount) {
      const video = await scrapeVideo(page, videoIndex, { targetUrl });

      if (video) {
        // Video meets threshold, save it
        videos.push(video);

        // Save each video to separate files immediately
        const { jsonPath, mdPath, pgSaved } = await saveVideoFiles(video, CONFIG.OUTPUT_DIR);
        if (jsonPath) {
          console.log(`  💾 Saved: ${path.basename(jsonPath)}${pgSaved ? ' + PostgreSQL' : ''}`);
        } else if (pgSaved) {
          console.log(`  💾 Saved to PostgreSQL`);
        } else {
          console.log(`  ⚠️  No storage configured (enable SAVE_TO_FILE or PostgreSQL)`);
        }

        console.log(`  📊 Progress: ${videos.length}/${videoCount} videos collected`);
      }

      // Navigate to next video if we haven't collected enough (feed mode only)
      if (!targetUrl && videos.length < videoCount) {
        console.log('\n⏭️  Moving to next video...');

        // Simulate finishing watching current video
        await simulateReading(500, 1500);

        // Use keyboard navigation with human-like behavior
        await humanKeyPress(page, 'ArrowDown');
        console.log('  ⬇️  Swiped to next video');

        // Wait for scroll animation and new video to load (human-like random wait)
        const scrollWait = await humanWait(
          CONFIG.HUMAN_DELAY.SCROLL_MIN,
          CONFIG.HUMAN_DELAY.SCROLL_MAX
        );
        console.log(`  ⏳ Waited ${scrollWait}ms for video transition`);

        // Wait for new video to become active
        await page.waitForSelector(SELECTORS.activeVideo, { timeout: 5000 }).catch(() => {});

        // Additional random pause to simulate user looking at new video
        await simulateReading(800, 1800);
      }

      videoIndex++;
    }

    console.log(`\n✅ Scraping complete! Saved ${videos.length} videos`);
    if (CONFIG.SAVE_TO_FILE) {
      console.log(`   Files: ${videos.length} .json + ${videos.length} .md in ${CONFIG.OUTPUT_DIR}`);
    }
    if (pgPool) {
      console.log(`   PostgreSQL: ${videos.length} videos saved to database`);
    }

  } catch (error) {
    console.error('❌ Error:', error.message);
    process.exit(1);
  } finally {
    if (context) {
      try {
        await context.close();
        console.log('🌐 Browser context closed');
      } catch (e) {
        console.log('   ⚠️ Close context:', e.message);
      }
    }
    await closePostgres();
  }
}

// Run the scraper
main();
