#!/usr/bin/env node

/**
 * Migration: Add local_file_path column to douyin_videos table.
 * Idempotent - safe to run multiple times.
 *
 * Usage: node scripts/add-local-file-path-column.js
 */

const { Pool } = require('pg');

const CONFIG = {
  host: process.env.PGHOST || 'localhost',
  port: parseInt(process.env.PGPORT || '5432', 10),
  database: process.env.PGDATABASE || 'douyin',
  user: process.env.PGUSER || 'postgres',
  password: process.env.PGPASSWORD || 'postgres',
  options: process.env.PGOPTIONS || '-c timezone=Asia/Shanghai',
};

async function main() {
  const pool = new Pool(CONFIG);
  try {
    await pool.query(`
      ALTER TABLE douyin_videos
      ADD COLUMN IF NOT EXISTS local_file_path TEXT;
    `);
    console.log('✅ Column local_file_path added (or already exists)');
    await pool.query(`
      COMMENT ON COLUMN douyin_videos.local_file_path IS
      'Local path after download, e.g. /data/douyin/videos/douyin_xxx.mp4';
    `);
    console.log('✅ Comment on local_file_path added');
    await pool.query(`
      CREATE INDEX IF NOT EXISTS idx_videos_local_file_path
      ON douyin_videos(local_file_path) WHERE local_file_path IS NOT NULL;
    `);
    console.log('✅ Index on local_file_path created');
  } catch (err) {
    console.error('❌ Migration failed:', err.message);
    process.exit(1);
  } finally {
    await pool.end();
  }
}

main();
