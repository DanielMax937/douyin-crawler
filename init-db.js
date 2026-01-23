#!/usr/bin/env node

/**
 * Initialize PostgreSQL database for Douyin Scraper
 *
 * This script is idempotent - safe to run multiple times.
 * It only creates database/tables/indexes if they don't exist.
 *
 * Usage:
 *   node init-db.js                    # Initialize database and tables
 *   node init-db.js --drop             # Drop and recreate tables (WARNING: deletes data)
 *
 * Environment variables:
 *   PGHOST     - PostgreSQL host (default: localhost)
 *   PGPORT     - PostgreSQL port (default: 5432)
 *   PGDATABASE - Database name (default: douyin)
 *   PGUSER     - Username (default: postgres)
 *   PGPASSWORD - Password (default: postgres)
 */

const { Pool, Client } = require('pg');

// Configuration
const CONFIG = {
  host: process.env.PGHOST || 'localhost',
  port: parseInt(process.env.PGPORT || '5432', 10),
  database: process.env.PGDATABASE || 'douyin',
  user: process.env.PGUSER || 'postgres',
  password: process.env.PGPASSWORD || 'postgres',
};

// SQL statements - all idempotent (IF NOT EXISTS / OR REPLACE)
const SQL = {
  dropTables: `
    DROP TABLE IF EXISTS douyin_comments CASCADE;
    DROP TABLE IF EXISTS douyin_videos CASCADE;
    DROP TABLE IF EXISTS video_task_steps CASCADE;
    DROP TABLE IF EXISTS video_tasks CASCADE;
  `,

  createVideosTable: `
    CREATE TABLE IF NOT EXISTS douyin_videos (
      id SERIAL PRIMARY KEY,
      video_id VARCHAR(64) UNIQUE NOT NULL,
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
  `,

  createCommentsTable: `
    CREATE TABLE IF NOT EXISTS douyin_comments (
      id SERIAL PRIMARY KEY,
      video_id VARCHAR(64) NOT NULL REFERENCES douyin_videos(video_id) ON DELETE CASCADE,
      username VARCHAR(255),
      content TEXT,
      time VARCHAR(64),
      location VARCHAR(64),
      likes INTEGER DEFAULT 0,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
  `,

  createVideoTasksTable: `
    CREATE TABLE IF NOT EXISTS video_tasks (
      id SERIAL PRIMARY KEY,
      video_id VARCHAR(64) NOT NULL REFERENCES douyin_videos(video_id) ON DELETE CASCADE,
      current_step VARCHAR(32) NOT NULL DEFAULT 'pending',
      status VARCHAR(32) NOT NULL DEFAULT 'pending',
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      UNIQUE(video_id)
    );
    COMMENT ON COLUMN video_tasks.current_step IS 'Current step: pending, download, submit, get_summary, completed';
    COMMENT ON COLUMN video_tasks.status IS 'Status: pending, processing, completed, failed';
  `,

  createVideoTaskStepsTable: `
    CREATE TABLE IF NOT EXISTS video_task_steps (
      id SERIAL PRIMARY KEY,
      video_id VARCHAR(64) NOT NULL REFERENCES douyin_videos(video_id) ON DELETE CASCADE,
      step_name VARCHAR(32) NOT NULL,
      status VARCHAR(32) NOT NULL DEFAULT 'pending',
      result JSONB,
      error_message TEXT,
      started_at TIMESTAMP,
      completed_at TIMESTAMP,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    COMMENT ON COLUMN video_task_steps.step_name IS 'Step: download, submit, get_summary';
    COMMENT ON COLUMN video_task_steps.status IS 'Status: pending, processing, completed, failed';
    COMMENT ON COLUMN video_task_steps.result IS 'JSON result from this step';
  `,

  createIndexes: `
    CREATE INDEX IF NOT EXISTS idx_videos_video_id ON douyin_videos(video_id);
    CREATE INDEX IF NOT EXISTS idx_videos_author ON douyin_videos(author);
    CREATE INDEX IF NOT EXISTS idx_videos_scraped_at ON douyin_videos(scraped_at);
    CREATE INDEX IF NOT EXISTS idx_videos_likes ON douyin_videos(likes DESC);
    CREATE INDEX IF NOT EXISTS idx_comments_video_id ON douyin_comments(video_id);
    CREATE INDEX IF NOT EXISTS idx_comments_likes ON douyin_comments(likes DESC);
    CREATE INDEX IF NOT EXISTS idx_video_tasks_video_id ON video_tasks(video_id);
    CREATE INDEX IF NOT EXISTS idx_video_tasks_status ON video_tasks(status);
    CREATE INDEX IF NOT EXISTS idx_video_tasks_current_step ON video_tasks(current_step);
    CREATE INDEX IF NOT EXISTS idx_video_task_steps_video_id ON video_task_steps(video_id);
    CREATE INDEX IF NOT EXISTS idx_video_task_steps_step_name ON video_task_steps(step_name);
    CREATE INDEX IF NOT EXISTS idx_video_task_steps_status ON video_task_steps(status);
  `,

  createTriggerFunction: `
    CREATE OR REPLACE FUNCTION update_updated_at_column()
    RETURNS TRIGGER AS $$
    BEGIN
      NEW.updated_at = CURRENT_TIMESTAMP;
      RETURN NEW;
    END;
    $$ language 'plpgsql';
  `,

  createTrigger: `
    DO $$
    BEGIN
      IF NOT EXISTS (
        SELECT 1 FROM pg_trigger WHERE tgname = 'update_douyin_videos_updated_at'
      ) THEN
        CREATE TRIGGER update_douyin_videos_updated_at
          BEFORE UPDATE ON douyin_videos
          FOR EACH ROW
          EXECUTE FUNCTION update_updated_at_column();
      END IF;
      IF NOT EXISTS (
        SELECT 1 FROM pg_trigger WHERE tgname = 'update_video_tasks_updated_at'
      ) THEN
        CREATE TRIGGER update_video_tasks_updated_at
          BEFORE UPDATE ON video_tasks
          FOR EACH ROW
          EXECUTE FUNCTION update_updated_at_column();
      END IF;
    END;
    $$;
  `,
};

/**
 * Create database if not exists
 */
async function ensureDatabase() {
  const client = new Client({
    ...CONFIG,
    database: 'postgres',
  });

  try {
    await client.connect();

    // Check if database exists
    const result = await client.query(
      `SELECT 1 FROM pg_database WHERE datname = $1`,
      [CONFIG.database]
    );

    if (result.rows.length === 0) {
      await client.query(`CREATE DATABASE ${CONFIG.database}`);
      console.log(`✅ Database "${CONFIG.database}" created`);
      return true;
    } else {
      console.log(`✅ Database "${CONFIG.database}" exists`);
      return false;
    }
  } finally {
    await client.end();
  }
}

/**
 * Initialize tables (idempotent)
 */
async function initTables(dropFirst = false) {
  const pool = new Pool(CONFIG);

  try {
    const client = await pool.connect();

    if (dropFirst) {
      console.log('🗑️  Dropping existing tables...');
      await client.query(SQL.dropTables);
      console.log('✅ Tables dropped');
    }

    // Create tables (IF NOT EXISTS)
    console.log('📦 Ensuring tables exist...');
    await client.query(SQL.createVideosTable);
    await client.query(SQL.createCommentsTable);
    await client.query(SQL.createVideoTasksTable);
    await client.query(SQL.createVideoTaskStepsTable);
    console.log('✅ Tables ready');

    // Create indexes (IF NOT EXISTS)
    console.log('📇 Ensuring indexes exist...');
    await client.query(SQL.createIndexes);
    console.log('✅ Indexes ready');

    // Create trigger function (OR REPLACE)
    console.log('⚡ Ensuring triggers exist...');
    await client.query(SQL.createTriggerFunction);
    await client.query(SQL.createTrigger);
    console.log('✅ Triggers ready');

    // Show table info
    const tablesResult = await client.query(`
      SELECT table_name
      FROM information_schema.tables
      WHERE table_schema = 'public' AND table_name LIKE 'douyin_%'
      ORDER BY table_name
    `);

    console.log('\n📊 Database tables:');
    for (const row of tablesResult.rows) {
      const countResult = await client.query(`SELECT COUNT(*) FROM ${row.table_name}`);
      console.log(`   ${row.table_name}: ${countResult.rows[0].count} rows`);
    }

    client.release();
  } finally {
    await pool.end();
  }
}

/**
 * Main function
 */
async function main() {
  const args = process.argv.slice(2);
  const shouldDrop = args.includes('--drop');

  console.log('🐘 Douyin Scraper Database Initialization');
  console.log('');
  console.log('Configuration:');
  console.log(`   Host:     ${CONFIG.host}`);
  console.log(`   Port:     ${CONFIG.port}`);
  console.log(`   Database: ${CONFIG.database}`);
  console.log(`   User:     ${CONFIG.user}`);
  console.log('');

  if (shouldDrop) {
    console.log('⚠️  WARNING: --drop flag detected, will delete all data!\n');
  }

  try {
    // Ensure database exists
    await ensureDatabase();

    // Initialize tables
    await initTables(shouldDrop);

    console.log('\n✅ Database initialization complete!');
    console.log('\nRun the scraper:');
    console.log('   node douyin-scraper.js 5');

  } catch (error) {
    console.error(`\n❌ Error: ${error.message}`);

    if (error.message.includes('ECONNREFUSED')) {
      console.log('\n💡 Tip: Make sure PostgreSQL is running:');
      console.log('   brew services start postgresql');
      console.log('   # or');
      console.log('   pg_ctl -D /usr/local/var/postgres start');
    }

    if (error.message.includes('password authentication failed')) {
      console.log('\n💡 Tip: Set correct credentials:');
      console.log('   export PGUSER=your_username');
      console.log('   export PGPASSWORD=your_password');
    }

    process.exit(1);
  }
}

main();
