#!/bin/bash

echo "🧪 Running 3 test attempts to verify scraper accuracy"
echo "=================================================="
echo ""

PASS_COUNT=0
FAIL_COUNT=0

for i in 1 2 3; do
  echo "📊 Test $i/3"
  echo "----------"
  
  # Run scraper
  node douyin-scraper.js 1 > test_$i.log 2>&1
  
  # Extract scraped title from log
  SCRAPED_TITLE=$(grep "📌 Initial title:" test_$i.log | sed 's/.*📌 Initial title: //' | sed 's/\.\.\.//')
  
  # Extract video ID from log
  VIDEO_ID=$(grep "🆔 Video ID:" test_$i.log | tail -1 | sed 's/.*🆔 Video ID: //')
  
  # Extract video link from log
  VIDEO_URL="https://www.douyin.com/video/$VIDEO_ID"
  
  echo "   Scraped Title: $SCRAPED_TITLE"
  echo "   Video ID: $VIDEO_ID"
  echo "   Video URL: $VIDEO_URL"
  
  # Query database for this video
  DB_TITLE=$(psql -h localhost -U postgres -d douyin -t -c "SELECT title FROM douyin_videos WHERE video_id = '$VIDEO_ID';" 2>/dev/null | xargs)
  
  echo "   DB Title: $DB_TITLE"
  
  # Open URL and get title
  echo "   🌐 Verifying URL..."
  agent-browser --headed open "$VIDEO_URL" > /dev/null 2>&1
  sleep 3
  ACTUAL_TITLE=$(agent-browser get title 2>/dev/null | sed 's/ - 抖音//')
  agent-browser close > /dev/null 2>&1
  
  echo "   Actual Title: $ACTUAL_TITLE"
  
  # Compare titles (check if scraped title is substring of actual title or vice versa)
  if [[ "$ACTUAL_TITLE" == *"$SCRAPED_TITLE"* ]] || [[ "$SCRAPED_TITLE" == *"$ACTUAL_TITLE"* ]]; then
    echo "   ✅ PASS: Titles match!"
    PASS_COUNT=$((PASS_COUNT + 1))
  else
    echo "   ❌ FAIL: Titles do NOT match!"
    FAIL_COUNT=$((FAIL_COUNT + 1))
  fi
  
  echo ""
  sleep 2
done

echo "=================================================="
echo "📊 Final Results:"
echo "   ✅ Passed: $PASS_COUNT/3"
echo "   ❌ Failed: $FAIL_COUNT/3"
echo ""

if [ $PASS_COUNT -eq 3 ]; then
  echo "🎉 SUCCESS: All 3 tests passed!"
  exit 0
else
  echo "⚠️  FAILURE: Not all tests passed"
  exit 1
fi
