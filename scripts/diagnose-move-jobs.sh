#!/bin/bash
# Diagnostic script for stuck move-to-object-storage jobs
# Run with: bash scripts/diagnose-move-jobs.sh

# Database connection parameters
DB_HOST="${DB_HOST:-127.0.0.1}"
DB_PORT="${DB_PORT:-9004}"
DB_NAME="${DB_NAME:-peertube_dev}"
DB_USER="${DB_USER:-peertube}"
DB_PASS="${DB_PASS:-peertube}"

VIDEO_UUID="${1:-}"

echo "=== Move-to-Object-Storage Diagnostic ==="
echo ""

# Function to run SQL query
run_sql() {
    PGPASSWORD="$DB_PASS" psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -t -c "$1" 2>/dev/null
}

if [ -n "$VIDEO_UUID" ]; then
    echo "Analyzing specific video: $VIDEO_UUID"
    echo ""
    
    # Get video info and job info
    echo "Video Info:"
    run_sql "SELECT uuid, id, state, \"publishedAt\" FROM video WHERE uuid LIKE '%$VIDEO_UUID%' LIMIT 1;" | head -4
    
    VIDEO_ID=$(run_sql "SELECT id FROM video WHERE uuid LIKE '%$VIDEO_UUID%' LIMIT 1;" | xargs)
    
    if [ -z "$VIDEO_ID" ]; then
        echo "Video not found!"
        exit 1
    fi
    
    echo ""
    echo "Job Info:"
    run_sql "SELECT * FROM \"videoJobInfo\" WHERE \"videoId\" = $VIDEO_ID;" | head -10
    
    echo ""
    echo "Video Files (storage status):"
    run_sql "SELECT vf.filename, vf.storage, vf.resolution, vf.size 
             FROM \"videoFile\" vf 
             WHERE vf.\"videoId\" = $VIDEO_ID
             ORDER BY vf.resolution DESC;"
    
    echo ""
    echo "Streaming Playlists (if any):"
    run_sql "SELECT vsp.id, vsp.storage, vsp.\"playlistFilename\"
             FROM \"videoStreamingPlaylist\" vsp
             WHERE vsp.\"videoId\" = $VIDEO_ID;"
    
    # Get HLS files
    HLS_ID=$(run_sql "SELECT id FROM \"videoStreamingPlaylist\" WHERE \"videoId\" = $VIDEO_ID LIMIT 1;" | xargs)
    if [ -n "$HLS_ID" ]; then
        echo ""
        echo "HLS Video Files:"
        run_sql "SELECT vf.filename, vf.storage, vf.resolution, vf.size
                 FROM \"videoFile\" vf
                 WHERE vf.\"videoStreamingPlaylistId\" = $HLS_ID
                 ORDER BY vf.resolution DESC;"
    fi
    
    echo ""
    echo "Thumbnails:"
    run_sql "SELECT filename, type, storage FROM thumbnail WHERE \"videoId\" = $VIDEO_ID;"
    
    echo ""
    echo "Captions:"
    run_sql "SELECT filename, language, storage FROM \"videoCaption\" WHERE \"videoId\" = $VIDEO_ID;"
    
    echo ""
    echo "Storyboards:"
    run_sql "SELECT filename, storage FROM storyboard WHERE \"videoId\" = $VIDEO_ID;"
    
    echo ""
    echo "Video Source:"
    run_sql "SELECT \"keptOriginalFilename\", storage FROM \"videoSource\" WHERE \"videoId\" = $VIDEO_ID;"
    
    # Summary
    echo ""
    echo "=== Summary ==="
    LOCAL_FILES=$(run_sql "SELECT COUNT(*) FROM \"videoFile\" WHERE (\"videoId\" = $VIDEO_ID OR \"videoStreamingPlaylistId\" IN (SELECT id FROM \"videoStreamingPlaylist\" WHERE \"videoId\" = $VIDEO_ID)) AND storage = 0;" | xargs)
    OS_FILES=$(run_sql "SELECT COUNT(*) FROM \"videoFile\" WHERE (\"videoId\" = $VIDEO_ID OR \"videoStreamingPlaylistId\" IN (SELECT id FROM \"videoStreamingPlaylist\" WHERE \"videoId\" = $VIDEO_ID)) AND storage = 1;" | xargs)
    PENDING_MOVE=$(run_sql "SELECT \"pendingMove\" FROM \"videoJobInfo\" WHERE \"videoId\" = $VIDEO_ID;" | xargs)
    
    echo "Local files: $LOCAL_FILES"
    echo "Object storage files: $OS_FILES"
    echo "pendingMove counter: $PENDING_MOVE"
    
    if [ "$LOCAL_FILES" = "0" ] && [ "$OS_FILES" -gt 0 ] && [ "$PENDING_MOVE" -gt 0 ]; then
        echo ""
        echo "⚠️  INCONSISTENCY: All files on object storage but pendingMove counter is positive!"
        echo "    To fix, run:"
        echo "    UPDATE \"videoJobInfo\" SET \"pendingMove\" = 0 WHERE \"videoId\" = $VIDEO_ID;"
        
        VIDEO_STATE=$(run_sql "SELECT state FROM video WHERE id = $VIDEO_ID;" | xargs)
        if [ "$VIDEO_STATE" = "TO_MOVE_TO_EXTERNAL_STORAGE" ]; then
            echo ""
            echo "    Also, video state is '$VIDEO_STATE', should be 'PUBLISHED'."
            echo "    UPDATE video SET state = 'PUBLISHED' WHERE id = $VIDEO_ID;"
        fi
    fi
    
else
    echo "Analyzing all videos with pendingMove..."
    echo ""
    
    echo "Videos with pendingMove > 0:"
    run_sql "
    SELECT v.uuid, v.state, vji.\"pendingMove\", vji.\"pendingTranscode\",
           (SELECT COUNT(*) FROM \"videoFile\" vf WHERE vf.\"videoId\" = v.id AND vf.storage = 0) as local_files,
           (SELECT COUNT(*) FROM \"videoFile\" vf WHERE vf.\"videoId\" = v.id AND vf.storage = 1) as os_files
    FROM video v
    JOIN \"videoJobInfo\" vji ON v.id = vji.\"videoId\"
    WHERE vji.\"pendingMove\" > 0
    ORDER BY v.\"publishedAt\" DESC
    LIMIT 20;
    "
    
    echo ""
    echo "Videos in storage-related states:"
    run_sql "
    SELECT uuid, state, \"publishedAt\"
    FROM video
    WHERE state IN ('TO_MOVE_TO_EXTERNAL_STORAGE', 'TO_MOVE_TO_EXTERNAL_STORAGE_FAILED', 
                    'TO_MOVE_TO_FILE_SYSTEM', 'TO_MOVE_TO_FILE_SYSTEM_FAILED')
    ORDER BY \"publishedAt\" DESC
    LIMIT 20;
    "
    
    echo ""
    echo "To diagnose a specific video, run:"
    echo "  bash scripts/diagnose-move-jobs.sh <video_uuid>"
fi

echo ""
echo "=== Done ==="
