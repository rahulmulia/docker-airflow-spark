#!/bin/bash

result_dir="result/spotify"

# Loop through each file in the result directory
for file in "$result_dir"/*; do
    # Use the full file path
    FILE_PATH="$file"

    # Variables
    USERNAME="jack"
    PASSWORD="123456"
    URL="http://localhost:8030/api/quickstart/track_spotify/_stream_load"

    curl --location-trusted -u "$USERNAME:$PASSWORD" \
        -T "$FILE_PATH" \
        -H "expect: 100-continue" \
        -H "column_separator:|" \
        -H "skip_header:1" \
        -H "enclose:\"" \
        -H "max_filter_ratio:1" \
        -H "columns:request_url,code,album_id,album_type,artists_name,total_tracks,available_markets,release_date,disc_number,duration_ms,isrc,spotify_id,name,type,spotify_uri" \
        -XPUT "$URL"


    echo " "
    echo " "
    echo " "
    echo "Success"

    rm -f "$FILE_PATH"

done
