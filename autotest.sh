#!/bin/bash

videoURLs=(
    "https://storage.googleapis.com/thinking-banner-421414_cloudbuild/LazySong.mp4"
)

watermarkURLs=(
    "https://storage.googleapis.com/thinking-banner-421414_cloudbuild/Logo.png"
)

for ((i = 0; i < ${#videoURLs[@]}; i++)); do
    videoURL="${videoURLs[$i]}"
    watermarkURL="${watermarkURLs[$i]}"

    response=$(curl -X POST http://127.0.0.1:8080/upload2 \
            -F "videoURL=$videoURL" \
            -F "watermarkURL=$watermarkURL")

            echo "URL upload response for video: $videoURL and watermark: $watermarkURL: $response"
            done
