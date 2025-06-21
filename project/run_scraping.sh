#!/bin/bash
source .venv/bin/activate

# Set error handling

set -e
trap 'echo "Error occurred. Check the logs for details." >&2' ERR

# Create necessary directories
mkdir -p logs

# Get current date for folder name
DATE_STR=$(date +%Y%m%d)
OUTPUT_DIR="data_${DATE_STR}"
mkdir -p "${OUTPUT_DIR}"

echo "Starting LinkedIn job scraping process..."
echo "Timestamp: $(date)"
echo "----------------------------------------"

# Run two spiders in parallel
echo "Starting parallel spiders..."
scrapy crawl linkedin_jobs -o "${OUTPUT_DIR}/data_1.csv" > "logs/spider_1.log" 2>&1 &


#Bholu@1997!
