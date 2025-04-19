#!/bin/bash

# Directories
DEV_DIR="dev"
PROD_DIR="production"

# Output files to move after running the pipeline
FILES=("clean_data3.db" "final_output3.csv" "script_log.log")

# Save the current directory to return later
CURRENT_DIR=$(pwd)

# Change to the dev directory where the Python script is located
cd "$DEV_DIR"

echo "Running pipeline_script.py..."
python3 pipeline_script.py

# Return to the original directory
cd "$CURRENT_DIR"

echo ""
echo "Moving output files to production..."
for file in "${FILES[@]}"; do
    SRC_FILE="$DEV_DIR/$file"
    if [ -f "$SRC_FILE" ]; then
        mv "$SRC_FILE" "$PROD_DIR/"
        echo "Moved $file to production."
        rm "$DEV_DIR/$file"  
        echo "Removed $file from dev."
    else
        echo "Warning: $file not found in $DEV_DIR."
    fi
done

echo ""
echo "Done!"



