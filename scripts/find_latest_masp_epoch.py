#!/usr/bin/env python3
"""
Utility script to find the latest MASP epoch from existing CSV files.
This is used to determine where to start fetching new data to avoid duplicates.
"""

import os
import csv
import glob
from typing import Optional

def find_latest_masp_epoch(csv_dir: str = "csv") -> Optional[int]:
    """
    Find the highest MASP epoch from all CSV files in the specified directory.
    
    Args:
        csv_dir: Directory containing CSV files (default: "csv")
        
    Returns:
        The highest MASP epoch found, or None if no CSV files exist
    """
    # Check if directory exists
    if not os.path.exists(csv_dir):
        print(f"Directory '{csv_dir}' does not exist")
        return None
    
    # Find all CSV files
    csv_files = glob.glob(os.path.join(csv_dir, "*.csv"))
    
    if not csv_files:
        print(f"No CSV files found in '{csv_dir}'")
        return None
    
    print(f"Found {len(csv_files)} CSV files in '{csv_dir}'")
    
    highest_masp_epoch = None
    
    for csv_file in csv_files:
        try:
            with open(csv_file, 'r', newline='') as file:
                reader = csv.DictReader(file)
                
                for row in reader:
                    try:
                        masp_epoch = int(row['masp_epoch'])
                        if highest_masp_epoch is None or masp_epoch > highest_masp_epoch:
                            highest_masp_epoch = masp_epoch
                    except (ValueError, KeyError) as e:
                        # Skip rows with invalid or missing masp_epoch
                        continue
                        
        except Exception as e:
            print(f"Error reading {csv_file}: {e}")
            continue
    
    return highest_masp_epoch

def main():
    """Main function to find and print the latest MASP epoch."""
    latest_epoch = find_latest_masp_epoch()
    
    if latest_epoch is not None:
        print(f"Latest MASP epoch found: {latest_epoch}")
        print(f"Next MASP epoch to fetch: {latest_epoch + 1}")
        # Print the value that can be used in GitHub Actions (newer syntax)
        print(f"next_masp_epoch={latest_epoch + 1} >> $GITHUB_OUTPUT")
    else:
        print("No MASP epoch data found")
        print("next_masp_epoch=0 >> $GITHUB_OUTPUT")

if __name__ == "__main__":
    main() 