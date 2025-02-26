import os
from os import makedirs
from os.path import join, exists
from glob import glob
import sys
import json
import gc
import psutil
import time
import argparse
import pdal
import numpy as np
from datetime import datetime

RED = "\033[91m"
BLUE = "\033[94m"
RESET = "\033[0m"

def get_date():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def memory_usage():
    process = psutil.Process(os.getpid())
    return process.memory_info().rss / (1024 * 1024)  # MB

def multipolygon_to_wkt_list(multipolygon):
    return [polygon.wkt for polygon in multipolygon.geoms] if not multipolygon.is_empty else []

def save_txt(num_downloaded, required, path):
    if not os.path.exists(path):
        os.makedirs(path)
    file_path = os.path.join(path, "num_sample-downloaded.txt")
    with open(file_path, 'w') as file:
        file.write(f"{required},")
        file.write(f"{num_downloaded}")

def read_num_sample(file_path):
    with open(file_path, 'r') as file:
        num_sample, _ = file.read().strip().split(',')
        return int(num_sample)

def filter_noise(file_path, output_path, index):
    json_template = {
        "pipeline": [
            {"filename": file_path, "type": "readers.las", "tag": "readerlas"},
            {"type": "filters.outlier", "method": "statistical", "mean_k": 12, "multiplier": 2.2},
            {"limits": "Classification![7:7]", "type": "filters.range", "tag": "nonoise"},
            {"filename": output_path, "tag": "writerslas", "type": "writers.las"}
        ]
    }
    
    try:
        print(f"Starting pipeline for index {index}")
        if exists(output_path):
            print(f"Skipped {output_path} because it exists")
            return
        
        output_dir = os.path.dirname(output_path)
        if not exists(output_dir):
            makedirs(output_dir)
        
        pipeline = pdal.Pipeline(json.dumps(json_template, indent=4))
        count = pipeline.execute()
        
        del pipeline
        gc.collect()
        print(f"[{get_date()}] Worker {index} finished. Memory usage: {memory_usage()} MB")
        print(f"[{get_date()}] Pipeline executed successfully for index {index} with {count} points processed.")
    except Exception as e:
        print(f"[{get_date()}] Pipeline execution failed for index {index}: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Process LiDAR tiles')
    parser.add_argument('--in_dir', type=str, default='../raw_tiles', help='Input directory')
    parser.add_argument('--out_dir', type=str, default='../processed_tiles', help='Output directory')
    args = parser.parse_args()
    
    in_root = args.in_dir
    out_root = args.out_dir
    
    start_time = time.time()
    
    file_paths = sorted(glob(join(in_root, "**", "*.laz")))
    output_paths = [x.replace(in_root, out_root) for x in file_paths]
    
    remaining_files = [(f, o, i) for i, (f, o) in enumerate(zip(file_paths, output_paths)) if not exists(o)]
    
    print(f"Remaining files to process: {len(remaining_files)}")
    
    for file_path, output_path, index in remaining_files:
        filter_noise(file_path, output_path, index)
    
    end_time = time.time()
    
    print(f"\n[{get_date()}] Pipeline execution completed")
    print(f"[{get_date()}] Execution time: {end_time - start_time:.2f} seconds")
