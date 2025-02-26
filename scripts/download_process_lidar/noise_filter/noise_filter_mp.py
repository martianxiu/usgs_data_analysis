import os
import sys
import json
import gc
import time
import argparse
import psutil
import multiprocessing
from glob import glob
from datetime import datetime
from os.path import join, exists
from multiprocessing import get_context
from collections import OrderedDict

import laspy
import numpy as np
import pdal
import geopandas as gpd
from shapely.geometry import MultiPolygon, Polygon

# ANSI color codes for terminal output
RED = "\033[91m"
GREEN = "\033[92m"
YELLOW = "\033[93m"
BLUE = "\033[94m"
MAGENTA = "\033[95m"
CYAN = "\033[96m"
WHITE = "\033[97m"
RESET = "\033[0m"


def get_date():
    """Returns the current date and time as a formatted string."""
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def memory_usage():
    """Returns the current memory usage in MB."""
    process = psutil.Process(os.getpid())
    return process.memory_info().rss / (1024 * 1024)  # Convert to MB


def multipolygon_to_wkt_list(multipolygon):
    """Converts a MultiPolygon to a list of WKT strings."""
    return [polygon.wkt for polygon in multipolygon.geoms] if not multipolygon.is_empty else []


def save_txt(num_downloaded, required, path):
    """Saves the number of samples downloaded to a text file."""
    os.makedirs(path, exist_ok=True)
    file_path = join(path, "num_sample-downloaded.txt")
    with open(file_path, "w") as file:
        file.write(f"{required},{num_downloaded}")


def read_num_sample(file_path):
    """Reads the number of downloaded samples from a text file."""
    with open(file_path, "r") as file:
        num_sample, num_downloaded = map(int, file.read().strip().split(","))
        return num_sample


def filter_noise(args_list):
    """Runs a PDAL pipeline to filter noise in LAS/LAZ files."""
    json_template = {
        "pipeline": [
            {"filename": "", "type": "readers.las", "tag": "readerlas"},
            {"type": "filters.outlier", "method": "statistical", "mean_k": 12, "multiplier": 2.2},
            {"limits": "Classification![7:7]", "type": "filters.range", "tag": "nonoise"},
            {"filename": "", "tag": "writerslas", "type": "writers.las"}
        ]
    }

    file_path, output_path, index = args_list

    try:
        if exists(output_path):
            print(f"Skipped {output_path} because it exists")
            return f"Skipped {output_path} because it exists"

        os.makedirs(os.path.dirname(output_path), exist_ok=True)

        pipeline_json = json_template.copy()
        pipeline_json["pipeline"][0]["filename"] = file_path  # Input file
        pipeline_json["pipeline"][-1]["filename"] = output_path  # Output file

        pipeline_json_str = json.dumps(pipeline_json, indent=4)

        pipeline = pdal.Pipeline(pipeline_json_str)
        count = pipeline.execute()

        del pipeline
        gc.collect()

        print(f"[{get_date()}] Worker {index} finished. Memory usage: {memory_usage()} MB")
        print(f"[{get_date()}] Pipeline executed successfully for index {index} with {count} points processed.")
        return count
    except Exception as e:
        error_msg = f"[{get_date()}] Pipeline failed for index {index} with error: {e}"
        print(error_msg)
        return error_msg


def run_with_timeout(args_list, max_workers, timeout, function):
    """Runs multiprocessing with a timeout for each task."""
    results = []
    with get_context("spawn").Pool(max_workers, maxtasksperchild=1) as pool:
        async_results = [(arg, pool.apply_async(function, args=(arg,))) for arg in args_list]

        for arg, async_result in async_results:
            try:
                results.append(async_result.get(timeout=timeout))
            except multiprocessing.TimeoutError:
                print(f"{RED}[{get_date()}] Task with argument {arg} exceeded the time limit of {timeout} seconds.{RESET}")
                results.append(None)

    return results


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Filter noise from LAS/LAZ files using PDAL.")
    parser.add_argument("--in_dir", type=str, default="../raw_tiles", help="Directory containing raw LAS/LAZ files.")
    parser.add_argument("--out_dir", type=str, default="../processed_tiles", help="Directory for processed LAS/LAZ files.")
    parser.add_argument("--max_workers", type=int, default=20, help="Number of worker processes.")

    args = parser.parse_args()

    in_root, out_root, max_workers = args.in_dir, args.out_dir, args.max_workers
    max_workers = max_workers if max_workers != -1 else os.cpu_count()

    start_time = time.time()

    file_paths = sorted(glob(join(in_root, "**", "*.laz"), recursive=True))
    output_paths = [f.replace(in_root, out_root) for f in file_paths]

    existing_index = [i for i, o in enumerate(output_paths) if not exists(o)]
    file_paths = [file_paths[i] for i in existing_index]
    output_paths = [output_paths[i] for i in existing_index]
    args_list = [(f, o, i) for i, (f, o) in enumerate(zip(file_paths, output_paths))]

    print(f"Remaining {len(existing_index)} files. CPU count: {max_workers}")

    timeout = 100 * 60  # Timeout in seconds per task
    results = run_with_timeout(args_list, max_workers, timeout, function=filter_noise)

    end_time = time.time()

    print(f"\n[{get_date()}] Pipeline execution completed")
    print(f"[{get_date()}] Execution time: {end_time - start_time:.2f} seconds")