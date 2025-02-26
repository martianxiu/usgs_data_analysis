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


def generate_and_execute_pipeline(args_list):
    """Generates and executes a PDAL pipeline for processing point cloud tiles."""
    index, sample_data, json_template, save_root, file_prefix, num_sample = args_list

    try:
        print(f"Starting pipeline for index {index}")
        url = sample_data.url
        name = url.split("/")[-2]
        epsg_code = sample_data.local_epsg_code
        polygons_str = multipolygon_to_wkt_list(sample_data.geometry)

        dst_path = join(save_root, name)
        os.makedirs(dst_path, exist_ok=True)

        log_path = join(dst_path, "log")
        log_file = join(log_path, "num_sample-downloaded.txt")
        prev_num_sample = read_num_sample(log_file) if exists(log_file) else 0

        if prev_num_sample >= num_sample:
            print(f"[{get_date()}] {name}: Already processed required samples.")
            return 0

        dst_path_temp = join(dst_path, "temp_download")
        os.makedirs(dst_path_temp, exist_ok=True)

        # Update pipeline JSON
        pipeline_json = json_template.copy()
        pipeline_json["pipeline"][0]["polygon"] = polygons_str[prev_num_sample:num_sample]
        pipeline_json["pipeline"][0]["filename"] = url
        pipeline_json["pipeline"][1]["polygon"] = polygons_str[prev_num_sample:num_sample]
        pipeline_json["pipeline"][2]["out_srs"] = f"EPSG:{epsg_code}"
        pipeline_json["pipeline"][4]["filename"] = join(dst_path_temp, file_prefix) + "_#.laz"

        # Execute pipeline
        pipeline_json_str = json.dumps(pipeline_json, indent=4)
        pipeline = pdal.Pipeline(pipeline_json_str)
        count = pipeline.execute()

        # Move and rename downloaded files
        filenames_temp = sorted(os.listdir(dst_path_temp))
        if not filenames_temp:
            num_downloaded = len(glob(join(dst_path, "*.laz")))
            print(f"[{get_date()}] {name} No files downloaded. Downloaded: {num_downloaded}/{len(polygons_str)}")
            save_txt(num_downloaded, num_sample, log_path)
            return 0

        for filename in filenames_temp:
            cur_id = int(filename.split(".")[0].split("_")[-1])
            new_name = f"{file_prefix}_{prev_num_sample + cur_id}.laz"
            os.rename(join(dst_path_temp, filename), join(dst_path, new_name))

        # Update log file
        num_downloaded = len(glob(join(dst_path, "*.laz")))
        save_txt(num_downloaded, num_sample, log_path)

        # Cleanup
        del pipeline
        gc.collect()

        print(f"[{get_date()}] Worker {index} finished. Memory usage: {memory_usage()} MB")
        return count

    except Exception as e:
        error_msg = f"[{get_date()}] Pipeline failed for index {index} with error: {e}"
        print(error_msg)
        return error_msg


def run_with_timeout(args_list, max_workers, timeout):
    """Runs multiprocessing with a timeout for each task."""
    results = []
    with get_context("spawn").Pool(max_workers, maxtasksperchild=1) as pool:
        async_results = [(arg, pool.apply_async(generate_and_execute_pipeline, args=(arg,))) for arg in args_list]

        for arg, async_result in async_results:
            try:
                results.append(async_result.get(timeout=timeout))
            except multiprocessing.TimeoutError:
                print(f"{RED}[{get_date()}] Task with argument {arg} exceeded timeout {timeout} seconds.{RESET}")
                results.append(None)

    return results


if __name__ == "__main__":
    json_template = {
        "pipeline": [
            {"polygon": [], "filename": "", "type": "readers.ept", "tag": "readdata"},
            {"type": "filters.crop", "polygon": []},
            {"in_srs": "EPSG:3857", "out_srs": "", "tag": "reprojectUTM", "type": "filters.reprojection"},
            {"limits": "Classification![7:7]", "type": "filters.range", "tag": "nonoise"},
            {"filename": "", "tag": "writerslas", "type": "writers.las"}
        ]
    }

    parser = argparse.ArgumentParser(description="Download and process 3DEP tiles.")
    parser.add_argument("--num_sample", type=int, default=100, help="Number of tiles per cloud.")
    parser.add_argument("--max_workers", type=int, default=20, help="Number of CPU cores.")
    parser.add_argument("--tile_list_path", type=str, required=True, help="Path to the tile list (GeoJSON).")
    parser.add_argument("--log_path", type=str, required=True, help="Path to log file.")
    parser.add_argument("--save_root", type=str, required=True, help="Root directory for saving results.")
    parser.add_argument("--timeout", type=int, required=True, help="Timeout in seconds per task.")

    args = parser.parse_args()

    gdf = gpd.read_file(args.tile_list_path)
    file_prefix = "tile"
    num_sample = args.num_sample
    max_workers = args.max_workers

    args_list = [
        (i, gdf.iloc[i], json_template.copy(), args.save_root, file_prefix, num_sample)
        for i in range(len(gdf))
    ]

    # Filter only remaining tasks
    remaining_args_list = [
        arg for arg in args_list if not exists(join(args.save_root, arg[1].url.split("/")[-2], "log", "num_sample-downloaded.txt"))
    ]

    print(f"Remaining tasks: {len(remaining_args_list)}/{len(args_list)}")
    print("Starting tile downloads...")

    start_time = time.time()
    results = run_with_timeout(remaining_args_list, max_workers, args.timeout)
    end_time = time.time()

    print(f"\n[{get_date()}] Execution completed. Processed: {num_sample}/{max_workers}")
    print(f"[{get_date()}] Total execution time: {end_time - start_time:.2f} seconds")