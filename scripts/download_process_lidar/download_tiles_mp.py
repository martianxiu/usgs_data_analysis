import os
from os.path import join, exists
from glob import glob
import sys
import json
from collections import OrderedDict
import laspy
import numpy as np
import matplotlib.pyplot as plt
import pdal
import geopandas as gpd
from shapely.geometry import MultiPolygon, Polygon
from joblib import Parallel, delayed
import time
import multiprocessing
from multiprocessing import Pool, get_context
import argparse
import gc
import psutil
from datetime import datetime

RED = "\033[91m"
GREEN = "\033[92m"
YELLOW = "\033[93m"
BLUE = "\033[94m"
MAGENTA = "\033[95m"
CYAN = "\033[96m"
WHITE = "\033[97m"
RESET = "\033[0m"

def get_date():
    # Get the current date and time
    now = datetime.now()
    # Format the date
    current_date_time = now.strftime("%Y-%m-%d %H:%M:%S")    # print("Current date:", current_date)
    return current_date_time

def memory_usage():
    process = psutil.Process(os.getpid())
    mem_info = process.memory_info()
    return mem_info.rss / (1024 * 1024)  # Convert to MB

def redirect_output(log_file):
    sys.stdout = open(log_file, 'a')
    sys.stderr = open(log_file, 'a')

def multipolygon_to_wkt_list(multipolygon):
    if not multipolygon.is_empty:
        # return [f'"{polygon.wkt}"' for polygon in multipolygon.geoms]
        return [polygon.wkt for polygon in multipolygon.geoms]
    return []
def save_txt(num_downloaded, required, path):
    if not os.path.exists(path):
        os.makedirs(path)
    filename = "num_sample-downloaded.txt"
    file_path = os.path.join(path, filename)
    
    with open(file_path, 'w') as file:
        file.write(f"{required},")
        file.write(f"{num_downloaded}")

def read_num_sample(file_path):
    with open(file_path, 'r') as file:
        content = file.read().strip()
        num_sample, num_downloaded = content.split(',')
        return int(num_sample)

# Function to generate and execute a pipeline
# @timeout.timeout(duration=5)
def generate_and_execute_pipeline(args_list):
    index, sample_data, json_template, save_root, file_prefix, num_sample = args_list
    try:
        print(f"Starting pipeline for index {index}")
        url = sample_data.url
        name = url.split('/')[-2]
        epsg_code = sample_data.local_epsg_code
        polygons_str = multipolygon_to_wkt_list(sample_data.geometry)
        
        dst_root = save_root
        dst_folder = name
        dst_path = join(dst_root, dst_folder)
        if not exists(dst_path):
            os.makedirs(dst_path)

        log_path = join(dst_path, 'log')
        log_file = join(log_path, 'num_sample-downloaded.txt')
        if exists(log_file):
            prev_num_sample = read_num_sample(log_file) # return the previous num_sample
        else:
            prev_num_sample = 0

        if prev_num_sample == 0:
            print(f'{BLUE}[{get_date()}] {name}: Download from the beginning{RESET}')
        # elif num_sample > prev_num_sample and prev_num_sample < len(polygons_str):
        elif num_sample > prev_num_sample:
            print(f"{BLUE}[{get_date()}] {name}: Continue from file {prev_num_sample} to file {num_sample}{RESET}")
        else:
            print(f"[{get_date()}] {name}: the samples you want {num_sample} are less than or equal to you now have tried to download {prev_num_sample}. Possibly some polygons return zero points. (in total you can have {len(polygons_str)}). So, exit.")
            return 0
        # temporary folder to download laz for renaming purpose. 
        dst_path_temp = join(dst_path, 'temp_download') 
        if not exists(dst_path_temp):
            os.makedirs(dst_path_temp)
        dst_name_prefix = file_prefix
        
        pipeline_json = json_template
        pipeline_json['pipeline'][0]['polygon'] = polygons_str[prev_num_sample:num_sample] # reader 
        pipeline_json['pipeline'][0]['filename'] = url # reader 
        pipeline_json['pipeline'][1]['polygon'] = polygons_str[prev_num_sample:num_sample] # crop to split the cloud
        pipeline_json['pipeline'][2]['out_srs'] = f'EPSG:{epsg_code}'
        pipeline_json['pipeline'][4]['filename'] = join(dst_path_temp, dst_name_prefix) + '_#.laz'
        
        # Convert the JSON dictionary to an OrderedDict to maintain key order
        pipeline_json_str = json.dumps(pipeline_json, indent=4)
        
        # Execute the PDAL pipeline using the PDAL Python bindings
        pipeline = pdal.Pipeline(pipeline_json_str)
        # Execute the pipeline
        count = pipeline.execute()
        
        # rename files and move them to the parent folder 
        print(f'[{get_date()}] now moving files .. ')
        filenames_temp = os.listdir(dst_path_temp)
        if len(filenames_temp) == 0:
            num_downloaded = len(glob(join(dst_path, '*.laz')))
            print(f'[{get_date()}] {name} there is nothing downloaded. Downloaded files/#polygons: {num_downloaded}/{len(polygons_str)}')
            save_txt(num_downloaded=num_downloaded, required=num_sample, path=log_path)
            return 0
        filenames_temp.sort()
        new_filenames = []
        for name in filenames_temp:
            cur_id = int(name.split('.')[0].split('_')[-1])
            new_id = prev_num_sample + cur_id
            new_name = f"{dst_name_prefix}_{new_id}.laz"
            print(f"[{get_date()}] {join(dst_path_temp, name)} --> {join(dst_path, new_name)}")
            os.rename(f"{join(dst_path_temp, name)}", f"{join(dst_path, new_name)}")

        # check num files alreayd downloaded
        num_downloaded = len(glob(join(dst_path, '*.laz')))
        save_txt(num_downloaded=num_downloaded, required=num_sample, path=log_path)
        
        # ensure the process releases the memory
        del pipeline
        gc.collect()  # Force garbage collection
        print(f"[{get_date()}] Worker {index} finished. Memory usage: {memory_usage()} MB")
        print(f"[{get_date()}] Pipeline executed successfully for index {index} with {count} points processed.")
        return count
    except Exception as e:
        print(f"[{get_date()}] Pipeline execution failed for index {index}: {e}")
        return f"[{get_date()}] Pipeline failed for index {index} with error: {e}"

def run_with_timeout(args_list, max_workers, timeout):
    results = []
    with get_context("spawn").Pool(max_workers, maxtasksperchild=1) as pool:
        async_results = []

        # Submit tasks asynchronously
        for arg in args_list:
            async_result = pool.apply_async(generate_and_execute_pipeline, args=(arg,))
            async_results.append((arg, async_result))

        # Collect results with timeout handling
        for arg, async_result in async_results:
            try:
                result = async_result.get(timeout=timeout)
                results.append(result)
            except multiprocessing.TimeoutError:
                print(f"{RED} {get_date()} Task with argument {arg} exceeded the time limit of {timeout} seconds.{RESET}")
                # print(f"{get_date()} Task with argument {arg} exceeded the time limit of {timeout} seconds.")
                results.append(None)

    return results

if __name__ == "__main__":
    json_template = {
        "pipeline": [
            {"polygon": [], "filename": "", "type": "readers.ept", "tag": "readdata"},
            {"type": "filters.crop", "polygon": []},
            {"in_srs": "EPSG:3857","out_srs": "", "tag": "reprojectUTM", "type": "filters.reprojection"},
            {"limits": "Classification![7:7]", "type": "filters.range", "tag": "nonoise"},
            {"filename": "", "tag": "writerslas", "type": "writers.las"}
        ]
    }
    # Create the parser
    parser = argparse.ArgumentParser(description='download tiles from 3DEP')
    
    # Add arguments
    parser.add_argument('--num_sample', type=int, default=100, help='number of tiles to get per cloud')
    parser.add_argument('--max_workers', type=int, default=20, help='number of CPUs')
    parser.add_argument('--tile_list_path', type=str, default='', help='')
    parser.add_argument('--log_path', type=str, default='', help='')
    parser.add_argument('--save_root', type=str, default='', help='')
    parser.add_argument('--timeout', type=int, default='', help='')
    
    # Parse the arguments
    args = parser.parse_args()
    
    tile_list_path = args.tile_list_path
    save_root = args.save_root
    log_file = args.log_path

    print("reading tile list files")
    gdf = gpd.read_file(tile_list_path)
    print(gdf.info())

    
    file_prefix = 'tile'
    num_sample = args.num_sample 
    max_workers = args.max_workers 
    num_cloud = len(gdf)

    # Start time for execution
    start_time = time.time()
    
    # Prepare arguments for multiprocessing
    args_list = [(i, gdf.iloc[i], json_template.copy(), save_root, file_prefix, num_sample) for i in range(num_cloud)]
    
    
    # args_list = args_list[:10] # debug

    remaining_args_list = []
    for i, arg in enumerate(args_list):
        index, sample_data, json_template, save_root, file_prefix, num_sample = arg
        url = sample_data.url
        name = url.split('/')[-2]
        num_available_polygons = len(sample_data.geometry.geoms)
        dst_root = save_root
        dst_folder = name
        dst_path = join(dst_root, dst_folder)
        log_path = join(dst_path, 'log')
        log_file = join(log_path, 'num_sample-downloaded.txt')
        if exists(log_file):
            prev_num_sample = read_num_sample(log_file) # return the previous num_sample
            if prev_num_sample < num_sample and prev_num_sample < num_available_polygons:
                remaining_args_list.append(arg)
            else:
                continue    
        else:
            remaining_args_list.append(arg)
    
    print(f"remaining/total: {len(remaining_args_list)}/{len(args_list)}")

    print("start downloading tiles")
    timeout = args.timeout  # Timeout in seconds for each task
    results = run_with_timeout(remaining_args_list, max_workers, timeout)

    # End time for execution
    end_time = time.time()

    # Print the process and results
    print(f"\n[{get_date()}] Pipeline execution completed. num_sample/max_workers: {num_sample}/{max_workers}")
    print(f"[{get_date()}] Execution time: {end_time - start_time:.2f} seconds")
