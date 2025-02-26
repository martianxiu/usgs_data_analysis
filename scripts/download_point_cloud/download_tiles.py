import os
from os.path import join, exists
from glob import glob
import sys
import json
import gc
import psutil
import time
import argparse
import geopandas as gpd
import pdal
from datetime import datetime

RED = "\033[91m"
BLUE = "\033[94m"
RESET = "\033[0m"

def get_date():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def memory_usage():
    process = psutil.Process(os.getpid())
    return process.memory_info().rss / (1024 * 1024)  # MB

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

def multipolygon_to_wkt_list(multipolygon):
    return [polygon.wkt for polygon in multipolygon.geoms] if not multipolygon.is_empty else []

def generate_and_execute_pipeline(index, sample_data, json_template, save_root, file_prefix, num_sample):
    try:
        print(f"Starting pipeline for index {index}")
        url = sample_data.url
        name = url.split('/')[-2]
        epsg_code = sample_data.local_epsg_code
        polygons_str = multipolygon_to_wkt_list(sample_data.geometry)
        
        dst_path = join(save_root, name)
        if not exists(dst_path):
            os.makedirs(dst_path)
        
        log_path = join(dst_path, 'log')
        log_file = join(log_path, 'num_sample-downloaded.txt')
        prev_num_sample = read_num_sample(log_file) if exists(log_file) else 0
        
        if num_sample <= prev_num_sample:
            print(f"[{get_date()}] {name}: Already processed {prev_num_sample}/{num_sample}. Skipping.")
            return 0
        
        dst_path_temp = join(dst_path, 'temp_download')
        if not exists(dst_path_temp):
            os.makedirs(dst_path_temp)
        
        pipeline_json = json_template.copy()
        pipeline_json['pipeline'][0]['polygon'] = polygons_str[prev_num_sample:num_sample]
        pipeline_json['pipeline'][0]['filename'] = url
        pipeline_json['pipeline'][1]['polygon'] = polygons_str[prev_num_sample:num_sample]
        pipeline_json['pipeline'][2]['out_srs'] = f"EPSG:{epsg_code}"
        pipeline_json['pipeline'][4]['filename'] = join(dst_path_temp, file_prefix) + '_#.laz'
        
        pipeline = pdal.Pipeline(json.dumps(pipeline_json, indent=4))
        count = pipeline.execute()
        
        print(f'[{get_date()}] Moving files...')
        filenames_temp = os.listdir(dst_path_temp)
        if not filenames_temp:
            num_downloaded = len(glob(join(dst_path, '*.laz')))
            save_txt(num_downloaded, num_sample, log_path)
            return 0
        
        filenames_temp.sort()
        for fname in filenames_temp:
            cur_id = int(fname.split('.')[0].split('_')[-1])
            new_id = prev_num_sample + cur_id
            new_fname = f"{file_prefix}_{new_id}.laz"
            os.rename(join(dst_path_temp, fname), join(dst_path, new_fname))
        
        num_downloaded = len(glob(join(dst_path, '*.laz')))
        save_txt(num_downloaded, num_sample, log_path)
        
        del pipeline
        gc.collect()
        print(f"[{get_date()}] Pipeline executed for index {index}, project {name}. Memory usage: {memory_usage()} MB")
        return count
    except Exception as e:
        print(f"[{get_date()}] Pipeline execution failed for index {index}, project {name}: {e}")
        return 0

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Download tiles from 3DEP')
    parser.add_argument('--num_sample', type=int, default=100, help='Number of tiles per cloud')
    parser.add_argument('--tile_list_path', type=str, required=True, help='Path to tile list file')
    parser.add_argument('--save_root', type=str, required=True, help='Directory to save results')
    args = parser.parse_args()
    
    print("Reading tile list files")
    gdf = gpd.read_file(args.tile_list_path)
    print(gdf.info())
    
    json_template = {
        "pipeline": [
            {"polygon": [], "filename": "", "type": "readers.ept", "tag": "readdata"},
            {"type": "filters.crop", "polygon": []},
            {"in_srs": "EPSG:3857", "out_srs": "", "tag": "reprojectUTM", "type": "filters.reprojection"},
            {"limits": "Classification![7:7]", "type": "filters.range", "tag": "nonoise"},
            {"filename": "", "tag": "writerslas", "type": "writers.las"}
        ]
    }
    
    file_prefix = 'tile'
    num_sample = args.num_sample
    num_cloud = len(gdf)
    
    start_time = time.time()
    
    for i in range(num_cloud):
        generate_and_execute_pipeline(i, gdf.iloc[i], json_template, args.save_root, file_prefix, num_sample)
    
    end_time = time.time()
    
    print(f"\n[{get_date()}] Pipeline execution completed. num_sample: {num_sample}")
    print(f"[{get_date()}] Execution time: {end_time - start_time:.2f} seconds")

