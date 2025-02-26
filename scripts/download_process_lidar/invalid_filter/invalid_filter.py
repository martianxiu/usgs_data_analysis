import os
import sys
from os.path import join, exists
import lazrs
from datetime import datetime
import laspy
import time
import argparse
import psutil
import numpy as np
import shutil
from collections import defaultdict

RED = "\033[91m"
RESET = "\033[0m"

def get_date():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def memory_usage():
    process = psutil.Process(os.getpid())
    return process.memory_info().rss / (1024 * 1024)  # MB

def laz_files_by_subfolder(root_dir):
    laz_files_dict = defaultdict(list)
    for dirpath, _, filenames in os.walk(root_dir):
        subfolder_name = os.path.basename(dirpath)
        for filename in filenames:
            if filename.endswith('.laz'):
                laz_files_dict[subfolder_name].append(os.path.join(dirpath, filename))
    return dict(laz_files_dict)

def load_laz_and_get_range(filepath):
    with laspy.open(filepath) as fh:
        infile = fh.read()
    points = np.vstack((infile.x, infile.y, infile.z)).transpose()
    x_range = (np.min(points[:, 0]), np.max(points[:, 0]))
    y_range = (np.min(points[:, 1]), np.max(points[:, 1]))
    return points, x_range, y_range, x_range[1] - x_range[0], y_range[1] - y_range[0]

def return_bigger_tile_mask(x_array):
    x_center = (np.min(x_array) + np.max(x_array)) / 2
    left_mask = x_array < x_center
    right_mask = x_array >= x_center 
    return left_mask if np.sum(left_mask) >= np.sum(right_mask) else right_mask

def check_invalid_and_correct(tile_path, out_root):
    region_name, tile_name = tile_path.split("/")[-2:]
    new_file_path = join(out_root, region_name, tile_name)
    print(f"[{get_date()}] Processing {region_name}/{tile_name}")
    
    with laspy.open(tile_path) as fh:
        infile = fh.read()
    x, y = infile.x, infile.y
    x_length, y_length = np.ptp(x), np.ptp(y)
    
    if x_length > 1000 or y_length > 1000:
        mask = return_bigger_tile_mask(x if x_length >= y_length else y)
        new_file = laspy.create(point_format=infile.header.point_format, file_version=infile.header.version)
        new_file.points = infile.points[mask]
        new_file.write(new_file_path)
        op_name = "filtered"
    else:
        shutil.copy(tile_path, new_file_path)
        op_name = 'copy'
    print(f"[{get_date()}] Success ({op_name}): {tile_path} --> {new_file_path}")

def flatten_list(input_list):
    return [item for sublist in input_list for item in sublist]

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Process invalid tiles')
    parser.add_argument('--in_dir', type=str, default='../raw_tiles', help='Input tile dir')
    parser.add_argument('--out_dir', type=str, default='../processed_tiles', help='Output tile dir')
    args = parser.parse_args()
    
    in_root = args.in_dir.rstrip('/')
    out_root = args.out_dir.rstrip('/')
    
    start_time = time.time()
    laz_files_dict = laz_files_by_subfolder(in_root)
    r_names = list(laz_files_dict.keys())
    for r_name in r_names:
        os.makedirs(join(out_root, r_name), exist_ok=True)
    
    laz_files_dict_values = ["/".join(l.split("/")[-2:]) for l in flatten_list(laz_files_dict.values()) if 'backup' not in l]
    laz_files_dict_exist_values = ["/".join(l.split("/")[-2:]) for l in flatten_list(laz_files_by_subfolder(out_root).values())]
    remaining = list(set(laz_files_dict_values) - set(laz_files_dict_exist_values))
    print(f"Total: {len(laz_files_dict_values)}, remaining: {len(remaining)}")
    
    for tile in remaining:
        check_invalid_and_correct(join(in_root, tile), out_root)
    
    print(f"[{get_date()}] Execution time: {time.time() - start_time:.2f} seconds")

