import os
import sys
import shutil
import time
import argparse
import psutil
import numpy as np
import laspy
import lazrs
import multiprocessing
from datetime import datetime
from os.path import join, exists
from multiprocessing import get_context
from collections import defaultdict

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


def run_with_timeout(args_list, max_workers, timeout, function):
    """Runs multiprocessing tasks with a timeout for each task."""
    results = []
    with get_context("spawn").Pool(max_workers, maxtasksperchild=1) as pool:
        async_results = [(arg, pool.apply_async(function, args=(arg,))) for arg in args_list]

        for arg, async_result in async_results:
            try:
                results.append(async_result.get(timeout=timeout))
            except multiprocessing.TimeoutError:
                print(f"{RED}[{get_date()}] Task {arg} exceeded the timeout of {timeout} seconds.{RESET}")
                results.append(["timeout error"])
            except (ValueError, laspy.errors.LaspyException, lazrs.LazrsError) as e:
                print(f"{RED}[{get_date()}] Error occurred with {arg}: {str(e)}{RESET}")
                results.append([str(e)])

    return results


def laz_files_by_subfolder(root_dir):
    """Returns a dictionary mapping subfolder names to lists of .laz files."""
    laz_files_dict = defaultdict(list)
    for dirpath, _, filenames in os.walk(root_dir):
        subfolder_name = os.path.basename(dirpath)
        for filename in filenames:
            if filename.endswith(".laz"):
                laz_files_dict[subfolder_name].append(join(dirpath, filename))
    return dict(laz_files_dict)


def load_laz_and_get_range(filepath):
    """Loads a .laz file and returns the X, Y, Z points along with their range and lengths."""
    with laspy.open(filepath) as fh:
        infile = fh.read()

    points = np.vstack((infile.x, infile.y, infile.z)).T
    x_range, y_range = (points[:, 0].min(), points[:, 0].max()), (points[:, 1].min(), points[:, 1].max())
    return points, x_range, y_range, x_range[1] - x_range[0], y_range[1] - y_range[0]


def check_invalid(arg_list):
    """Identifies invalid tiles based on heuristic length constraints."""
    global_idx, region_name, samples, sample_num = arg_list
    print(f"[{get_date()}] {global_idx} Processing {region_name}")
    
    invalid_tiles = [
        sample for i, sample in enumerate(samples)
        if load_laz_and_get_range(sample)[3] > 1000 or load_laz_and_get_range(sample)[4] > 1000
        if (i + 1) <= sample_num
    ]
    return invalid_tiles


def return_bigger_tile_mask(x_array):
    """Returns a mask for the larger half of the point cloud split by X center."""
    x_center = (x_array.min() + x_array.max()) / 2
    left_mask, right_mask = x_array < x_center, x_array >= x_center
    return left_mask if left_mask.sum() >= right_mask.sum() else right_mask


def check_invalid_and_correct(args_list):
    """Processes tiles to filter invalid ones or copies valid ones to output directory."""
    global_idx, tile_path, out_root = args_list
    region_name, tile_name = tile_path.split("/")[-2], tile_path.split("/")[-1]
    new_file_path = join(out_root, region_name, tile_name)

    print(f"[{get_date()}] {global_idx} Processing {region_name}/{tile_name}")

    with laspy.open(tile_path) as fh:
        infile = fh.read()

    x, y = infile.x, infile.y
    x_range, y_range = (x.min(), x.max()), (y.min(), y.max())
    x_length, y_length = x_range[1] - x_range[0], y_range[1] - y_range[0]

    if x_length > 1000 or y_length > 1000:
        mask = return_bigger_tile_mask(x if x_length >= y_length else y)
        new_file = laspy.create(point_format=infile.header.point_format, file_version=infile.header.version)
        new_file.points = infile.points[mask]
        new_file.write(new_file_path)
        operation = "filtered"
    else:
        shutil.copy(tile_path, new_file_path)
        operation = "copied"

    msg = f"[{get_date()}] Success ({operation}): {tile_path} --> {new_file_path}"
    print(msg)
    return msg


def write_list_to_txt(filename, data_list):
    """Writes a list to a text file, each item on a new line."""
    with open(filename, "w") as file:
        file.writelines(f"{item}\n" for item in data_list)


def flatten_list(input_list):
    """Flattens a nested list into a single list."""
    return [item for sublist in input_list for item in sublist]


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process invalid tiles")
    parser.add_argument("--in_dir", type=str, default="../raw_tiles", help="Input tile directory")
    parser.add_argument("--out_dir", type=str, default="../processed_tiles", help="Output tile directory")
    parser.add_argument("--max_workers", type=int, default=20, help="Number of CPU workers")

    args = parser.parse_args()
    in_root, out_root = args.in_dir.rstrip("/"), args.out_dir.rstrip("/")
    max_workers = os.cpu_count() if args.max_workers == -1 else args.max_workers

    print(f"Using {max_workers} workers")

    start_time = time.time()

    laz_files_dict = laz_files_by_subfolder(in_root)
    r_names = list(laz_files_dict.keys())

    # Pre-create destination directories
    for r_name in r_names:
        os.makedirs(join(out_root, r_name), exist_ok=True)

    # Filter out already processed files
    laz_files_dict_values = flatten_list(laz_files_dict.values())
    laz_files_dict_values = [value for value in laz_files_dict_values if "backup" not in value]

    laz_files_dict_exist = laz_files_by_subfolder(out_root)
    laz_files_dict_exist_values = flatten_list(laz_files_dict_exist.values())

    remaining = set("/".join(l.split("/")[-2:]) for l in laz_files_dict_values) - set(
        "/".join(l.split("/")[-2:]) for l in laz_files_dict_exist_values
    )
    remaining = [join(in_root, l) for l in remaining]

    print(f"Total: {len(laz_files_dict_values)}, Remaining: {len(remaining)}")

    args_list = [(f"{idx+1}/{len(remaining)}", fp, out_root) for idx, fp in enumerate(remaining)]

    print(f"[{get_date()}] Start processing.")
    timeout = 100 * 60
    results = run_with_timeout(args_list, max_workers, timeout, function=check_invalid_and_correct)

    end_time = time.time()

    print(f"[{get_date()}] Execution completed in {end_time - start_time:.2f} seconds")