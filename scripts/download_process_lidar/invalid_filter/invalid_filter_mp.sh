#!/bin/bash

in_dir=../../../data/processed_tiles_developed_forest/
out_dir=../../../data/processed_tiles_developed_forest_invalid_filtered/
max_workers=2

python invalid_filter_mp.py --in_dir $in_dir --out_dir $out_dir --max_workers $max_workers


