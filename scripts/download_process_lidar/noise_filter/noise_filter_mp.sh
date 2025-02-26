#!/bin/bash

in_dir=../../../data/raw_tiles_developed_forest/
out_dir=../../../data/processed_tiles_developed_forest/
max_workers=2

python noise_filter_mp.py --in_dir $in_dir --out_dir $out_dir --max_workers 2
echo 'finished'

