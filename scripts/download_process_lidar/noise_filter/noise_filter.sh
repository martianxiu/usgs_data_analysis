#!/bin/bash

in_dir=../../../data/raw_tiles_developed_forest/
out_dir=../../../data/processed_tiles_developed_forest/

python noise_filter.py --in_dir $in_dir --out_dir $out_dir
echo 'finished'

