#!/bin/bash

in_dir=../../../data/processed_tiles_developed_forest/
out_dir=../../../data/processed_tiles_developed_forest_invalid_filtered/

python invalid_filter.py --in_dir $in_dir --out_dir $out_dir


