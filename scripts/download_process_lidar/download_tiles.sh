#!/bin/bash

num_sample=5
tile_list_path=../../data/sampled_tiles/sample_1000_developed_forest.gpkg
save_root=../../data/raw_tiles_developed_forest

python download_tiles.py --num_sample $num_sample  --tile_list_path $tile_list_path --save_root $save_root
echo 'finished'
