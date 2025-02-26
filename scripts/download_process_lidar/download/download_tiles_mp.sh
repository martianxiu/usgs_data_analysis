#!/bin/bash

num_sample=0
limit=80
max_workers=2
tile_list_path=../../../data/sampled_tiles/sample_1000_developed_forest.gpkg
log_path=download_log/download.log
save_root=../../../data/raw_tiles_developed_forest
timeout_sec=10000

while [ $num_sample -le $limit ]
do
    python download_tiles_mp.py --num_sample $num_sample --max_workers $max_workers --tile_list_path $tile_list_path --log_path $log_path --save_root $save_root  --timeout $timeout_sec
    echo 'finished'
    num_sample=$((num_sample + 5))
done
