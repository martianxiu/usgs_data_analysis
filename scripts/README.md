# Data Processing Workflow

## Step 1: Crop NLCD Land Cover with Point Cloud Boundary
Follow the `crop_NLCD_with_pc_boundary.ipynb` notebook located in `scripts/nlcd`. 

**Note:** The NLCD 2021 dataset must be downloaded beforehand. Refer to the instructions in the `data` directory.

## Step 2: Download and Crop DEM with Point Cloud Boundary
Follow the `crop_dem_with_pc_boundary.ipynb` notebook in `scripts/dem`.

## Step 3: Perform Patch/Tile Sampling
Execute the `patch_sampling.ipynb` notebook found in `scripts/sampling`.

## Step 4: Download the Point Cloud Tiles
This step requires the `.gpkg` file generated in Step 3.

```sh
cd scripts/download_process_lidar/download
sh download_tiles.sh
```

A multiprocessing version of the script is also available, though it consumes significantly more RAM than the single-process version.

## Step 5: Apply Noise Filter
This step processes the tiles downloaded in Step 4.

```sh
cd scripts/download_process_lidar/noise_filter
sh noise_filter.sh
```

A multiprocessing version of the script is also available.

## Step 6: Detect and Correct Invalid Tiles
In some cases, the script may download files containing two tiles that are far apart, with one being significantly smaller. This step replaces the original file with the larger tile.

This step processes the tiles from Step 5.

```sh
cd scripts/download_process_lidar/invalid_filter
sh invalid_filter.sh
```

A multiprocessing version of the script is also available.

