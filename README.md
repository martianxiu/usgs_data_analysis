# Geospatial Data Analysis in the paper "Advancing ALS Applications with Large-Scale Pre-training"

## Introduction
This repository contains the code for geospatial data analysis and dataset development used in the technical report [Advancing ALS Applications with Large-Scale Pre-training: Dataset Development and Downstream Assessment](https://arxiv.org/abs/2501.05095).

## Installation
We use Conda to create a virtual environment for this project.

### Create and activate the environment:
```sh
conda create -y -n geospatial python=3.10
conda activate geospatial
```

### Install required libraries:
```sh
pip install lazrs opencv-python joblib matplotlib geopandas laspy py3dep utm scikit-image rasterio colorama jupyterlab
conda install -c conda-forge pdal
conda install -y ipykernel
```

### Install Jupyter kernel:
```sh
python -m ipykernel install --user --name=geospatial --display-name "geospatial"
```

## Data Structure
Raw and processed data are stored in the `data` folder. After processing the datasets using scripts in the `scripts` folder, the directory structure should be as follows:

```
.
├── 3DEP_30m_clip
│   ├── AK_BrooksCamp_2012_3dep30m.tif
│   └── ...
├── NLCD_all
│   ├── NLCD_landcover_2021_release_all_files_20230630.zip
│   └── ...
├── point_cloud_boundary
│   └── 20240627.topojson
├── processed_tiles_developed_forest
│   ├── CO_Eastern_North_Priority_2018
│   │   ├── tile_1.laz
│   │   ├── tile_2.laz
│   │   └── ...
│   └── ...
├── processed_tiles_developed_forest_invalid_filtered
│   ├── CO_Eastern_North_Priority_2018
│   │   ├── tile_1.laz
│   │   ├── tile_2.laz
│   │   └── ...
│   └── ...
├── raw_tiles_developed_forest
│   ├── CO_Eastern_North_Priority_2018
│   │   ├── tile_1.laz
│   │   └── ...
│   └── ...
├── sampled_tiles
│   └── sample_1000_developed_forest.gpkg
```

## Data Processing
All data processing scripts are located in the `scripts` folder. Please refer to the scripts for detailed processing steps.


## Citation
If you find our work useful in your research, please consider citing:
```
@misc{xiu2025advancingalsapplicationslargescale,
      title={Advancing ALS Applications with Large-Scale Pre-training: Dataset Development and Downstream Assessment}, 
      author={Haoyi Xiu and Xin Liu and Taehoon Kim and Kyoung-Sook Kim},
      year={2025},
      eprint={2501.05095},
      archivePrefix={arXiv},
      primaryClass={cs.CV},
      url={https://arxiv.org/abs/2501.05095}, 
}
```
