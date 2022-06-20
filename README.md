# AgriTech_lidar_ETL

**Table of content**

- [Overview](#overview)
- [Requirements](#requirements)
- [Install](#install)
- [Data](#data)
- [Notebooks](#notebooks)
- [Scripts](#scripts)
- [Test](#test)

## Overview

> In this project I took data using API provided by USGS_3DEP ( United States Geological Survey 3D Elevation Program). AgriTech is a company working on maize farms and this project is done for the study of maize farms for water flow across different geographical areas. Extraction, Visualization and transformation of data were achieved in this project.

## Requirements

- PDAL
- Laspy
- Geopandas

## Install

```
git clone hhttps://github.com/Micky373/AgriTech_lidar_ETL.git
cd AgriTech_lidar_ETL
pip install -r requirements.txt
```

## Data

- The USGS (United States Geological Survey) 3D Elevation Program (3DEP) provides access to the 3DEP repository's lidar point cloud data. Users may interact with enormous amounts of lidar point cloud data without having to download them to local PCs thanks to 3DEP's adoption of cloud storage and processing.
- The EPT format point cloud data is freely available via AWS. Entwine Point Tile (EPT) is an octree-based storage format for point cloud data that is simple and adaptable. JSON metadata and binary point data are both present in the organization of an EPT dataset. The JSON file is essential metadata for understanding the contents of an EPT dataset.

## Notebooks

> All the analysis and examples of implementation will be here in the form of .ipynb file

## Scripts

> All the modules for the analysis are found here

## Test

> All the unit and integration tests are found here
