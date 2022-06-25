import logging
from typing import Tuple
import pdal
from json import load, dumps
import sys
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from mpl_toolkits import mplot3d
import geopandas as gpd
from shapely.geometry import Polygon
from shapely.geometry import Point
from logger_creator import CreateLogger
from subsampler import CloudSubSampler

# A class that handles all the data fetching tasks

class DataFetcher():
    
    # Initializing the class by adding polygon CRS format and region

    def __init__(self, polygon: Polygon, epsg: str, region: str = ''):
        try:
            self.data_location = "https://s3-us-west-2.amazonaws.com/usgs-lidar-public/"
            # Finding the polygon edges for bound
            minx, miny, maxx, maxy = self.get_polygon_edges(polygon, epsg)

            if(region != ''):
                self.region = self.check_region(region)
                self.file_location = self.data_location + self.region + "/ept.json"
            else:
                self.region = self.get_region_by_bounds(minx, miny, maxx, maxy)
                self.file_location = self.region

            self.load_pipeline_template()
            self.epsg = epsg

        except Exception as e:
            sys.exit(1)
        
    
    # Checking if the region exsits in the listed text file

    def check_region(self, region: str) -> str:
        
        with open('../filename.txt', 'r') as locations:
            locations_list = locations.readlines()

        if(region in locations_list):
            return region
        else:
            sys.exit(1)

    
    # A function to get the region name given the bounds or polygon edges

    def get_region_by_bounds(self, minx: float, miny: float, maxx: float, maxy: float, indx: int = 1):

        aws_dataset_info_csv = pd.read_csv('../aws_dataset.csv')
        for index, bound in enumerate(aws_dataset_info_csv['Bound/s'].to_list()):
            bound = bound.strip('][').replace(
                ']', '').replace('[', '').split(',')
            bound = list(map(float, bound))

            bminx, bminy, bmaxx, bmaxy = bound[0 * indx], bound[1 *
                                                                indx], bound[3 * indx], bound[4 * indx]

            if((minx >= bminx and maxx <= bmaxx) and (miny >= bminy and maxy <= bmaxy)):
                access_url = aws_dataset_info_csv['Access Url/s'].to_list()[
                    index][2:-2]

                region = aws_dataset_info_csv['Region/s'].to_list()[
                    index] + '_' + aws_dataset_info_csv['Year/s'].to_list()[index][2:-2]

                return access_url
            else:
                sys.exit()


    # Creating and loading a pipeline given a filename that is optional if not given we will pass our  template
    def load_pipeline_template(self, file_name: str = '../pipeline_template.json') -> None:
        
        try:
            with open(file_name, 'r') as read_file:
                template = load(read_file)

            self.template_pipeline = template


        except Exception as e:
            sys.exit(1)

    
    # Given the polygon finding out the polygon edges 

    def get_polygon_edges(self, polygon: Polygon, epsg: str) -> tuple:
        
        try:
            grid = gpd.GeoDataFrame([polygon], columns=["geometry"])
            grid.set_crs(epsg=epsg, inplace=True)

            grid['geometry'] = grid.geometry.to_crs(epsg=3857)

            minx, miny, maxx, maxy = grid.geometry[0].bounds
            # bounds: ([minx, maxx], [miny, maxy])
            self.extraction_bounds = f"({[minx, maxx]},{[miny,maxy]})"

            # Cropping Bounds
            self.polygon_cropping = self.get_crop_polygon(grid.geometry[0])

            grid['geometry'] = grid.geometry.to_crs(epsg=epsg)
            self.geo_df = grid

            return minx, miny, maxx, maxy

        except Exception as e:
            sys.exit(1)

    
    # Creating the polygon crop for the pipeline croping function

    def get_crop_polygon(self, polygon: Polygon) -> str:
        
        polygon_cords = 'POLYGON(('
        for i in list(polygon.exterior.coords):
            polygon_cords += f'{i[0]} {i[1]},'

        polygon_cords = polygon_cords[:-1] + '))'

        return polygon_cords

    # Modifies and creating the pipeline
    def construct_simple_pipeline(self) -> None:

        self.pipeline = []
        reader = self.template_pipeline['reader']
        reader['bounds'] = self.extraction_bounds
        reader['filename'] = self.file_location
        self.pipeline.append(reader)

        cropper = self.template_pipeline['cropping_filter']
        cropper['polygon'] = self.polygon_cropping
        self.pipeline.append(cropper)

        self.pipeline.append(self.template_pipeline['range_filter'])
        self.pipeline.append(self.template_pipeline['assign_filter'])

        reprojection = self.template_pipeline['reprojection_filter']
        reprojection['out_srs'] = f"EPSG:{self.epsg}"
        self.pipeline.append(reprojection)

        self.pipeline = pdal.Pipeline(dumps(self.pipeline))

    # A function to create a pipeline for tif data generation given a resolution and windowsize   
    def construct_pipeline_template_tif(self, file_name: str, resolution: int = 1, window_size: int = 6, tif_values: list = ["all"]):
        self.pipeline = []
        reader = self.template_pipeline['reader']
        reader['bounds'] = self.extraction_bounds
        reader['filename'] = self.data_location + self.region + "/ept.json"
        self.pipeline.append(reader)

        self.pipeline.append(self.template_pipeline['range_filter'])
        self.pipeline.append(self.template_pipeline['assign_filter'])

        reprojection = self.template_pipeline['reprojection_filter']
        reprojection['out_srs'] = f"EPSG:{self.epsg}"
        self.pipeline.append(reprojection)

        # Filtering
        self.pipeline.append(self.template_pipeline['smr_filter'])
        self.pipeline.append(self.template_pipeline['smr_range_filter'])

        laz_writer = self.template_pipeline['laz_writer']
        laz_writer['filename'] = f"{file_name}_{self.region}.laz"
        self.pipeline.append(laz_writer)

        tif_writer = self.template_pipeline['tif_writer']
        tif_writer['filename'] = f"{file_name}_{self.region}.tif"
        tif_writer['output_type'] = tif_values
        tif_writer["resolution"] = resolution
        tif_writer["window_size"] = window_size
        self.pipeline.append(tif_writer)

        self.pipeline = pdal.Pipeline(dumps(self.pipeline))

    # A function to get the data from the pipeline
    def get_data(self):

        try:
            self.data_count = self.pipeline.execute()
            self.create_cloud_points()
            self.original_cloud_points = self.cloud_points
            self.original_elevation_geodf = self.get_elevation_geodf()
        except Exception as e:
            sys.exit(1)