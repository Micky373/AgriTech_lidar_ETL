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


    def load_pipeline_template(self, file_name: str = './pipeline_template.json') -> None:
        
        try:
            with open(file_name, 'r') as read_file:
                template = load(read_file)

            self.template_pipeline = template

            logger.info('Successfully Loaded Pdal Pipeline Template')

        except Exception as e:
            logger.exception('Failed to Load Pdal Pipeline Template')
            sys.exit(1)