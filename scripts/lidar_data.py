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
            logger.error('Region Not Available')
            sys.exit(1)
