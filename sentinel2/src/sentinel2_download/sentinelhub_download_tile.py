"""
This function searches and downloads sentinel L2A scenes from the AWS archive,
using the tools in sentinelhub.

Lines 146-162 currently used if the desired tile and date are already known
"""

# run with python3
from sentinelhub import WmsRequest, WcsRequest, MimeType, CRS, BBox, Geometry, MimeType
from sentinelhub import WebFeatureService, BBox, CRS, DataSource, SHConfig, SentinelHubRequest, AwsTileRequest,AwsTile,get_area_info,AwsProductRequest
from sentinelhub import BBoxSplitter, OsmSplitter, TileSplitter, CustomGridSplitter, UtmZoneSplitter, UtmGridSplitter

import os
import fiona
import datetime

import numpy as np
import pandas as pd

from shapely.geometry import shape
from oauthlib.oauth2 import BackendApplicationClient
from requests_oauthlib import OAuth2Session

client_id = ''
client_secret = ''
config = SHConfig()
config.instance_id = ''
config.aws_access_key_id = ''
config.aws_secret_access_key = ''

"""
config.sh_client_id = client_id
config.sh_client_secret = client_secret
"""
# Create a session
client = BackendApplicationClient(client_id=client_id)
oauth = OAuth2Session(client=client)
# Get token for the session
token = oauth.fetch_token(token_url='https://services.sentinel-hub.com/oauth/token',
                          client_id=client_id, client_secret=client_secret)

# All requests using this session will have an access token automatically added
resp = oauth.get("https://services.sentinel-hub.com/oauth/tokeninfo")
print(resp.content)

# open boundary shapefile/json file
site = 'Arisaig'
site_shapefile = '/home/dmilodow/DataStore_DTM/STFC/DATA/EDINAAerial/%s_2017/%s_bbox_wgs84.shp' % (site,site)
for feature in fiona.open(site_shapefile): polygon = shape(feature['geometry'])

osm_splitter = OsmSplitter([polygon], CRS.WGS84, zoom_level=8) # Open Street Map Grid
search_bbox = osm_splitter

# define time interval of interest for imagery collection
search_time_interval = ('2019-06-25T00:00:00','2019-07-26T23:59:59')

# for each tile record the desired information
datainfo = pd.DataFrame(columns=['productIdentifier','tilecode','completionDate'])
for t,bbox in enumerate(search_bbox.bbox_list):
    for tile_info in get_area_info(bbox, search_time_interval, maxcc=0.20):
        datainfo = datainfo.append({'productIdentifier': tile_info['properties']['productIdentifier'],
                                    'tilecode' : tile_info['properties']['title'][49:55],
                                    'completionDate': tile_info['properties']['completionDate'][:10],
                                    }, ignore_index=True)

# check for duplicate images and filter these
datainfo = datainfo.drop_duplicates(subset='productIdentifier')
datainfo.index = np.arange(0,len(datainfo))

datainfo['datacoveragepct'] = np.nan
datainfo['cloudpixelpct'] = np.nan

# collect metadata for each image
for i in range(len(datainfo)):
    try:
        tile_id = datainfo.productIdentifier[i]
        tile_name, time, aws_index = AwsTile.tile_id_to_tile(tile_id)
        request = AwsTileRequest(
            tile = tile_name,
            time = time,
            aws_index = aws_index,
            bands=[''],
            metafiles = ['tileInfo'],
            data_source = DataSource.SENTINEL2_L2A)
        infos = request.get_data()
        datainfo['datacoveragepct'][datainfo.productIdentifier == datainfo.productIdentifier[i]] = infos[0]['dataCoveragePercentage']
        datainfo['cloudpixelpct'][datainfo.productIdentifier == datainfo.productIdentifier[i]] = infos[0]['cloudyPixelPercentage']
    except : pass

# filter to include only S2 images with coverage above a specified % thredhold
datainfo = datainfo[datainfo.datacoveragepct > 95]
datainfo = datainfo.dropna(subset=['datacoveragepct','datacoveragepct'])
datainfo.index = np.arange(0,len(datainfo))

# download data @10m
# - Band 2  (Blue)
# - Band 3  (Green)
# - Band 4  (Red)
# download data @20m
# - Bands 5-7 (vegetation red edge)
# - Band 8A (vegetation red edge)
# - Bans 11 & 12 (SWIR)
# Also
# - AOT: Aerosol optical thickness
# - SCL: Scene classification map
# - TCI: True colour image
# - VIS: ?
# - WVP: Water vapour
tile_id = 'T30VUJ'
datainfosub = datainfo[datainfo.tilecode == tile_id]
tile_name, time, aws_index = AwsTile.tile_id_to_tile(datainfosub.productIdentifier.values[0])
data_folder = '../DATA/Sentinel2/awsdata/%s' % tile_id
if os.path.exists(data_folder) == False:
    os.mkdir(data_folder)

"""
request = AwsTileRequest(
            tile = tile_name,
            time = time,
            aws_index = aws_index,
            #bands=['R10m/B02', 'R10m/B03', 'R10m/B04', 'R10m/B08', 'R10m/TCI',
            #	   'R20m/B05', 'R20m/B06', 'R20m/B07', 'R20m/B8A', 'R20m/B11', 'R20m/B12',
            #	   'R20m/AOT', 'R20m/SCL', 'R20m/TCI', 'R20m/VIS', 'R20m/WVP'],
            data_folder = data_folder,
            data_source = DataSource.SENTINEL2_L2A,
            config=config,
            safe_format = True)
request.save_data()

data_folder = '../DATA/Sentinel2/awsdata/%s/L1C/' % tile_id
request = AwsTileRequest(
            tile = tile_name,
            time = time,
            aws_index = aws_index,
            #bands=['R10m/B02', 'R10m/B03', 'R10m/B04', 'R10m/B08', 'R10m/TCI',
            #	   'R20m/B05', 'R20m/B06', 'R20m/B07', 'R20m/B8A', 'R20m/B11', 'R20m/B12',
            #	   'R20m/AOT', 'R20m/SCL', 'R20m/TCI', 'R20m/VIS', 'R20m/WVP'],
            data_folder = data_folder,
            data_source = DataSource.SENTINEL2_L1C,
            config=config,
            safe_format = True)
request.save_data()

product_id = AwsTile(tile_name=tile_id, time=time, aws_index=0, data_source=DataSource.SENTINEL2_L1C).get_product_id()
request = AwsProductRequest(product_id=product_id,
            data_folder=data_folder,
            config=config,
            safe_format=True)

"""
## This is the code used to get a specified tile and date
config = SHConfig() # config is a SHconfig object that contains the aws credentials etc: https://sentinelhub-py.readthedocs.io/en/latest/config.html
config.aws_access_key_id = '<YOUR_ACCESS_KEY_ID>'
config.aws_secret_access_key = '<YOUR_SECRET_ACCESS_KEY>'
request = AwsTileRequest(
            tile = '30VUJ',
            time = '2019-7-15',
            aws_index = 0,
            data_folder = '../DATA/Sentinel2/awsdata/T30VUJ/',
            data_source = DataSource.SENTINEL2_L2A,
            config=config,
            safe_format = True)
request.save_data() # save's data to disk (this stage works)
#tile = request.get_data() # load data into a python object (this throws the error)

"""
# command line script to run cloud masking with fmask
fmask_sentinel2Stacked.py -o cloud.img --safedir S2B_MSIL1C_20180918T235239_N0206_R130_T56JNQ_20180919T011001.SAFE
"""
