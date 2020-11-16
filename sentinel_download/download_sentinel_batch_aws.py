"""
This function downloads multiple sentinel 1 and 2 scenes from the AWS archive,
using the tools in sentinelhub.

A prerequisite to running this script is to run the sentinelsat query of the
archive to generate a list of products to be downloaded.

Download Sentinel 1 GRD scenes in HH and HV polarisation
Download Sentinel 2 L1C scenes for producing cloud masks with fmask.
Download Sentinel 2 L2A scenes for atmospherically & topographically corrected,
analysis-ready data.

Code snippet at the end is a command line example for running fmask to produce
cloud masks for specified L1C scene.
"""

# run with python3
import numpy as np

from sentinelhub import SHConfig, AwsTileRequest,AwsTile,AwsProductRequest
from oauthlib.oauth2 import BackendApplicationClient
from requests_oauthlib import OAuth2Session

client_id = ''
client_secret = ''
"""
config = SHConfig()
config.instance_id = ''
config.aws_access_key_id = ''
config.aws_secret_access_key = ''
"""
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

## This is the code used to get a specified tile and date
config = SHConfig() # config is a SHconfig object that contains the aws credentials etc: https://sentinelhub-py.readthedocs.io/en/latest/config.html
config.aws_access_key_id = '<YOUR_ACCESS_KEY_ID>'
config.aws_secret_access_key = '<YOUR_SECRET_ACCESS_KEY>'

## Now load in the lists of tiles
sites = ['Ardfern1', 'Ardfern2', 'Arisaig', 'Auchteraw', 'GlenLoy', 'Mandally',  'Achdalieu']
s1list = []
s2list = []
for site in sites:
    s1scenes = np.load('query_results/%s_query_results_sentinel_1.npz' % site,
                        allow_pickle=True)['arr_0'][()]
    for scene in list(s1scenes.keys()):
        s1list.append(s1scenes[scene]['title'])

    s2scenes = np.load('query_results/%s_query_results_sentinel_2.npz' % site,
                        allow_pickle=True)['arr_0'][()]
    for scene in list(s2scenes.keys()):
        s2list.append(s2scenes[scene]['title'])

s1list = np.unique(s1list)
s2list = np.unique(s2list)

# download the sentinel 1 data
for id in s1list:
    GRDrequest = AwsProductRequest(
                product_id = id,
                data_folder = '../DATA/Sentinel1/GRD/',
                config=config,
                safe_format = True)
    GRDrequest.save_data() # save's data to disk (this stage works)

# download the sentinel 2 data
for id in s2list:
    L2Arequest = AwsProductRequest(
                product_id = id,
                data_folder = '../DATA/Sentinel2/L2A/',
                config=config,
                safe_format = True)
    L2Arequest.save_data()

    # get tile and date information to retrieve corresponing L1C tile
    tile = id.split('_')[5]
    year = id.split('_')[6][:4]
    month = id.split('_')[6][4:6]
    day = id.split('_')[6][6:8]
    L1Crequest = AwsTileRequest(
            tile = tile,
            time = '%s-%s-%s' % (year,month,day),
            data_folder = '../DATA/Sentinel2/L1C/',
            data_source = DataSource.SENTINEL2_L1C,
            config=config,
            safe_format = True)
    L1Crequest.save_data()

"""
# command line script to run cloud masking with fmask
fmask_sentinel2Stacked.py -o cloud.img --safedir S2B_MSIL1C_20180918T235239_N0206_R130_T56JNQ_20180919T011001.SAFE
"""
