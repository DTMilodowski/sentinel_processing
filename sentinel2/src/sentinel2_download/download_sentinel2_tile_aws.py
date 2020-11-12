"""
This function searches and downloads sentinel 2 scenes from the AWS archive,
using the tools in sentinelhub. Prior to running this script it is recommended
to query the archive for the scenes that meet your time period of interest and
cloud cover requirements, using e.g. query_sentinel2.py.

Download Sentinel 2 L1C scenes for producing cloud masks with fmask.
Download Sentinel 2 L2A scenes for atmospherically & topographically corrected,
analysis-ready data.

Code snippet at the end is a command line example for running fmask to produce
cloud masks for specified L1C scene.
"""

# run with python3
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
L1Crequest = AwsTileRequest(
            tile = '30VUJ',
            time = '2019-7-15',
            aws_index = 0,
            data_folder = '../DATA/Sentinel2/L1C/T30VUJ/',
            data_source = DataSource.SENTINEL2_L1C,
            config=config,
            safe_format = True)
L1Crequest.save_data() # save's data to disk (this stage works)

L2Arequest = AwsTileRequest(
            tile = '30VUJ',
            time = '2019-7-15',
            aws_index = 0,
            data_folder = '../DATA/Sentinel2/L2A/T30VUJ/',
            data_source = DataSource.SENTINEL2_L2A,
            config=config,
            safe_format = True)
L2Arequest.save_data() # save's data to disk (this stage works)


"""
# command line script to run cloud masking with fmask
fmask_sentinel2Stacked.py -o cloud.img --safedir S2B_MSIL1C_20180918T235239_N0206_R130_T56JNQ_20180919T011001.SAFE
"""
