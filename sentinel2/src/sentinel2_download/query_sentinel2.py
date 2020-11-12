"""
This function queries the S2 L2A archive via scihub and returns a list of tiles,
with metadata for a region and time period of interest, with an option to filter
by cloud cover. It uses the sentinelsat package, and a ROI is specified using a
geoJSON file.

Note that to use fmask to isolate cloud cover more accurately than the default
layer, it is necessary to download the corresponding L1C tile. It is recommended
that ultimately the AWS route is taken for downloading the datasets
"""
import datetime
from sentinelsat import SentinelAPI, read_geojson, geojson_to_wkt

username = '<username>'
pswd = '<password>'
download_api = SentinelAPI(username, pswd, api_url='https://scihub.copernicus.eu/dhus/')

download_dir = '/disk/scratch/local.2/dmilodow/Sentinel2/L2A/'
sites = ['Ardfern1','Ardfern2','Arisaig','Auchteraw','GlenLoy','Mandally','Achdalieu']

start_date = datetime.datetime.strptime('2019-04-01','%Y-%m-%d').date() # start date for time period of interest
end_date = datetime.datetime.strptime('2019-09-30','%Y-%m-%d').date() # end date for time period of interest

max_cloud_cover = 30 # percent

for site in sites:
    if 'Ardfern' in site:
        site_json = '/home/dmilodow/DataStore_DTM/STFC/DATA/EDINAAerial/%s_2015/%s_bbox_wgs84.json' % (site,site)
    else:
        site_json = '/home/dmilodow/DataStore_DTM/STFC/DATA/EDINAAerial/%s_2017/%s_bbox_wgs84.json' % (site,site)

    footprint = geojson_to_wkt(read_geojson(site_json))
    scenes = download_api.query(footprint,
                     date = (start_date, end_date),
                     platformname = 'Sentinel-2',
                     processinglevel = 'Level-2A',
                     cloudcoverpercentage = (0, max_cloud_cover))
    np.savez(scenes,'%s_query_results.npz' % site)
