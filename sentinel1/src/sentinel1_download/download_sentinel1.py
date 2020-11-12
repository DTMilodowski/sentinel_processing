"""
This function downloads S1 GRD files for a region and time period of interest
using the sentinelsat package. Region of interest is specified using a
shapefile (site_shapefile)

Note that the quick-access scihub data servers only host the past 12 months of
data. Older scenes have to be downloaded from the Long Term Archive (LTA),
which is a much slower process (need to request and then wait for the files to
be transferred; limited number of requests per hour). This script has a rather
inelegant solution. For large numbers of data layers, consider alternatives
(e.g. AWS)
"""
import os
import sys
import datetime
from time import sleep
from sentinelsat import SentinelAPI, read_geojson, geojson_to_wkt

"""
# code to convert shapefile to geojson for ingestion into sentinelsat api
import geopandas as gpd
for site in sites:
    if 'Ardfern' in site:
        site_shapefile = '/home/dmilodow/DataStore_DTM/STFC/DATA/EDINAAerial/%s_2015/%s_bbox_wgs84.shp' % (site,site)
    else:
        site_shapefile = '/home/dmilodow/DataStore_DTM/STFC/DATA/EDINAAerial/%s_2017/%s_bbox_wgs84.shp' % (site,site)

        file = gpd.read_file(site_shapefile)
        file.to_file('%s.json' % site_shapefile[:-4], driver="GeoJSON")
"""

username = '<username>'
pswd = '<password>'
download_api = SentinelAPI(username, pswd, api_url='https://scihub.copernicus.eu/dhus/')

download_dir = '/disk/scratch/local.2/dmilodow/Sentinel1/GRDtiles/'
sites = ['Ardfern1','Ardfern2','Arisaig','Auchteraw','GlenLoy','Mandally','Achdalieu']

start_date = datetime.datetime.strptime('2019-04-01','%Y-%m-%d').date()
end_date = datetime.datetime.strptime('2019-09-30','%Y-%m-%d').date()
for site in sites:
    if 'Ardfern' in site:
        site_json = '/home/dmilodow/DataStore_DTM/STFC/DATA/EDINAAerial/%s_2015/%s_bbox_wgs84.json' % (site,site)
    else:
        site_json = '/home/dmilodow/DataStore_DTM/STFC/DATA/EDINAAerial/%s_2017/%s_bbox_wgs84.json' % (site,site)

    footprint = geojson_to_wkt(read_geojson(site_json))
    scenes = download_api.query(footprint,
                     date = (start_date, end_date),
                     productType='GRD',
                     sensoroperationalmode='IW',
                     polarisationmode='VV VH',
                     platformname = 'Sentinel-1')
                     #cloudcoverpercentage = (0, 30))

    scenes_retry = []
    for scene_id in scenes:
        scene = scenes[scene_id]
        print('date: %s; scene: %s' % (scene['beginposition'][:10],scene['title']))
        if not download_api.is_online(scene_id):
            if not os.path.isfile('%s/%s.zip' % (download_dir,scene['title'])):
                scenes_retry.append(scene)
        else:
            try:
                download_api.download(scene_id,directory_path=download_dir)
            except:
                scenes_retry.append(scene)

    print('%i scenes remaining to be requested' % len(scenes_retry))
    problem_scenes = []
    while len(scenes_retry)>0:
        scenes_offline=[]
        for ii,scene in enumerate(scenes_retry):
            print('%i of %i; date: %s; scene: %s' % (ii,len(scenes_retry),scene['beginposition'][:10],scene['title']))
            move_to_next_scene=False
            scene_tested = False
            count = 0
            while not move_to_next_scene:
                # if available, download now
                try:
                    count+=1
                    online=download_api.is_online(scene_id)
                    download_api.download(scene_id,directory_path=download_dir)
                    if not online:
                        if os.path.isfile('%s/%s.zip' % (download_dir,scene['title'])):
                            scene_tested=True
                            move_to_next_scene=True
                        else:
                            if not scene_tested:
                                scenes_offline.append(scene)
                                scene_tested = True
                    move_to_next_scene=True
                except:
                    if count>2:
                        if scene_id not in problem_scenes:
                            problem_scenes.append(scene_id)
                        move_to_next_scene=True
                    print('reached quota limit! Trying again in 10 minutes...',end='\r')
                    sleep(60*5)
                    print('reached quota limit! Trying again in 5 minutes...',end='\r')
                    sleep(60)
                    print('reached quota limit! Trying again in 4 minutes...',end='\r')
                    sleep(60)
                    print('reached quota limit! Trying again in 3 minutes...',end='\r')
                    sleep(60)
                    print('reached quota limit! Trying again in 2 minutes...',end='\r')
                    sleep(60)
                    print('reached quota limit! Trying again in 1 minute...',end='\r')
                    sleep(30)
                    print('reached quota limit! Trying again in 30 seconds...',end='\r')
                    sleep(10)
                    print('reached quota limit! Trying again in 20 seconds...',end='\r')
                    sleep(10)
                    print('reached quota limit! Trying again in 10 seconds...',end='\r')
                    sleep(10)

        scenes_retry=scenes_offline.copy()
        print('%i scenes remaining to be requested' % len(scenes_retry))

    # final check of downloads to ensure all downloaded
    for scene in scenes:
        print('date: %s; scene: %s' % (scene['beginposition'][:10],scene['title']))
        if not download_api.is_online(scene_id):
            if not os.path.isfile('%s/%s.zip' % (download_dir,scene['title'])):
                scenes_retry.append(scene)
        else:
            download_api.download(scene_id,directory_path=download_dir)

"""
sites = ['Achdalieu','Ardfern1','Ardfern2','Arisaig','Auchteraw','GlenLoy','Mandally']
import os
for site in sites:
    if 'Ardfern' in site:
        shp1 = '/home/dmilodow/DataStore_DTM/STFC/DATA/EDINAAerial/%s_2015/%s_bbox.shp' % (site,site)
        shp2 = '/home/dmilodow/DataStore_DTM/STFC/DATA/EDINAAerial/%s_2015/%s_bbox_wgs84.shp' % (site,site)
    else:
        shp1 = '/home/dmilodow/DataStore_DTM/STFC/DATA/EDINAAerial/%s_2017/%s_bbox.shp' % (site,site)
        shp2 = '/home/dmilodow/DataStore_DTM/STFC/DATA/EDINAAerial/%s_2017/%s_bbox_wgs84.shp' % (site,site)

    os.system('ogr2ogr -f "ESRI Shapefile" -s_srs EPSG:27700 -t_srs EPSG:4326 %s %s' % (shp2,shp1))

for site in sites:
    prefix = '/exports/csce/datastore/geos/users/dmilodow/STFC/DATA/OStopo/%s/%s' % (site,site)
    os.system('gdalwarp -dstnodata -9999 -t_srs EPSG:27700 -r cubicspline %s_DTM.tif %s_DTM_OSgrid.tif' % (prefix,prefix))
"""
