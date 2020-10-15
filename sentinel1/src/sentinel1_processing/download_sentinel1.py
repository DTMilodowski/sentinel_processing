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

sys.path.append('/exports/csce/datastore/geos/users/dmilodow/STFC/esa_sentinel')
import sentinel_api as api
from sentinelsat import SentinelAPI
username = '<username>'
pswd = '<password>'

download_dir = '/disk/scratch/local.2/dmilodow/Sentinel1/GRDtiles/'
sites = ['Ardfern1','Ardfern2','Arisaig','Auchteraw','GlenLoy','Mandally','Achdalieu']

start_date = datetime.datetime.strptime('2019-04-01','%Y-%m-%d').date()
end_date = datetime.datetime.strptime('2019-09-30','%Y-%m-%d').date()

for site in sites:
    if 'Ardfern' in site:
        site_shapefile = '/home/dmilodow/DataStore_DTM/STFC/DATA/EDINAAerial/%s_2015/%s_bbox_wgs84.shp' % (site,site)
    else:
        site_shapefile = '/home/dmilodow/DataStore_DTM/STFC/DATA/EDINAAerial/%s_2017/%s_bbox_wgs84.shp' % (site,site)

    # please also specify the Hub URL:
    # All Sentinel-1 and -2 scenes beginning from 15th Nov. 2015: https://scihub.copernicus.eu/apihub/
    # All historic Sentinel-1 scenes: https://scihub.copernicus.eu/dhus/
    s1 = api.SentinelDownloader(username, pswd, api_url='https://scihub.copernicus.eu/dhus/')

    # set directory for
    # - filter scenes list with existing files
    # - set directory path for data download
    s1.set_download_dir(download_dir)

    # load geometries from shapefile
    s1.load_sites(site_shapefile)

    # search for scenes with some restrictions (e.g., minimum overlap 1%)
    s1.search('S1A*', min_overlap=0.9, start_date='%i-%i-%i' % (start_date.year, start_date.month, start_date.day),
    end_date = '%i-%i-%i' % (end_date.year, end_date.month, end_date.day),
    date_type='beginPosition',
    productType='GRD', sensoroperationalmode='IW',
    orbitdirection='Ascending',polarisationmode='VV VH')

    s1.search('S1A*', min_overlap=0.9, start_date='%i-%i-%i' % (start_date.year, start_date.month, start_date.day),
    end_date = '%i-%i-%i' % (end_date.year, end_date.month, end_date.day),date_type='beginPosition',
    productType='GRD', sensoroperationalmode='IW',
    orbitdirection='Descending',polarisationmode='VV VH')

    s1.search('S1B*', min_overlap=0.9, start_date='%i-%i-%i' % (start_date.year, start_date.month, start_date.day),
    end_date = '%i-%i-%i' % (end_date.year, end_date.month, end_date.day),
    date_type='beginPosition',
    productType='GRD', sensoroperationalmode='IW',
    orbitdirection='Ascending',polarisationmode='VV VH')

    s1.search('S1B*', min_overlap=0.9, start_date='%i-%i-%i' % (start_date.year, start_date.month, start_date.day),
    end_date = '%i-%i-%i' % (end_date.year, end_date.month, end_date.day),
    date_type='beginPosition',
    productType='GRD', sensoroperationalmode='IW',
    orbitdirection='Descending',polarisationmode='VV VH')

    # you can either write results to a bash file for wget or download files directly in this script
    # s1.write_results('wget', 'sentinel_api_s1_download.sh')
    #s1.download_all()
    scenes = s1.get_scenes()
    scenes_retry = []
    download_api = SentinelAPI(username, pswd, api_url='https://scihub.copernicus.eu/dhus/')
    for scene in scenes:
        print('date: %s; scene: %s' % (scene['beginposition'][:10],scene['title']))
        #download_api.download(scene['id'],directory_path=download_dir)
        if not download_api.is_online(scene['id']):
            if not os.path.isfile('%s/%s.zip' % (download_dir,scene['title'])):
                scenes_retry.append(scene)
        else:
            try:
                download_api.download(scene['id'],directory_path=download_dir)
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
                    online=download_api.is_online(scene['id'])
                    download_api.download(scene['id'],directory_path=download_dir)
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
                        if scene['id'] not in problem_scenes:
                            problem_scenes.append(scene['id'])
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
        #download_api.download(scene['id'],directory_path=download_dir)
        if not download_api.is_online(scene['id']):
            if not os.path.isfile('%s/%s.zip' % (download_dir,scene['title'])):
                scenes_retry.append(scene)
        else:
            download_api.download(scene['id'],directory_path=download_dir)

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
