"""
This function uses the pyroSAR processing pipeline to process the original GRD
tiles into corrected backscatter (see Tuckenbrodt et al 2019).
- Read S1 GRD tile
- Thermal noise removal
- Border noise removal
- Apply orbit file
- Calibration
- Terrain flattening
- (optional) speckle filter
- Terrain correction
- Conversion of backscatter amplitude to dB units
"""
import os
import sys
sys.path.append('/home/dmilodow/scratch/ESA_SNAP/bin/gpt/')
import numpy as np
import glob as glob
import fiona
from pyroSAR.snap import geocode
datadir = '/disk/scratch/local.2/dmilodow/Sentinel1/GRDtiles/'
crs_epsg = 27700 # OSGB grid
dem_ndv = -9999
scenes = glob.glob('%sS1*.zip' % datadir)
resolution=10

sites = ['Arisaig','Achdalieu','Ardfern1','Ardfern2','Auchteraw','GlenLoy','Mandally']
for site in sites:
    # set up directory structture for output layers
    outdirbase = '/disk/scratch/local.2/dmilodow/Sentinel1/%s/' % site
    outdir1 = '%sprocessed_no_filter/' % outdirbase
    outdir2 = '%sprocessed_refine_lee/' % outdirbase
    if os.path.isdir(outdirbase)==False:
        os.mkdir(outdirbase)
    if os.path.isdir(outdir1)==False:
        os.mkdir(outdir1)
    """
    if os.path.isdir(outdir2)==False:
        os.mkdir(outdir2)
    """
    if 'Ardfern' in site:
        site_shapefile = '/home/dmilodow/DataStore_DTM/STFC/DATA/EDINAAerial/%s_2015/%s_bbox_wgs84.shp' % (site,site)
    else:
        site_shapefile = '/home/dmilodow/DataStore_DTM/STFC/DATA/EDINAAerial/%s_2017/%s_bbox_wgs84.shp' % (site,site)

    dem_file = '/home/dmilodow/DataStore_DTM/STFC/DATA/OStopo/%s/%s_DTM_OSgrid.tif' % (site,site)

    # process with pyroSAR
    for scene in scenes:
        geocode(scene, outdir=outdir1, t_srs=crs_epsg, tr=resolution, scaling='dB',
                polarizations='all', geocoding_type='Range-Doppler',
                removeS1BorderNoise=True, removeS1BorderNoiseMethod='pyroSAR',
                removeS1ThermalNoise=True, terrainFlattening=True,
                externalDEMFile=dem_file, externalDEMNoDataValue=dem_ndv,
                externalDEMApplyEGM=True, speckleFilter=False,
                refarea='gamma0', shapefile=site_shapefile)
        """
        geocode(scene, outdir=outdir2, t_srs=crs_epsg, tr=resolution, scaling='dB',
                polarizations='all', geocoding_type='Range-Doppler',
                removeS1BorderNoise=True, removeS1BorderNoiseMethod='pyroSAR',
                removeS1ThermalNoise=True, terrainFlattening=True,
                externalDEMFile=dem_file, externalDEMNoDataValue=dem_ndv,
                externalDEMApplyEGM=True, speckleFilter='Refined Lee',
                refarea='gamma0', shapefile=site_shapefile)
        """

# Regrid final rasters onto the same grid. For now, use a nearest neighbour
# interpolation
for site in sites:
    if 'Ardfern' in site:
        site_shapefile = '/home/dmilodow/DataStore_DTM/STFC/DATA/EDINAAerial/%s_2015/%s_bbox.shp' % (site,site)
    else:
        site_shapefile = '/home/dmilodow/DataStore_DTM/STFC/DATA/EDINAAerial/%s_2017/%s_bbox.shp' % (site,site)

    outdir = '/disk/scratch/local.2/dmilodow/Sentinel1/%s/processed_no_filter/' % site
    scenes = glob.glob('%sS1A*.tif' % outdir)
    # open shapefile and extract bounding box
    shp = fiona.open(site_shapefile)
    min_x=np.inf;max_x=-np.inf;min_y=np.inf;max_y=-np.inf

    for object in shp:
        for poly in object['geometry']['coordinates']:
            for coords in poly:
                min_x = np.floor(np.min([min_x,coords[0]])/resolution)*resolution
                min_y = np.floor(np.min([min_y,coords[1]])/resolution)*resolution
                max_x = np.ceil(np.max([max_x,coords[0]])/resolution)*resolution
                max_y = np.ceil(np.max([max_y,coords[1]])/resolution)*resolution

    # now use gdal to regrid onto the desired grid
    for scene in scenes:
        scene_regrid = '%s_regrid.tif' % scene[:-4]
        os.system('gdalwarp -tr %f %f -te %f %f %f %f %s %s' % (resolution,resolution,
                                                                min_x,min_y,max_x,max_y,
                                                                scene,scene_regrid))
