"""
temporal_filter_SAR_backscatter_annual
================================================================================
Average backscatter values from a temporal stack of Sentinel 1 SAR backscatter
rasters for one year. Treat ascending (A) and descending (D) orbits separately.
Output: generates geotiffs for the annual average and standard deviation of the
VV and VH backscatter amplitudes and their difference VV-VH
--------------------------------------------------------------------------------
"""
import glob
import os
import sys
sys.path.append('../data_io/')

import numpy as np
import xarray as xr

from matplotlib import pyplot as plt
import seaborn as sns
sns.set()

import data_io as io

plt.rcParams["axes.axisbelow"] = False

def rescale(array,llim=None,ulim=None):
    if llim is None:
        llim=np.nanmin(array)
    if ulim is None:
        ulim=np.nanmax(array)
    return np.interp(array, (llim, ulim), (0, 1))

#sites = ['Achdalieu','Ardfern1','Ardfern2','Arisaig','Auchteraw','GlenLoy','Mandally']
sites = ['Arisaig']
year = 2019
#sites = ['Ardfern1']
for site in sites:
    for orbit in ['A','D']:
        print('Temporal filter. Processing site: %s' % site)
        basedir = '/disk/scratch/local.2/dmilodow/Sentinel1/%s/' % site
        indir = '%s/processed_no_filter/' % basedir
        outdir = '%s/processed_temporal/%i' % (basedir,year)
        if os.path.isdir(basedir)==False:
            print("base directory not found: %s" % basedir)
            break
        if os.path.isdir(indir)==False:
            print("data directory not found: %s" % indir)
            break
        if os.path.isdir(outdir)==False:
            if os.path.isdir('%s/processed_temporal/' % basedir)==False:
                os.mkdir('%s/processed_temporal/' % basedir)
            os.mkdir(outdir)

        vh_files = glob.glob('%s*%s_%i*_VH_tnr_bnr_Orb_Cal_TF_TC_dB_regrid.tif' % (indir,orbit,year))
        vv_files = glob.glob('%s*%s_%i*_VV_tnr_bnr_Orb_Cal_TF_TC_dB_regrid.tif' % (indir,orbit,year))

        n_vh = len(vh_files)
        summer_mask = np.zeros(n_vh,dtype='bool')

        print('\t polarisation: VH')
        vh_mean = xr.open_rasterio(vh_files[0])
        vh_std = vh_mean.copy(deep=True)
        bands,rows,cols = vh_mean.values.shape
        vh_stack = np.zeros((n_vh,bands,rows,cols))*np.nan
        for ii,file in enumerate(vh_files):
            month = int(file.split('/')[-1][16:18])
            summer_mask[ii] = (month<10)*(month>3)
            vh_stack[ii]=xr.open_rasterio(file).values
        vh_stack[vh_stack==0]=np.nan

        print('\t polarisation: VV')
        n_vv = len(vv_files)
        vv_stack = np.zeros((n_vv,bands,rows,cols))*np.nan
        for ii,file in enumerate(vv_files):
            vv_stack[ii]=xr.open_rasterio(file).values
        vv_stack[vv_stack==0]=np.nan

        diff_stack = vv_stack-vh_stack
        nancount = np.sum(np.sum(np.sum(np.isnan(diff_stack),axis=3),axis=2),axis=1)
        nanfrac = nancount/(rows*cols)

        """
        vv_mean = vh_mean.copy(deep=True)
        vv_std = vh_mean.copy(deep=True)
        diff_mean = vh_mean.copy(deep=True)
        diff_std = vh_mean.copy(deep=True)

        vh_mean.values = rescale(np.nanmean(vh_stack,axis=0))
        vh_std.values = np.nanstd(vh_stack,axis=0)
        vv_mean.values = rescale(np.nanmean(vv_stack,axis=0))
        vv_std.values = np.nanstd(vv_stack,axis=0)
        diff_mean.values = rescale(np.nanmean(diff_stack,axis=0))
        diff_std.values = np.nanstd(diff_stack,axis=0)
        """

        vh_mean_summer = vh_mean.copy(deep=True)
        vh_std_summer = vh_mean.copy(deep=True)
        vv_mean_summer = vh_mean.copy(deep=True)
        vv_std_summer = vh_mean.copy(deep=True)
        diff_mean_summer = vh_mean.copy(deep=True)
        diff_std_summer = vh_mean.copy(deep=True)

        vh_mean_summer.values = rescale(np.nanmean(vh_stack[summer_mask],axis=0))
        vh_std_summer.values = np.nanstd(vh_stack[summer_mask],axis=0)
        vv_mean_summer.values = rescale(np.nanmean(vv_stack[summer_mask],axis=0))
        vv_std_summer.values = np.nanstd(vv_stack[summer_mask],axis=0)
        diff_mean_summer.values = rescale(np.nanmean(diff_stack[summer_mask],axis=0))
        diff_std_summer.values = np.nanstd(diff_stack[summer_mask],axis=0)

        winter_mask = summer_mask==False
        vh_mean_winter = vh_mean.copy(deep=True)
        vh_std_winter = vh_mean.copy(deep=True)
        vv_mean_winter = vh_mean.copy(deep=True)
        vv_std_winter = vh_mean.copy(deep=True)
        diff_mean_winter = vh_mean.copy(deep=True)
        diff_std_winter = vh_mean.copy(deep=True)

        vh_mean_winter.values = rescale(np.nanmean(vh_stack[winter_mask],axis=0))
        vh_std_winter.values = np.nanstd(vh_stack[winter_mask],axis=0)
        vv_mean_winter.values = rescale(np.nanmean(vv_stack[winter_mask],axis=0))
        vv_std_winter.values = np.nanstd(vv_stack[winter_mask],axis=0)
        diff_mean_winter.values = rescale(np.nanmean(diff_stack[winter_mask],axis=0))
        diff_std_winter.values = np.nanstd(diff_stack[winter_mask],axis=0)

        print('\t Writing to geotiff')
        vv_mean_file = '%s/S1A__IW__%s_%i_VV_tnr_bnr_Orb_Cal_TF_TC_dB_temporal_mean_summer.tif' % (outdir,orbit,year)
        io.write_xarray_to_GeoTiff(vv_mean_summer[0],vv_mean_file)
        vh_mean_file = '%s/S1A__IW__%s_%i_VH_tnr_bnr_Orb_Cal_TF_TC_dB_temporal_mean_summer.tif' % (outdir,orbit,year)
        io.write_xarray_to_GeoTiff(vh_mean_summer[0],vh_mean_file)
        diff_mean_file = '%s/S1A__IW__%s_%i_diffVVVH_tnr_bnr_Orb_Cal_TF_TC_dB_temporal_mean_summer.tif' % (outdir,orbit,year)
        io.write_xarray_to_GeoTiff(diff_mean_summer[0],diff_mean_file)
        vv_std_file = '%s/S1A__IW__%s_%i_VV_tnr_bnr_Orb_Cal_TF_TC_dB_temporal_stdev_summer.tif' % (outdir,orbit,year)
        io.write_xarray_to_GeoTiff(vv_std_summer[0],vv_std_file)
        vh_std_file = '%s/S1A__IW__%s_%i_VH_tnr_bnr_Orb_Cal_TF_TC_dB_temporal_stdev_summer.tif' % (outdir,orbit,year)
        io.write_xarray_to_GeoTiff(vh_std_summer[0],vh_std_file)
        diff_std_file = '%s/S1A__IW__%s_%i_diffVVVH_tnr_bnr_Orb_Cal_TF_TC_dB_temporal_stdev_summer.tif' % (outdir,orbit,year)
        io.write_xarray_to_GeoTiff(diff_std_summer[0],diff_std_file)

        vv_mean_file = '%s/S1A__IW__%s_%i_VV_tnr_bnr_Orb_Cal_TF_TC_dB_temporal_mean_winter.tif' % (outdir,orbit,year)
        io.write_xarray_to_GeoTiff(vv_mean_winter[0],vv_mean_file)
        vh_mean_file = '%s/S1A__IW__%s_%i_VH_tnr_bnr_Orb_Cal_TF_TC_dB_temporal_mean_winter.tif' % (outdir,orbit,year)
        io.write_xarray_to_GeoTiff(vh_mean_winter[0],vh_mean_file)
        diff_mean_file = '%s/S1A__IW__%s_%i_diffVVVH_tnr_bnr_Orb_Cal_TF_TC_dB_temporal_mean_winter.tif' % (outdir,orbit,year)
        io.write_xarray_to_GeoTiff(diff_mean_winter[0],diff_mean_file)
        vv_std_file = '%s/S1A__IW__%s_%i_VV_tnr_bnr_Orb_Cal_TF_TC_dB_temporal_stdev_winter.tif' % (outdir,orbit,year)
        io.write_xarray_to_GeoTiff(vv_std_winter[0],vv_std_file)
        vh_std_file = '%s/S1A__IW__%s_%i_VH_tnr_bnr_Orb_Cal_TF_TC_dB_temporal_stdev_winter.tif' % (outdir,orbit,year)
        io.write_xarray_to_GeoTiff(vh_std_winter[0],vh_std_file)
        diff_std_file = '%s/S1A__IW__%s_%i_diffVVVH_tnr_bnr_Orb_Cal_TF_TC_dB_temporal_stdev_winter.tif' % (outdir,orbit,year)
        io.write_xarray_to_GeoTiff(diff_std_winter[0],diff_std_file)
