import sys
import numpy as np
import xarray as xr
import seaborn as sns

from matplotlib import pyplot as plt

%run ./preprocessing/dsen2/utils/DSen2Net.py

sys.path.append('../woodland_fragments/src/data_io/')
import data_io as io

site = 'Arisaig'
tile_id = 'T30VUJ'
data_folder = '../DATA/Sentinel2/awsdata/%s/L2A_T30VUJ_A012304_20190715T114358/IMG_DATA/' % tile_id

# For DSen2 superresolve
MDL_PATH = "./preprocessing/dsen2/models/"
INPUT_SHAPE = ((4, None, None), (6, None, None))
MODEL = s2model(INPUT_SHAPE, num_layers=6, feature_size=128)
PREDICT_FILE = MDL_PATH+'s2_032_lr_1e-04.hdf5'
MODEL.load_weights(PREDICT_FILE)

def DSen2(d10, d20):
    """Super resolves 20 meter bans using the DSen2 convolutional
       neural network, as specified in Lanaras et al. 2018
       https://github.com/lanha/DSen2

        Parameters:
         d10 (arr): (4, X, Y) shape array with 10 meter resolution
         d20 (arr): (6, X, Y) shape array with 20 meter resolution

        Returns:
         prediction (arr): (6, X, Y) shape array with 10 meter superresolved
                          output of DSen2 on d20 array
    """
    test = [d10, d20]
    input_shape = ((4, None, None), (6, None, None))
    prediction = _predict(test, input_shape, deep=False)
    return prediction

def _predict(test, input_shape, model = MODEL, deep=False, run_60=False):

    prediction = model.predict(test, verbose=1)
    return prediction

# Bounds to clip bbox
W= 324700
E = 334700
N=6317000
S=6307000

B02_10m = xr.open_rasterio('%s/R10m/T30VUJ_20190715T114359_B02_10m.jp2' % data_folder)[0].sel(x=slice(W,E),y=slice(N,S))
rows_10m,cols_10m = B02_10m.values.shape
B03_10m = xr.open_rasterio('%s/R10m/T30VUJ_20190715T114359_B03_10m.jp2' % data_folder)[0].sel(x=slice(W,E),y=slice(N,S))
B04_10m = xr.open_rasterio('%s/R10m/T30VUJ_20190715T114359_B04_10m.jp2' % data_folder)[0].sel(x=slice(W,E),y=slice(N,S))
B08_10m = xr.open_rasterio('%s/R10m/T30VUJ_20190715T114359_B08_10m.jp2' % data_folder)[0].sel(x=slice(W,E),y=slice(N,S))

B11_20m = xr.open_rasterio('%s/R20m/T30VUJ_20190715T114359_B11_20m.jp2' % data_folder)[0].sel(x=slice(W,E),y=slice(N,S))
rows_20m,cols_20m = B11_20m.values.shape
B05_20m = xr.open_rasterio('%s/R20m/T30VUJ_20190715T114359_B05_20m.jp2' % data_folder)[0].sel(x=slice(W,E),y=slice(N,S))
B06_20m = xr.open_rasterio('%s/R20m/T30VUJ_20190715T114359_B06_20m.jp2' % data_folder)[0].sel(x=slice(W,E),y=slice(N,S))
B07_20m = xr.open_rasterio('%s/R20m/T30VUJ_20190715T114359_B07_20m.jp2' % data_folder)[0].sel(x=slice(W,E),y=slice(N,S))
B8A_20m = xr.open_rasterio('%s/R20m/T30VUJ_20190715T114359_B8A_20m.jp2' % data_folder)[0].sel(x=slice(W,E),y=slice(N,S))
B12_20m = xr.open_rasterio('%s/R20m/T30VUJ_20190715T114359_B12_20m.jp2' % data_folder)[0].sel(x=slice(W,E),y=slice(N,S))

d10_bands = np.asarray([[B02_10m,B03_10m,B04_10m,B08_10m]]).astype('float')/10000.
d20_bands = np.asarray([[B05_20m,B06_20m,B07_20m,B8A_20m,B11_20m,B12_20m]]).astype('float')/10000.

from skimage.transform import resize
d10_bands = resize(d10_bands,d10_bands.shape,order=0)
d20_bands = resize(d20_bands,(d10_bands.shape[0],d20_bands.shape[1],d10_bands.shape[2],d10_bands.shape[3]),order=0)
d20_bands_supres = DSen2(d10_bands,d20_bands)

# Combine the 10 m resolution layers
d10_bands = np.concatenate((d10_bands,d20_bands_supres),axis=1)
d10_bands = np.swapaxes(d10_bands,1,-1)
d10_bands = np.swapaxes(d10_bands,1,2)

# calculate the extra layers
%run /home/dmilodow/DataStore_DTM/STFC/wri_restoration_mapper/restoration-mapper/src/preprocessing/indices.py
d10_bands = evi(d10_bands, True)
d10_bands = bi(d10_bands, True)
d10_bands = msavi2(d10_bands, True)
d10_bands = si(d10_bands, True)

# load cloud mask
# !fmask_sentinel2Stacked.py -o cloud.img --safedir ../DATA/Sentinel2/awsdata/T30VUJ/L1C/S2B_MSIL1C_20190715T114359_N0208_R123_T30VUJ_20190715T150908.SAFE/
clouds = xr.open_rasterio('cloud.img')[0].sel(x=slice(W,E),y=slice(N,S)).values
cloud_and_shadow = clouds==2
cloud_and_shadow[clouds==3]=True
cloud_and_shadow[clouds==0]=True
cloud_and_shadow = resize(cloud_and_shadow,d10_bands[0,:,:,0].shape).astype('bool')
for tstep in range(0,d10_bands.shape[0]):
    for band in range(0,d10_bands.shape[-1]):
        d10_bands[tstep,:,:,band][cloud_and_shadow] = np.nan

# write layers to file for ingestion into next phase of the analysis.
coords = {'y':B04_10m.coords['y'].values,'x':B04_10m.coords['x'].values,'band':np.arange(d10_bands.shape[-1])+1}
outarray=xr.DataArray(d10_bands[0],coords=coords,dims=('y','x','band'))
outfile = '../DATA/Sentinel2/processed_bands_and_derivatives/%s_sentinel2_bands_10m' % site
io.write_xarray_to_GeoTiff(outarray,outfile,EPSG_CODE = '32630')
