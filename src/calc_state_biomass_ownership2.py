import glob
import os
import rasterio
import numpy as np
from dask.distributed import Client
from dask import array as da
from dask import dataframe as dd

STATES = ['washington']

def mask_raster(in_raster, mask_raster):
    """Reads in a raster, changes masked values to NaN"""
    with rasterio.open(in_raster) as src:
        img = src.read()
        img_mask = src.read_masks()

    with rasterio.open(mask_raster) as src:
        canopy_mask = src.read_masks()

    full_mask = img_mask * canopy_mask

    img = img.astype('float32')
    img[full_mask == 0] = np.nan

    return img

def make_df(state, year):
    MASK_RASTER = '../data/interim/canopy_cover/{}_canopy_mask_2000-2015.tif'.format(state)

    OWN_RASTER = '../data/interim/washington_ownership_v2.tif'
    own = mask_raster(OWN_RASTER, MASK_RASTER)[0,:,:].ravel()

    CTY_RASTER = '../data/interim/{}_counties.tif'.format(state)
    cty = mask_raster(CTY_RASTER, MASK_RASTER)[0,:,:].ravel()

    BIO_RASTER = '../data/interim/unmasked_biomass/{}_biomass_{}.tif'.format(state, year)
    bio = mask_raster(BIO_RASTER, MASK_RASTER)[0,:,:].ravel()

    stacked = np.stack([own, cty, bio], axis=-1)
    COLUMNS = ['owner', 'county', 'biomass_{}'.format(year)]
    df = dd.from_array(stacked, columns=COLUMNS).dropna(how='all')

    return df

def calc_summary(df):
    AGG_FUNCS = ['count', 'sum', 'min', 'max', 'mean', 'std']
    res = df.groupby(by=['owner', 'county'])[df.columns[-1]].agg(AGG_FUNCS)

    return res

def format_summary(results):
    INT_COLS = results.columns[:-2]  # all except mean and std are integers
    results[INT_COLS] = results[INT_COLS].astype('Int64')  # some have nulls
    FLOAT_COLS = results.columns[-2:]  # round these floats
    results[FLOAT_COLS] = results[FLOAT_COLS].round(1)

    return results

if __name__ == "__main__":
    client = Client(memory_limit='48GB', processes=False)

    for state in STATES:
        GLOB_STR = '../data/interim/unmasked_biomass/{}_biomass_*.tif'.format(state)
        biomass_rasters = glob.glob(GLOB_STR)
        years = [x[-8:-4] for x in biomass_rasters]

        for year in years:
            OUT_CSV = '../data/interim/biomass_summaries_v2/{}_{}_summary.csv'.format(state,year)
            if os.path.exists(OUT_CSV):
                print('Skipping', state, year, OUT_CSV, 'already exists.')
                continue

            else:
                df = make_df(state, year)
                to_compute = calc_summary(df)
                future = client.compute(to_compute)
                results = future.result().reset_index()
                summary = format_summary(results)

                summary.to_csv(OUT_CSV, header=True, index=False)
                print('Finished', state, year)
