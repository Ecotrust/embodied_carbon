"""Generates clipped and masked rasters for Oregon and Washington"""
import subprocess
import glob
import os
from functools import partial
from multiprocessing import Pool

##  BASIC SETTINGS FOR OUR RASTER PROCESSING

TR = 30  # TARGET RESOLUTION

BOUNDS = {  # BOUNDING BOXES FOR OREGON AND WASHINGTON RASTERS
    'oregon': [-2294730, 2301450, -1584300, 2906130],
    'washington': [-2137770, 2734170, -1545540, 3172590]
    }

SHPS = {'washington': '../data/raw/washington_state_boundary.shp',
        'oregon': '../data/raw/oregon_state_boundary.shp'}

def cut_and_mask(in_raster, out_raster, shapefile, bounds, tr=TR):
    CMD = ['gdalwarp',
           '-cutline', shapefile,
           '-crop_to_cutline',
           '-te', *[str(x) for x in bounds],
           '-tr', str(TR), str(TR),
           '-tap',
           '-co', 'TILED=TRUE',
           '-co', 'COMPRESS=LZW',
           '-co', 'blockxsize=256',
           '-co', 'blockysize=256',
           in_raster,
           out_raster]

    proc = subprocess.run(CMD,
                          stdout=subprocess.PIPE,
                          stderr=subprocess.PIPE)
    return proc

biomass_rasters = glob.glob('../data/external/landtrendr/biomass*.tif')

def clip_biomass(state, in_raster):
    basename = os.path.basename(in_raster)
    year = basename.split('_')[1]
    out_raster = '../data/interim/{}_biomass_{}.tif'.format(state, year)

    if os.path.exists(out_raster):
        print(state.upper(), year, 'previously done. Skipping.')
        return

    else:
        bbox = BOUNDS[state]
        shp = SHPS[state]

        proc = cut_and_mask(in_raster, out_raster, shp, bbox)
        print(state.upper(), year, 'done.')

clip_oregon = partial(clip_biomass, 'oregon')
clip_washington = partial(clip_biomass, 'washington')

if __name__ == "__main__":
    with Pool(12) as p:
        p.map(clip_oregon, biomass_rasters)
        p.map(clip_washington, biomass_rasters)
