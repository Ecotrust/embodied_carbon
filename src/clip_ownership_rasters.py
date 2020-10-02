"""Generates clipped and masked rasters for Oregon and Washington"""
import subprocess
import glob
import os
from functools import partial
from multiprocessing import Pool

##  BASIC SETTINGS FOR OUR RASTER PROCESSING

TR = 30  # TARGET RESOLUTION

STATES = ['oregon', 'washington']

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
           '-r', 'near',
           '-co', 'TILED=TRUE',
           '-co', 'COMPRESS=LZW',
           '-co', 'blockxsize=256',
           '-co', 'blockysize=256',
           '-t_srs', 'EPSG:5070',
           in_raster,
           out_raster]

    proc = subprocess.run(CMD,
                          stdout=subprocess.PIPE,
                          stderr=subprocess.PIPE)
    return proc

ownership_raster = '../data/external/usfs/Data/forown2016'

def clip_owners(state):
    in_raster = ownership_raster
    out_raster = '../data/interim/{}_ownership.tif'.format(state)
    bbox = BOUNDS[state]
    shp = SHPS[state]

    proc = cut_and_mask(in_raster, out_raster, shp, bbox)
    print(state.upper(), 'done.')

if __name__ == "__main__":
    with Pool(4) as p:
        p.map(clip_owners, STATES)
