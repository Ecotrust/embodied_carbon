"""Generates clipped and masked rasters for Oregon and Washington"""
import subprocess
import glob
import os
from multiprocessing import Pool
import geopandas as gpd

##  BASIC SETTINGS FOR OUR RASTER PROCESSING

TR = 30  # TARGET RESOLUTION

STATES = ['oregon', 'washington']

US_STATES_SHP = '../data/external/census/cb_2018_us_state_500k.shp'
COUNTIES_SHP = '../data/external/census/cb_2018_us_county_500k.shp'

BOUNDS = {  # BOUNDING BOXES FOR OREGON AND WASHINGTON RASTERS
    'oregon': [-2294730, 2301450, -1584300, 2906130],
    'washington': [-2137770, 2734170, -1545540, 3172590]
    }

def rasterize(shapefile, out_raster, bounds, tr=TR):
    CMD = ['gdal_rasterize',
           '-te', *[str(x) for x in bounds],
           '-tr', str(TR), str(TR),
           '-tap',
           '-co', 'TILED=TRUE',
           '-co', 'COMPRESS=LZW',
           '-co', 'blockxsize=256',
           '-co', 'blockysize=256',
           '-a_srs', 'EPSG:5070',
           '-a_nodata', '0',
           '-a', 'GEOID',
           '-ot', 'UInt32',
           shapefile,
           out_raster]

    proc = subprocess.run(CMD,
                          stdout=subprocess.PIPE,
                          stderr=subprocess.PIPE)
    return proc


def clip_counties(state):
    states = gpd.read_file(US_STATES_SHP)
    state_id = states.loc[states.NAME == state.title(), 'STATEFP'].values[0]

    counties = gpd.read_file(COUNTIES_SHP)
    my_counties = counties.loc[counties.STATEFP == state_id]
    OUT_SHP = '../data/interim/{}_counties.shp'.format(state)
    my_counties.to_crs(epsg=5070).to_file(OUT_SHP)

    out_raster = '../data/interim/{}_counties.tif'.format(state)
    bbox = BOUNDS[state]

    proc = rasterize(OUT_SHP, out_raster, bbox)
    print(state.upper(), 'done.')

if __name__ == "__main__":
    with Pool(4) as p:
        p.map(clip_counties, STATES)
