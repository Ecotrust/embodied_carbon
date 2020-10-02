"""Calculates annual differences between rasters in a directory. Rasters
are assumed to follow a naming convention of :
    <statename>_biomass_<yyyy>.tif    -->    oregon_biomass_1986.tif

Input files are assumed to live in ../data/interim/unmasked_biomass/
Output files will be written to ../data/interim/unmasked_biomass_differences/

Output file naming convention is :
    <statename>_diff_<yyyy>-<yyyy>.tif    -->    oregon_diff_1986-1987.tif
"""
import os
import glob
import subprocess
from multiprocessing import Pool

IN_DIR =  '../data/interim/unmasked_biomass'
OUT_DIR = '../data/interim/unmasked_biomass_differences'
STATES = ['oregon', 'washington']

def parse_raster_name(in_raster):
    basename = os.path.basename(in_raster)
    statename = basename.split('_')[0]
    year = basename[-8:-4]

    return basename, statename, year

def calc_diff(in_rasters):
    """Subracts raster1 from raster2"""
    in_raster1, in_raster2 = in_rasters
    base1, state1, year1 = parse_raster_name(in_raster1)
    base2, state2, year2 = parse_raster_name(in_raster2)
    assert state1 == state2

    diff_year = '{}-{}'.format(year1,year2)
    out_basename = '_'.join([state1,'diff', diff_year]) + '.tif'
    out_raster = os.path.join(OUT_DIR,
                              out_basename)

    CMD = ' '.join(['gdal_calc.py',
           '--calc="B-A"',
           '-A ' + in_raster1,
           '-B ' + in_raster2,
           '--co', 'TILED=TRUE',
           '--co', 'COMPRESS=LZW',
           '--co', 'blockxsize=256',
           '--co', 'blockysize=256',
           '--type', 'Int16',
           '--outfile={}'.format(out_raster)])

    proc = subprocess.run(CMD,
                          stderr=subprocess.PIPE,
                          stdout=subprocess.PIPE,
                          shell=True)
    print('Processed', out_basename)
    return proc


if __name__ == "__main__":
    with Pool(16) as p:
        for statename in STATES:
            rasters = glob.glob(os.path.join(IN_DIR,
                                             '{}_*.tif'.format(statename)))
            to_run = [(rasters[i], rasters[i+1])
                      for i in range(len(rasters[:-1]))]
            p.map(calc_diff, to_run)
