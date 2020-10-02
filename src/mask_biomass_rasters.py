"""Masks biomass rasters for Oregon and Washington to forest cover only"""
import subprocess
import rasterio
import glob
import os
from multiprocessing import Pool
from functools import partial

def mask(mask_raster, in_raster):
    basename = os.path.basename(in_raster)
    out_raster = '../data/interim/masked_biomass/{}'.format(basename)

    with rasterio.open(mask_raster) as msk_src:
        msk = msk_src.read(masked=True)

    with rasterio.open(in_raster) as img_src:
        img = img_src.read(masked=True)
        meta = img_src.meta

    img[msk.mask] = meta['nodata']
    img.mask = np.logical_or(img.mask, msk.mask)

    with rasterio.open(out_raster, 'w', **meta,
                       compress="LZW", tiled=True,
                       blockxsize=256, blockysize=256) as dst:
        dst.write(img)
        dst.write_mask(~img.mask)

OR_MASK = '../data/interim/canopy_cover/oregon_canopy_mask_2000-2015.tif'
mask_or = partial(mask, OR_MASK)

WA_MASK = '../data/interim/canopy_cover/washington_canopy_mask_2000-2015.tif'
mask_wa = partial(mask, WA_MASK)

if __name__ == "__main__":
    or_unmasked = glob.glob('../data/interim/unmasked_biomass/oregon*.tif')
    wa_unmasked = glob.glob('../data/interim/unmasked_biomass/washington*.tif')

    with Pool(4) as p:
        p.map(mask_or, or_unmasked)
        p.map(mask_wa, wa_unmasked)
    print('Done.')
