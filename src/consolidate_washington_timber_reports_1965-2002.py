import numpy as np
import pandas as pd

EXCEL_SRC = '../data/external/obe_econ_rprts_timbharv_65_2002.xls'
NAMES = ['county', 'tribal', 'industry', 'large_private', 'small_private', 'state', 'other_non_federal', 'national_forest', 'other_federal']
COLS = 'A:E,G:J'

# we'll gather each year's timber outputs as a dataframe
# and store them in a list of dataframes
harvest_dfs = []

# get read the timber output data from each sheet in the spreadsheet
for yr in range(1965,2003):
    east_df = pd.read_excel(EXCEL_SRC, sheet_name=str(yr), usecols=COLS, header=None, names=NAMES, skiprows=range(0,12), nrows=17)
    west_df = pd.read_excel(EXCEL_SRC, sheet_name=str(yr), usecols=COLS, header=None, names=NAMES, skiprows=range(0,35), nrows=19)
    harvest_df = pd.concat([east_df, west_df], axis=0, ignore_index=True)
    harvest_df['year'] = yr
    harvest_dfs.append(harvest_df)

# consolidate all the annual harvest reports into a single dataframe
harvests = pd.concat(harvest_dfs, axis=0, ignore_index=True)

# in some years, national forest timber production isn't reported, these cells contain a string
# let's just replace those strings with nodata
harvests.loc[harvests.national_forest.apply(lambda x: type(x) != int), 'national_forest'] = np.nan
# to maintain integer format with nulls, need to convert datatype to Int64
harvests['national_forest'] = harvests['national_forest'].astype('Int64')

# write out the consolidated data
harvests.to_csv('../data/raw/washington_timber_harvest_1965-2002.csv', index=False)
