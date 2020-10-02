import os
import re
import glob
import numpy as np
import pandas as pd


excel_files = glob.glob('../data/external/reports_2003-2017/*xl*')
fnames = [os.path.basename(f) for f in excel_files]
years = [int(re.findall('\d{4}', fname)[0]) for fname in fnames]

OWNERS = {'Private - Industrial': 'industry',
          'Private - Large': 'large_private',
          'Private - Small': 'small_private',
          'Private - Unknown': 'unknown_private',
          'State': 'state',
          'Other Public': 'other_public',
          'Federal': 'federal'}

# capitalization is not consistent in every spreadsheet, so we'll need
# to identify each sheet_name that matches any of the counties listed above
COUNTIES = ['ASOTIN', 'CHELAN', 'CLALLAM', 'CLARK', 'COLUMBIA', 'COWLITZ',
            'FERRY', 'GARFIELD', 'GRAYS HARBOR', 'ISLAND', 'JEFFERSON', 'KING',
            'KITSAP', 'KITTITAS', 'KLICKITAT', 'LEWIS', 'LINCOLN', 'MASON',
            'OKANOGAN', 'PACIFIC', 'PEND OREILLE', 'PIERCE', 'SAN JUAN',
            'SKAGIT', 'SKAMANIA', 'SNOHOMISH', 'SPOKANE', 'STEVENS',
            'THURSTON', 'WAHKIAKUM', 'WHATCOM', 'YAKIMA']

COUNTIES2 = [x+'2' for x in COUNTIES]
COUNTIES_PLUS = [x+' COUNTY' for x in COUNTIES]

year_dfs = []
for idx, f in enumerate(excel_files):
    sheets = pd.ExcelFile(f).sheet_names
    good_sheets = [s for s in sheets if s.upper() in COUNTIES]
    if len(good_sheets) == 0:
        good_sheets = [s for s in sheets if s.upper() in COUNTIES2]
    if len(good_sheets) == 0:
        good_sheets = [s for s in sheets if s.upper() in COUNTIES_PLUS]
    county_dfs = []

    for sheet_name in good_sheets:
        df = pd.read_excel(f, index_col=0, sheet_name=sheet_name,
                           header=None, skiprows=range(0,10))
        # the rows with individual owner types are preceded by two spaces
        use = df.index.fillna('').str.lstrip().str.rstrip().isin(OWNERS.keys())
        data = df.loc[use].T.iloc[-1]
        # strip whitespace from the names
        data.index = data.index.str.strip()
        data['year'] = years[idx]

        if sheet_name.upper() in COUNTIES2:
            county_name = sheet_name[:-1]
        elif sheet_name.upper() in COUNTIES_PLUS:
            county_name = sheet_name.split(' County')[0]
        else:
            county_name = sheet_name
        data['county'] = county_name.upper()

        county_dfs.append(data)
    try:
        year_dfs.append(pd.concat(county_dfs, axis=1,
                                  ignore_index=True, sort=True).T)
    except:
        print('Failed on', f)
        pass

consolidated = pd.concat(year_dfs, axis=0, ignore_index=True, sort=True)
consolidated = consolidated.sort_values(by=['year', 'county'])
COL_ORDER = ['year', 'county'] + list(OWNERS.keys())
consolidated = consolidated[COL_ORDER].rename(OWNERS, axis=1)

consolidated.to_csv('../data/raw/washington_timber_harvest_2003-2017.csv',
                    index=False)
