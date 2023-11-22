import re
import json
from collections import defaultdict

import pandas as pd

with open('data/stanice.js', encoding='utf8') as f:
    lines = f.readlines()

# keep only lines which define geometry
data = [json.loads(re.sub('.* = ', '', l))['features'] for l in lines
            if re.match('var stanice', l)]
# extract geometry data
data = [[f['properties'] for f in features] for features in data]

# all into one list
data_merged = []
for d in data:
    data_merged += d

stations_data = pd.DataFrame.from_records(data_merged)
stations_data.drop('Column1', axis=1, inplace=True)
# columns in prevod_stanic.xlsx:
#   js: name in stanice.js
#   precip_known: names in precipitation data matching stations from stanice.js
#   precip_unknown: names from precipitation data which are not present in stanice.js
#   final: mostly equals 'precip_unknown' with small modifications to several station names
#   non_unique: stations whose names are not unique (2 occurrences with distant coordinates)
#   close_but_two: stations which appear twice with only slightly different coordinates
trans = pd.read_excel('data/prevod_stanic.xlsx', sheet_name='from_js')
# keep only the final selection
final = trans.loc[trans['final'].notna(), ['js', 'precip_known', 'final']]


# join the two
both = pd.merge(final, stations_data, left_on='js', right_on='FNAME', how='left')
# keep only non-duplicated stations
both = both.loc[~both['final'].duplicated()]
both.to_csv('data/stations_data.csv', index=False)

stat = defaultdict(lambda: [])
for d in data:
    for one in d:
        stat[one['FNAME']] += [one]

nam = list(stat.keys())
vals = list(stat.values())

l = [len(x) for x in vals]

diff = [round(float(vals[i][0]['ELEVATION']) - float(vals[i][1]['ELEVATION']), 1) for i in range(len(vals)) if len(vals[i])==2]
i_diff = list(zip(range(len(diff)), diff))
[x for x in i_diff if abs(x[1] > 3)]
print(1)



