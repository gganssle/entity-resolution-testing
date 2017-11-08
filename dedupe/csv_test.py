from future.builtins import next

import os
import csv
import re
import logging
import optparse
import collections
import numpy
import dedupe
from unidecode import unidecode

def preProcess(column):
    try : # python 2/3 string differences
        column = column.decode('utf8')
    except AttributeError:
        pass
    column = unidecode(column)
    column = re.sub('  +', ' ', column)
    column = re.sub('\n', ' ', column)
    column = column.strip().strip('"').strip("'").lower().strip()
    if not column:
        column = None
    return column

input_file = '../dedupe-examples/dedupe-examples-0.5/csv_example/csv_example_messy_input.csv'

data_d = {}
with open(input_file) as f:
    reader = csv.DictReader(f)
    for row in reader:
        clean_row = [(k, preProcess(v)) for (k, v) in row.items()]
        row_id = int(row['Id'])
        data_d[row_id] = dict(clean_row)

fields = [
{'field' : 'Site name', 'type': 'String'},
{'field' : 'Address', 'type': 'String'},
{'field' : 'Zip', 'type': 'Exact', 'has missing' : True},
{'field' : 'Phone', 'type': 'String', 'has missing' : True}
]

deduper = dedupe.Dedupe(fields)
deduper.sample(data_d, 1500)

dedupe.consoleLabel(deduper)

deduper.train()

print('blocking...')

threshold = deduper.threshold(data_d, recall_weight=2)

print('clustering...')
clustered_dupes = deduper.match(data_d, threshold)

print('# duplicate sets', len(clustered_dupes))

new_dupes = []
for i in range(len(clustered_dupes)):
    if type(clustered_dupes[i][1]) == numpy.ndarray:
        new_dupes.append((clustered_dupes[i][0], tuple(clustered_dupes[i][1])))
    else:
        new_dupes.append(clustered_dupes[i])
        
outkeys = list(data_d[0].keys())
outkeys.insert(0, 'Cluster ID')

with open('output.csv', 'w') as f:
    writer = csv.writer(f)
    writer.writerow(outkeys)
    for (j, val) in enumerate(new_dupes):
        idx = 0
        for i in new_dupes[j][0]:
            row = list(data_d[new_dupes[j][0][idx]].values()) # data values
            row.insert(0, j) # insert cluster number
            writer.writerow(row)
            idx += 1
