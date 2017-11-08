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

new_dupes = []
for i in range(len(clustered_dupes)):
    if type(clustered_dupes[i][1]) == numpy.ndarray:
        new_dupes.append((clustered_dupes[i][0], tuple(clustered_dupes[i][1])))
    else:
        new_dupes.append(clustered_dupes[i])

cluster_membership = collections.defaultdict(lambda : 'x')


for (cluster_id, cluster) in enumerate(new_dupes):
    for record_id in cluster:
        cluster_membership[record_id] = cluster_id

output_file = 'output.csv'
with open(output_file, 'w') as f:
    writer = csv.writer(f)

    with open(input_file) as f_input :
        reader = csv.reader(f_input)

        heading_row = next(reader)
        heading_row.insert(0, 'Cluster ID')
        writer.writerow(heading_row)

        for row in reader:
            row_id = int(row[0])
            cluster_id = cluster_membership[row_id]
            row.insert(0, cluster_id)
            writer.writerow(row)

