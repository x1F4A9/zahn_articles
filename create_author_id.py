import nltk
import os
import csv
import re
from tqdm import tqdm
import hashlib

input_location = os.path.join('/media/abc-123/EDGAR/article_authors_raw.csv')
output_location = os.path.join('/media/abc-123/EDGAR/article_authors_integer_id.csv')

output = []
with open(input_location, 'r', errors = 'ignore') as f:
    reader = csv.reader(f)
    t_output = []
    for row in reader:
        id = ''.join([i for i in row[1:]]).encode('utf-8')
        id = hashlib.md5(id).hexdigest()
        t_output=[row[0], id]
        with open(output_location, 'a', errors='ignore') as g:
            writer = csv.writer(g)
            writer.writerow(t_output)

