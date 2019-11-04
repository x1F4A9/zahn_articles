import nltk
import os
import csv
import re
from tqdm import tqdm

article_directory = os.path.join('/media/abc-123/EDGAR/Zahn/Output_2')
output_location = os.path.join('/media/abc-123/EDGAR/article_authors_3.csv')
files = os.listdir(article_directory)

def append_file(filename, output):
    with open(os.path.join(filename), 'a', errors='ignore') as csvOut:
        writer = csv.writer(csvOut)
        writer.writerow(output)


for file in tqdm(files):
    with open(os.path.join(article_directory, file)) as f:
        head = [next(f) for x in range(6)]
        for line in head:
            if re.search('By ', line):
                line = line.split()
                result =[]
                result.append(file)
                for word in line:
                    if 'Byhgfhfhf' in word:
                        continue
                    else:
                        result.append(word)
                append_file(output_location, result)

