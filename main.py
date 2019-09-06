from Library.parser import ParseRtf, IdentifyFilename
import os
from tqdm import tqdm
from collections import defaultdict

identified_companies_csv = os.path.join('/media/abc-123/EDGAR/Zahn/Export', 'all.csv')
identify_filenames = IdentifyFilename(identified_companies_csv)

main_path = os.path.join('/media/abc-123/EDGAR/Zahn/Export', 'Articles')
output_path = os.path.join('/media/abc-123/EDGAR/Zahn/', 'Output_3')
os.makedirs(os.path.join('/media/abc-123/EDGAR/Zahn/', 'Output_3'), exist_ok=True)
files = os.listdir(main_path)
rtf_parser = ParseRtf(output_path)

files_read = defaultdict(int)


for file in tqdm(files):
    #print(file)
    filename = identify_filenames.construct_output_filename(file)
    files_read[file] += 1
    if filename is None:
        print('fn none')
    with open(os.path.join(main_path, file), 'r', errors='ignore') as f:
        text = []
        for line in f:
            if rtf_parser.remove_images(line):
                text.append(line)
        rtf_parser.parse_list(text, filename, file=file)

files_not_output_t = {}

for key in files_read.keys():
    files_not_output_t[key] = rtf_parser.files_output.get(key)

files_not_output = {}
for key in files_read.keys():
    if files_not_output_t.get(key, 0) == 0:
        files_not_output[key] = 0

with open(os.path.join('/media/abc-123/EDGAR/Zahn/files_output_3.txt'),'w') as f:
    f.write(str(rtf_parser.files_output))

with open(os.path.join('/media/abc-123/EDGAR/Zahn/files_not_output_3.txt'),'w') as f:
    f.write(str(files_not_output))