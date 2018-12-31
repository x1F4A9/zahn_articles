from Library.parser import ParseRtf, IdentifyFilename
import os
from tqdm import tqdm

identified_companies_csv = os.path.join('E:\\', 'Zahn', 'WORK_FILTER_FOR_ALL_SAS7BDAT.csv')
identify_filenames = IdentifyFilename(identified_companies_csv)

main_path = os.path.join('E:\\', 'Zahn', 'Articles_Sample')
output_path = os.path.join('E:\\', 'Zahn', 'Output_Sample')
files = os.listdir(main_path)
rtf_parser = ParseRtf(output_path)


for file in tqdm(files):
    filename = identify_filenames.construct_output_filename(file)
    with open(os.path.join(main_path, file), 'r', errors='ignore') as f:
        for line in f:
            if rtf_parser.identify_rtf_article(line):
                rtf_parser.parse(line, filename)
