from Library.parser import ParseRtf, IdentifyFilename
import os
from tqdm import tqdm
from collections import defaultdict

identified_companies_csv = os.path.join('PATH_TO_CSV_FILE_FROM_SAS7BDAT_HERE')
identify_filenames = IdentifyFilename(identified_companies_csv)


main_path = os.path.join('PATH_TO_RTF_FILES')
#output folder is created automatically if it does not exist, just place a folder name here
output_path = os.path.join('PATH_TO_OUTPUT')
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


#DIAGNOSTIC ONLY -- WILL REMOVE
# files_not_output_t = {}
#
# for key in files_read.keys():
#     files_not_output_t[key] = rtf_parser.files_output.get(key)
#
# files_not_output = {}
# for key in files_read.keys():
#     if files_not_output_t.get(key, 0) == 0:
#         files_not_output[key] = 0
#
# with open(os.path.join('DIAGNOSTIC_ONLY--WILL_REMOVE'),'w') as f:
#     f.write(str(rtf_parser.files_output))
#
# with open(os.path.join('DIAGNOSTIC_ONLY--WILL_REMOVE),'w') as f:
#     f.write(str(files_not_output))