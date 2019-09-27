#this is a multiprocessing codebase for identifying specific sections of an 8-k. The multithreading works
#TODO: ADD COMMENTS ASAP

import sys, os
sys.path.append('/home/abc-123/PycharmProjects/Edgar')
import csv
import multiprocessing as mp
from headerParse import runEdgarHeaderSearch
from edgar_parser import edgar_parser

if __name__ == '__main__':
    header_dict = {}
    with open('/media/abc-123/EDGAR/ALL_8K_HEADER_INFO_2002_2019_RO.csv', 'r', errors = 'ignore') as f:
        reader = csv.DictReader(f)
        for line in reader:
            header_dict[line['ACCESSION NUMBER']] = line

    def write_files(filename):
        filename_path_components = filename.split('/')
        accession_number = filename_path_components[-1][:-4]
        parser = edgar_parser()
        try:
            if 'Results' in header_dict.get(accession_number).get('ITEM INFORMATION'):
                with open(filename, 'r', errors='ignore') as g:
                    parser.output_location = os.path.join('/home/abc-123/EDGAR/8K_Output_2/', header_dict[accession_number]['FILING YEAR'])
                    g.seek(0)
                    parser.new_document(g.read())
                    parser.save_documents('99', ('ex99', 'EX99', 'ex-99', 'EX-99', '99_1', '991',),
                                          filename=header_dict[accession_number]['ACCESSION NUMBER'], expanded_search=True)
        except AttributeError:
            return


    file_locations = []
    pool = mp.Pool(mp.cpu_count() - 1)
    headerSearch = runEdgarHeaderSearch()

    root_dir = '/media/abc-123/EDGAR/Forms/8-K'


    file_locations = []
    for root, dir, files in os.walk(root_dir, topdown=True):
        for file in files:
            file_locations.append(os.path.join(root_dir, root, file))

    pool.map(write_files, file_locations)
            # with open(os.path.join(root_dir, root, file), errors='ignore') as f:
            #     # if root.split('/')[-2] in ['2018', '2019', '2013', '2012', '2010', '2008', '2006', '2005']:
            #     #     continue
            #
            #     #if header_dict['ACCESSION NUMBER'] != '0001144204-18-058900':
            #     #    continue
            #     # if header_dict['FILING YEAR'] in ['2018', '2019', '2013', '2012', '2010', '2008', '2006', '2005']:
            #     #     continue
            #     if 'Results' in header_dict['ITEM INFORMATION']:
            #         parser.output_location = os.path.join('/home/abc-123/EDGAR/8K_Output/',header_dict['FILING YEAR'])
            #         f.seek(0)
            #         parser.new_document(f.read())
            #         parser.save_documents('99', ('ex99', 'EX99', 'ex-99', 'EX-99'),
            #                               filename=header_dict['ACCESSION NUMBER'], expanded_search = True)

