import sys, os
sys.path.append('/home/pikakilla/PycharmProjects/Edgar')

from headerParse import runEdgarHeaderSearch
from edgar_parser import edgar_parser

if __name__ == '__main__':
    headerSearch = runEdgarHeaderSearch()
    parser = edgar_parser()
    root_dir = '/media/pikakilla/EDGAR/EDGAR/8-K'


    for root, dir, files in os.walk(root_dir, topdown=True):
        for file in files:
            with open(os.path.join(root_dir, root, file), errors='ignore') as f:
                # if root.split('/')[-2] in ['2018', '2019', '2013', '2012', '2010', '2008', '2006', '2005']:
                #     continue
                text = f.readlines(10000)
                header_dict = headerSearch.searchEdgarHeader(textSnippet=text)
                #if header_dict['ACCESSION NUMBER'] != '0001144204-18-058900':
                #    continue
                if header_dict['FILING YEAR'] in ['2018', '2019', '2013', '2012', '2010', '2008', '2006', '2005']:
                    continue
                if 'Results' in header_dict['ITEM INFORMATION']:
                    parser.output_location = os.path.join('/media/pikakilla/EDGAR/8K_Output/',header_dict['FILING YEAR'])
                    f.seek(0)
                    parser.new_document(f.read())
                    parser.save_documents('99', ('ex99', 'EX99', 'ex-99', 'EX-99'),
                                          filename=header_dict['ACCESSION NUMBER'], expanded_search = True)

