import sys, os
import csv
sys.path.append('/home/pikakilla/PycharmProjects/Edgar')

from headerParse import runEdgarHeaderSearch
addHeaders = True
csvFile = os.path.join('/media/pikakilla/EDGAR/ALL_8K_HEADER_INFO_2004_2019.csv')


if __name__ == '__main__':
    headerSearch = runEdgarHeaderSearch(sort=True,default=True)
    root_dir = '/media/pikakilla/EDGAR/EDGAR/8-K'
    with open(csvFile, 'w', newline='', errors="ignore") as csvFile:
        csvOutput = csv.writer(csvFile, quotechar='"', quoting=csv.QUOTE_MINIMAL)
        for root, dir, files in os.walk(root_dir, topdown=True):
            for file in files:
                with open(os.path.join(root_dir, root, file), errors='ignore') as f:
                    file = f.readlines(25000)
                    if addHeaders:
                        outputFiling = headerSearch.searchEdgarHeader(textSnippet=file, columnHeaderLabelsOnly=True)
                        addHeaders = False
                        csvOutput.writerow(outputFiling)
                        outputFiling = []
                    filingList = headerSearch.searchEdgarHeader(textSnippet=file)
                    csvOutput.writerow(filingList)
                    outputFiling = []