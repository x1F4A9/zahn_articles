import sys
import os
import csv
sys.path.append('/home/abc-123/PycharmProjects/Edgar')
from headerParse import runEdgarHeaderSearch
import multiprocessing as mp

root_dir = '/media/abc-123/EDGAR/Forms/8-K'


csvFile = os.path.join('/media/abc-123/EDGAR/ALL_8K_HEADER_INFO_2002_2019_2.csv')

headerSearch = runEdgarHeaderSearch(sort=True, default=True)

with open(csvFile, 'w', newline='', errors="ignore", encoding='UTF-8') as csv_o:
    csvOutput = csv.writer(csv_o, quotechar='"', quoting=csv.QUOTE_MINIMAL)
    outputFiling = headerSearch.searchEdgarHeader(textSnippet=' ', columnHeaderLabelsOnly=True)
    csvOutput.writerow(outputFiling)

results = []

def worker(arg, q):
    with open(arg, errors='ignore') as f:
        file = f.readlines(25000)
        res = headerSearch.searchEdgarHeader(textSnippet=file)
        q.put(res)
        return res


def listener(q):
    ''' listens for messages on the q'''
    with open(csvFile, 'a', newline='', errors="ignore", encoding='UTF-8') as csv_l:
        while 1:
            csvOutput = csv.writer(csv_l, quotechar='"', quoting=csv.QUOTE_MINIMAL)
            header_data = q.get()
            if header_data == 'kill':
                break
            csvOutput.writerow(header_data)
            csv_l.flush()

def main():
    manager = mp.Manager()
    q = manager.Queue()
    pool = mp.Pool(mp.cpu_count() - 1)

    watcher = pool.apply_async(listener, (q,))

    filings_location = []
    for root, dir, files in os.walk(root_dir, topdown=True):
        for file in files:
            filings_location.append(os.path.join(root_dir, root, file))

    jobs = []
    for filing in filings_location:
        job = pool.apply_async(worker, (filing, q))
        jobs.append(job)

    for job in jobs:
        job.get()

    q.put('kill')
    pool.close()
    pool.join()






    # with open(csvFile, 'w', newline='', errors="ignore", encoding='UTF-8') as csvFile:
    #     csvOutput = csv.writer(csvFile, quotechar='"', quoting=csv.QUOTE_MINIMAL)
    #     for root, dir, files in os.walk(root_dir, topdown=True):
    #         for file in files:
    #             with open(os.path.join(root_dir, root, file), errors='ignore') as f:
    #                 file = f.readlines(25000)
    #                 if addHeaders:
    #                     outputFiling = headerSearch.searchEdgarHeader(textSnippet=file, columnHeaderLabelsOnly=True)
    #                     addHeaders = False
    #                     csvOutput.writerow(outputFiling)
    #                     outputFiling = []
    #                 filingList = headerSearch.searchEdgarHeader(textSnippet=file)
    #                 csvOutput.writerow(filingList)
    #                 outputFiling = []


#todo -- make this parallel processing
if __name__ == '__main__':
    main()
