#TODO: Add more comments
import sys
import os
import csv
sys.path.append('/home/abc-123/PycharmProjects/Edgar')
from headerParse import runEdgarHeaderSearch
import multiprocessing as mp

root_dir = '/media/abc-123/EDGAR/Forms/8-K'


csvFile = os.path.join('/home/abc-123/ALL_8K_HEADER_INFO_2002_2019_2.csv')

headerSearch = runEdgarHeaderSearch(sort=True, default=True)

with open(csvFile, 'w', newline='', errors="ignore", encoding='UTF-8') as csv_o:
    csvOutput = csv.writer(csv_o, quotechar='"', quoting=csv.QUOTE_MINIMAL)
    outputFiling = headerSearch.searchEdgarHeader(textSnippet=' ', columnHeaderLabelsOnly=True)
    csvOutput.writerow(outputFiling)

results = []

def worker(arg, q):
    '''main process'''
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
    #must use Manager queue here, or will not work
    #manager locks the file and creates a queue
    manager = mp.Manager()
    q = manager.Queue()
    pool = mp.Pool(mp.cpu_count() - 1)

    #put listener to work first
    #we do not want to have data before the listener is ready
    watcher = pool.apply_async(listener, (q,))


    #get the list of all the filings to open
    filings_location = []
    for root, dir, files in os.walk(root_dir, topdown=True):
        for file in files:
            filings_location.append(os.path.join(root_dir, root, file))

    #create a list of current jobs
    #fire off all the workers
    jobs = []
    for filing in filings_location:
        job = pool.apply_async(worker, (filing, q))
        jobs.append(job)

    #collec results from the workers through the pool result queue
    for job in jobs:
        job.get()

    #we are done. kill the listener
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



if __name__ == '__main__':
    main()
