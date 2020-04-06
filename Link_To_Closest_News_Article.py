#TODO: Remove unused code
#TODO: Declutter the birdsnests.

import csv
import math
import os
import nltk
#nltk.download('all')
nltk.download('cmudict')
nltk.download('punkt')
from concurrent import futures
from collections import OrderedDict
from tqdm import tqdm
import multiprocessing as mp
import logging
import pickle
from datetime import datetime
from bs4 import BeautifulSoup

#logging for debugging purposes
# mpl = mp.log_to_stderr()
# mpl.setLevel(logging.DEBUG)

#pickle variables -- saves processing
use_pickle = True
use_pickle_article = True

#local imports
import data_labels
import config
#from x import * is normally a bad practice -- but this file contains only functions so namespaces are not a concern!
from article_tools import *
##############################
from linguistic_tools import branching, parse_sentences

class ViviDict(dict):
    def __missing__(self, key):
        value = self[key] = type(self)()
        return value

#construct all valid 8-k's
all_8k_headers = {}
with open('/media/abc-123/EDGAR/ALL_8K_HEADER_INFO_2002_2019_RO.csv', 'r', errors='ignore') as all_8k_header_file:
    reader = csv.DictReader(all_8k_header_file)
    for line in reader:
        all_8k_headers[line['ACCESSION NUMBER']] = line




def find_articles(article):
    #print('spawned')
    year = article[0]
    news_article_location = os.path.join(articles_root, article[1])
    r_val = []
    with open(news_article_location, 'r', errors='ignore') as news_article:
        news_article_text = news_article.read()
        edgar_article_key = article[2]
        #only want those 8-Ks with a cover sheet and the earnings press release
        #if public_doc_count_check(all_8k_headers[edgar_article_key]['PUBLIC DOCUMENT COUNT'], 2):
        #    return (False, article)
        # if edgar_article_key in files_to_skip:
        #     return False
        edgar_article_match = parsed_articles_dict[year].get(edgar_article_key, None)
        if edgar_article_match:
            for edgar_article_candidate in edgar_article_match:
                edgar_article_fields = edgar_article_candidate[0].split(sep='<>')
                edgar_article_location = os.path.join(parsed_articles_root_dir+edgar_article_candidate[1])
                with open(edgar_article_location, 'r', errors='ignore') as edgar_article:
                    #todo: create ordered_fieldnames class to declutter code
                    #done
                    # if all_8k_headers[edgar_article_key]['LINK'] != 'http://www.sec.gov/Archives/edgar/data/794367/000079436710000154/es8k08112010.htm':
                    #     continue
                    ordered_fieldnames_1 = return_blank_ordered_dictionary()
                    identifying_fieldnames = article[1].split('_')
                    #
                    edgar_article_text = edgar_article.read()
                    soup = BeautifulSoup(edgar_article_text, "lxml")
                    #keep some tables -- they will be removed by the sentence parser if its pure numbers
                    #some filers put their entire presentation in tabular format
                    soup = remove_tables(soup, .8)

                    edgar_article_text = soup.get_text()

                    #post processing -- guest 2018 -- remove short lines
                    edgar_article_text = remove_short_lines(edgar_article_text, 19)
                    news_article_text = remove_short_lines(news_article_text, 19)

                    edgar_article_sentences = parse_sentences(edgar_article_text)
                    news_article_sentences = parse_sentences(news_article_text)
                    edgar_branching_obj = branching()
                    news_branching_obj = branching()
                    simscore_branching_obj = branching()
                    for label in data_labels.label_headers:
                        if 'SIMSCORE_' in label:
                            a = simscore_branching_obj.brancher(label, news_article_text, edgar_article_text, news_article_sentences, edgar_article_sentences)
                            if a or a == 0:
                                ordered_fieldnames_1[label] = a
                            else:
                                ordered_fieldnames_1[label] = '-99'
                        elif 'EDGAR' in label:
                            a = edgar_branching_obj.brancher(label, news_article_text, edgar_article_text, news_article_sentences, edgar_article_sentences)
                            if a or a == 0:
                                ordered_fieldnames_1[label] = a
                            else:
                                ordered_fieldnames_1[label] = '-99'
                        elif 'NEWS_ARTICLE' in label:
                            a = news_branching_obj.brancher(label, news_article_text, edgar_article_text,
                                                             news_article_sentences, edgar_article_sentences)
                            if a or a == 0:
                                ordered_fieldnames_1[label] = a
                            else:
                                ordered_fieldnames_1[label] = '-99'
                    #todo: Add this to the config file -- this is a birds nest and is unreadable.
                    ordered_fieldnames_1['GVKEY'] = identifying_fieldnames[1]
                    ordered_fieldnames_1['FDS'] = identifying_fieldnames[2]
                    ordered_fieldnames_1['CUSIP'] = identifying_fieldnames[3]
                    ordered_fieldnames_1['TICKER'] = identifying_fieldnames[4]
                    ordered_fieldnames_1['ACCESSION_NUMBER'] = edgar_article_key
                    ordered_fieldnames_1['EXHIBIT_FILENAME'] = edgar_article_fields[1]
                    ordered_fieldnames_1['ARTICLE_FILENAME'] = article[1]
                    ordered_fieldnames_1['EXHIBIT_NAME'] = edgar_article_fields[2]
                    ordered_fieldnames_1['INTERNAL_EXHIBIT_FILENAME'] = edgar_article_candidate[0]
                    ordered_fieldnames_1['INTERNAL_ARTICLE_FILENAME'] = article[1]
                    ordered_fieldnames_1['ARTICLE_TIMESTAMP'] = article[3]
                    ordered_fieldnames_1['EXHIBIT_TIMESTAMP'] = article[4]
                    ordered_fieldnames_1['TIMESTAMP_DISTANCE'] = article[5]
                    ordered_fieldnames_1['ACCEPTANCE-DATETIME'] = all_8k_headers[edgar_article_key]['ACCEPTANCE-DATETIME']
                    ordered_fieldnames_1['CENTRAL_INDEX_KEY'] = all_8k_headers[edgar_article_key]['CENTRAL INDEX KEY']
                    ordered_fieldnames_1['CITY'] = all_8k_headers[edgar_article_key]['CITY']
                    ordered_fieldnames_1['COMPANY_CONFORMED_NAME'] = all_8k_headers[edgar_article_key]['COMPANY CONFORMED NAME']
                    ordered_fieldnames_1['CONFORMED_PERIOD_OF_REPORT'] = all_8k_headers[edgar_article_key]['CONFORMED PERIOD OF REPORT']
                    ordered_fieldnames_1['CONFORMED_SUBMISSION_TYPE'] = all_8k_headers[edgar_article_key]['CONFORMED SUBMISSION TYPE']
                    ordered_fieldnames_1['FILED_AS_OF_DATE'] = all_8k_headers[edgar_article_key]['FILED AS OF DATE']
                    ordered_fieldnames_1['FILENAME'] = all_8k_headers[edgar_article_key]['FILENAME']
                    ordered_fieldnames_1['FILING_YEAR'] = all_8k_headers[edgar_article_key]['FILING YEAR']
                    ordered_fieldnames_1['FISCAL_YEAR_END'] = all_8k_headers[edgar_article_key]['FISCAL YEAR END']
                    ordered_fieldnames_1['ITEM_INFORMATION'] = all_8k_headers[edgar_article_key]['ITEM INFORMATION']
                    ordered_fieldnames_1['LINK'] = all_8k_headers[edgar_article_key]['LINK']
                    ordered_fieldnames_1['PUBLIC_DOCUMENT_COUNT'] = all_8k_headers[edgar_article_key]['PUBLIC DOCUMENT COUNT']
                    ordered_fieldnames_1['STANDARD_INDUSTRIAL_CLASSIFICATION'] = all_8k_headers[edgar_article_key]['STANDARD INDUSTRIAL CLASSIFICATION']
                    ordered_fieldnames_1['STATE'] = all_8k_headers[edgar_article_key]['STATE']
                    ordered_fieldnames_1['STATE_OF_INCORPORATION'] = all_8k_headers[edgar_article_key]['STATE OF INCORPORATION']
                    r_val.append(ordered_fieldnames_1)
    return (True, r_val)


#overall structure
#the linking table is a dictionary where the KEY is the GVKEY and the value is the CIK
#There is also a cusip linking table where the KEY is the CUSIP and the value is the CIK
#The CIK maps to the 8-K header table
#The header table is preprocessed to contain the following information:
#The key is the cik
#the value is a dictionary of YEARS
#each year has a LIST of acceptance datetime and accession number PAIRS
#once we have the LOWEST comparison value between filing and news article, we DIRECTLY open the 8-K filing
#We need to create a "quarter identification" method
#create an output list that maps the LOCATION of the article with the LOCATION of the closest article


################################################
#map simililarity scores in a separate program
#once we have this information -- directly open the file.
#compare file to the article
#report score in an output csv
################################################


#create articles list
articles_root = os.path.join('/media/abc-123/M2/Zahn/Output_2')
#articles_root = os.path.join('/media/abc-123/EDGAR/Zahn/Output_2')

articles = os.listdir(articles_root)

#construct the list:



#k = gvkey, v = datetime group, article name pair
gv_article_dict = {}
#k = cusip, v = datetime group, article name pair
cusip_article_dict = {}
#k = gvkey, v = datetime group, article name, cusip triple
gv_cusip_article_dict = {}

if use_pickle == False:
    def update_dict(dictionary, key, article_name, cusip=None):
        article = article_name.split(sep='_')
        # populate
        year = article[5][0:4]
        if dictionary.get(year, None):
            if dictionary[year].get(key, None):
                dictionary[year][key].append((str(article[5] + article[6][0:6]), article_name, cusip))
            else:
                dictionary[year].update({key: [(str(article[5] + article[6][0:6]), article_name, cusip)]})
        else:
            dictionary[year] = {key: [(str(article[5] + article[6][0:6]), article_name, cusip)]}
        return dictionary

    for article_fn in articles:
        article_split = article_fn.split(sep='_')
        gvkey = article_split[1]
        cusip = article_split[3]
        #populate
        gv_article_dict = update_dict(gv_article_dict, gvkey, article_fn)
        cusip_article_dict = update_dict(cusip_article_dict, cusip, article_fn)
        gv_cusip_article_dict = update_dict(gv_cusip_article_dict, gvkey, article_fn, cusip=cusip)

    pickle.dump(gv_article_dict, open('gv_article_dict.pickle', 'wb'))
    pickle.dump(cusip_article_dict, open('cusip_article_dict.pickle', 'wb'))
    pickle.dump(gv_cusip_article_dict, open('gv_cusip_article_dict.pickle', 'wb'))

if use_pickle:
    gv_article_dict = pickle.load(open('gv_article_dict.pickle', 'rb'))
    cusip_article_dict = pickle.load(open('cusip_article_dict.pickle', 'rb'))
    gv_cusip_article_dict = pickle.load(open('gv_cusip_article_dict.pickle', 'rb'))


#create linking dictionary
gv_cik_link = {}
cusip_cik_link = {}
gv_cusip_link = {}
with open('./Library/linking_table.csv', 'r', errors='ignore') as f:
    csv_file = csv.DictReader(f)
    for line in csv_file:
        gv_cik_link[str(int(line['gvkey']))] = line.get('cik', None)
        cusip_cik_link[line['cusip']] = line.get('cik', None)
        gv_cusip_link[str(int(line['gvkey']))] = line.get('cusip', None)

#create cik linking dictionary
#TAKE ONLY THOSE 8-KS WITH RESULTS OF OPERATIONS IN MAP
#link the CIK from the header file to the relevant 8-k's
#structure:
#YEAR|CIK|FILINGS
#PATH GV_CUSIP_ARTICLE_DICT (Get YEAR/GET GVKEY/GET DATETIMESTAMP/GET FILENAME)
#GVKEY-CIK MAP (GET CIK FROM GVKEY)
#FILINGS-DICT (MATCH YEAR/FIND CLOSEST FILING/GET FILENAME)
#COMPARE SIMILARITIES BETWEEN FILES

header_file = os.path.join('/media/abc-123/EDGAR/ALL_8K_HEADER_INFO_2002_2019_RO.csv')
header_dict = {}
#ACCEPTANCE-DATETIME,ACCESSION NUMBER,CENTRAL INDEX KEY,CITY,COMPANY CONFORMED NAME,CONFORMED PERIOD OF REPORT,
#CONFORMED SUBMISSION TYPE,FILED AS OF DATE,FILENAME,FILING YEAR,FISCAL YEAR END,ITEM INFORMATION,
#LINK,PUBLIC DOCUMENT COUNT,STANDARD INDUSTRIAL CLASSIFICATION,STATE,STATE OF INCORPORATION

if use_pickle:
    header_dict = pickle.load(open('header_dict.pickle', 'rb'))

else:
    with open(header_file, 'r', errors='ignore') as f:
        csv_file = csv.DictReader(f)
        for line in csv_file:
            if 'Results' not in line.get('ITEM INFORMATION'):
                continue
            year = line.get('FILING YEAR', None)
            quarter = line.get('ACCEPTANCE-DATETIME', None)
            try:
                quarter = str(math.ceil(int(quarter[4:6])/4))
            except (TypeError, ValueError):
                quarter = line.get('FILED AS OF DATE', None)
                try:
                    quarter = str(math.ceil(int(quarter[4:6])/4))
                except (TypeError, ValueError):
                    quarter = 0
            key = line.get('CENTRAL INDEX KEY', None)
            #careful below! Older versions of edgar header search wont correctly return the acceptance-datetime
            #use the newest headerParse version from the github EDGAR repository.
            time = line.get('ACCEPTANCE-DATETIME', None)
            accession = line.get('ACCESSION NUMBER', None)
            location = os.path.join('/'+year, 'QTR'+str(quarter), accession+'.txt')
            if header_dict.get(year, None):
                if header_dict[year].get(key, None):
                    header_dict[year][key].append((accession, time, location))
                else:
                    header_dict[year].update({key: [(accession, time, location)]})
            else:
                header_dict[year] = {key: [(accession, time, location)]}
    pickle.dump(header_dict, open('header_dict.pickle', 'wb'))

#compare articles
#steps
#1) build parsed article list
#2) parse the filenames
#3) compare EACH of the files
#4) report both results

parsed_articles_dict = {}
parsed_articles_root_dir = '/media/abc-123/M2/8K_Output'
#parsed_articles_root_dir = '/media/abc-123/EDGAR/8K_Output'
if use_pickle:
    parsed_articles_dict = pickle.load(open('parsed_articles_dict.pickle', 'rb'))
else:
    for root, dir, files in os.walk(parsed_articles_root_dir):
        for file in files:
            filename_values = file.split(sep='<>')
            accession = filename_values[0]
            year = root[-4:]
            location = os.path.join('/'+year, file)
            if parsed_articles_dict.get(year, None):
                if parsed_articles_dict[year].get(accession, None):
                    parsed_articles_dict[year][accession].append((file, location))
                else:
                    parsed_articles_dict[year].update({accession: [(file, location)]})
            else:
                parsed_articles_dict[year] = {accession: [(file, location)]}

    pickle.dump(parsed_articles_dict, open('parsed_articles_dict.pickle', 'wb'))

articles_to_compare = []
#TODO: Refactor -- this is a birdsnest.
if use_pickle_article:
    articles_to_compare = pickle.load(open('articles_to_compare.pickle', 'rb'))
else:
    for article_year in gv_cusip_article_dict:
        #for each gvkey in the article dictionary (articles are separated by gvkey time)
        for gvkey in gv_cusip_article_dict[article_year]:
            for article_values in gv_cusip_article_dict[article_year][gvkey]:
                #get the article information (from the filename)
                #the timestamp is the first value in this tuple
                article_time = article_values[0]
                #grab the cik from the gvkey
                cik = gv_cik_link.get(gvkey, None)
                if cik:
                    #set the initial lowest time value to an arbitrarially large number (in seconds)
                    lowest = 10000000000000000000
                    articles = None
                    #loop through each filing/year. We need to take into consideration
                    #that a filing can be released at the end of the year and an article
                    #will come out in the next year. We cant do simple subtraction
                    for EDGAR_filing_year, v in header_dict.items():
                        #compare the year with the article year, if the filing year < article year, continue
                        if EDGAR_filing_year < article_year:
                            continue
                        filing_values = header_dict[EDGAR_filing_year].get(cik, None)
                        if filing_values:
                            for value in filing_values:
                                #check the timevalues for error values
                                if value[1] == '-99' or article_values == '-99':
                                    continue
                                #convert the datetimes to datetime format
                                try:
                                   # print(value[1])
                                    d1 = datetime.strptime(value[1], "%Y%m%d%H%M%S")
                                except ValueError:
                                    print(value[1])
                                    print('value error d1')
                                except TypeError:
                                    print(value[1])
                                    print('type error d1')
                                try:
                                  #  print(article_values[0])
                                    d2 = datetime.strptime(article_values[0], "%Y%m%d%H%M%S")
                                except ValueError as error_text:
                                     print(article_values)
                                     print(len(article_values[0]))
                                     print('value error d2 {}'.format(error_text))
                                     # try:
                                     #     d2 = datetime.strptime(article_values[0][])
                                #compare the datetime formats, keep the lowest
                                if (d2-d1).total_seconds() > 0:
                                    #changed this code -- we want to grab ALL articles that are 5 days or less 432000 seconds
                                    #if lowest > (d2-d1).total_seconds():
                                    if (d2-d1).total_seconds() <= 432000:
                                        #print((d2-d1).total_seconds())
                                        seconds = (d2-d1).total_seconds()
                                        #lowest = (d2-d1).total_seconds()
                                        #change this in the future, horrible HORRIBLE code.
                                        #year, hand collected article filename, exhibit filename, hand collected article timestamp,
                                        #exhibit timestamp, distance between timestamps.
                                        #comment below out to grab closest article
                                        articles = (article_year, article_values[1], value[0], article_values[0], value[1], seconds,
                                                    EDGAR_filing_year)
                                        articles_to_compare.append(articles)

                    #if a match is found, append the match to the articles to compare
                    #commented out -- remove to grab the closest article
                    #if articles is not None:
                        #articles_to_compare.append(articles)
    pickle.dump(articles_to_compare, open('articles_to_compare.pickle', 'wb'))


#compare the files

#match parsed articles with articles to compare
#perform cosine similarity
output_file = '/media/abc-123/EDGAR/simscore_after.csv'
counter = 1

#todo: CLEAN UP THIS CODE. IT IS HARDLY READABLE TO OTHERS
#old style of writing -- this is ugly as all sin
# while os.path.isfile(output_file):
#     output_file = os.path.join('/media/abc-123/EDGAR/simscore_'+str(counter)+'.csv')
#     counter += 1
#     ordered_fieldnames = OrderedDict(
#         [('ACCESSION NUMBER', None), ('GVKEY', None), ('FDS', None), ('CUSIP', None), ('TICKER', None),
#          ('ARTICLE FILENAME', None), ('EXHIBIT FILENAME', None), ('SIMSCORE COSINE', None),
#          ('SIMSCORE JACCARD', None), ('EXHIBIT NAME', None), ('INTERNAL ARTICLE FILENAME', None),
#          ('INTERNAL EXHIBIT FILENAME', None), ('ARTICLE TIMESTAMP', None), ('EXHIBIT TIMESTAMP', None),
#          ('TIMESTAMP DISTANCE', None), ('ACCEPTANCE-DATETIME', None), ('CENTRAL INDEX KEY', None),
#          ('CITY', None), ('COMPANY CONFORMED NAME', None), ('CONFORMED PERIOD OF REPORT', None),
#          ('CONFORMED SUBMISSION TYPE', None), ('FILED AS OF DATE', None), ('FILENAME', None), ('FILING YEAR', None),
#          ('FISCAL YEAR END', None), ('ITEM INFORMATION', None), ('LINK', None), ('PUBLIC DOCUMENT COUNT', None),
#          ('STANDARD INDUSTRIAL CLASSIFICATION', None), ('STATE', None), ('STATE OF INCORPORATION', None),
#          ('NEWS ARTICLE PRESENT SENTENCE COUNT', None), ('NEWS ARTICLE PAST SENTENCE COUNT', None),
#          ('NEWS ARTICLE NUMERIC SENTENCE COUNT', None), ('NEWS ARTICLE EARNINGS SENTENCE COUNT', None),
#          ('NEWS ARTICLE FORWARD LOOKING SENTENCE COUNT', None), ('NEWS ARTICLE TOTAL SENTENCE COUNT', None),
#          ('EDGAR PRESENT SENTENCE COUNT', None), ('EDGAR PAST SENTENCE COUNT', None),
#          ('EDGAR NUMERIC SENTENCE COUNT', None), ('EDGAR EARNINGS SENTENCE COUNT', None),
#          ('EDGAR FORWARD LOOKING SENTENCE COUNT', None), ('EDGAR TOTAL SENTENCE COUNT', None),
#          ('NEWS ARTICLE POSITIVE WORDS', None), ('NEWS ARTICLE NEGATIVE WORDS', None),
#          ('NEWS ARTICLE TOTAL WORDS', None), ('NEWS ARTICLE FOG', None),
#          ('EDGAR POSITIVE WORDS', None), ('EDGAR NEGATIVE WORDS', None),
#          ('EDGAR TOTAL WORDS', None), ('EDGAR FOG', None), ])
#
# with open(output_file, 'w', errors='ignore', newline='') as f:
#     writer = csv.DictWriter(f, fieldnames=ordered_fieldnames)
#     writer.writeheader()
#counter = 0
# pool = mp.Pool(mp.cpu_count() - 2)
# results = []

#uncomment this to test writting the output of a subset of the articles in multithreading
#articles_to_compare = articles_to_compare[1:100]




#write everything asyncronously
#The listener WILL SILENTLY CRASH. IF THERE IS NOTHING WRITTEN, THEN THE LISTENER CRASHED! BAD BAD BAD!
#welcome to the absolute, complete hell of multiprocessing. debugging is nearly impossible. yey.
# def listener(q):
#     ''' listens for messages on the q'''
#     counter = 1
#     ordered_fieldnames_headers = OrderedDict(
#         [('ACCESSION NUMBER', None), ('GVKEY', None), ('FDS', None), ('CUSIP', None), ('TICKER', None),
#          ('ARTICLE FILENAME', None), ('EXHIBIT FILENAME', None), ('SIMSCORE COSINE', None),
#          ('SIMSCORE JACCARD', None), ('EXHIBIT NAME', None), ('INTERNAL ARTICLE FILENAME', None),
#          ('INTERNAL EXHIBIT FILENAME', None), ('ARTICLE TIMESTAMP', None), ('EXHIBIT TIMESTAMP', None),
#          ('TIMESTAMP DISTANCE', None), ('ACCEPTANCE-DATETIME', None), ('CENTRAL INDEX KEY', None),
#          ('CITY', None), ('COMPANY CONFORMED NAME', None), ('CONFORMED PERIOD OF REPORT', None),
#          ('CONFORMED SUBMISSION TYPE', None), ('FILED AS OF DATE', None), ('FILENAME', None), ('FILING YEAR', None),
#          ('FISCAL YEAR END', None), ('ITEM INFORMATION', None), ('LINK', None), ('PUBLIC DOCUMENT COUNT', None),
#          ('STANDARD INDUSTRIAL CLASSIFICATION', None), ('STATE', None), ('STATE OF INCORPORATION', None),
#          ('NEWS ARTICLE PRESENT SENTENCE COUNT', None), ('NEWS ARTICLE PAST SENTENCE COUNT', None),
#          ('NEWS ARTICLE NUMERIC SENTENCE COUNT', None), ('NEWS ARTICLE EARNINGS SENTENCE COUNT', None),
#          ('NEWS ARTICLE FORWARD LOOKING SENTENCE COUNT', None), ('NEWS ARTICLE TOTAL SENTENCE COUNT', None),
#          ('EDGAR PRESENT SENTENCE COUNT', None), ('EDGAR PAST SENTENCE COUNT', None),
#          ('EDGAR NUMERIC SENTENCE COUNT', None), ('EDGAR EARNINGS SENTENCE COUNT', None),
#          ('EDGAR FORWARD LOOKING SENTENCE COUNT', None), ('EDGAR TOTAL SENTENCE COUNT', None),
#          ('NEWS ARTICLE POSITIVE WORDS', None), ('NEWS ARTICLE NEGATIVE WORDS', None),
#          ('NEWS ARTICLE TOTAL WORDS', None), ('NEWS ARTICLE FOG', None),
#          ('EDGAR POSITIVE WORDS', None), ('EDGAR NEGATIVE WORDS', None),
#          ('EDGAR TOTAL WORDS', None), ('EDGAR FOG', None), ])
#     output_file = '/media/abc-123/EDGAR/simscore_after.csv'
#     while os.path.isfile(output_file):
#         output_file = os.path.join('/media/abc-123/EDGAR/simscore_' + str(counter) + '.csv')
#         counter += 1
#
#     with open('/media/abc-123/EDGAR/multiple_files.txt', 'w', newline='', errors='ignore', encoding='UTF-8') as multiple_l:
#         with open(output_file, 'w', newline='', errors="ignore", encoding='UTF-8') as csv_l:
#             writer = csv.DictWriter(csv_l, fieldnames=ordered_fieldnames_headers)
#             writer.writeheader()
#             #counter_output = 1
#             while 1:
#                 #print('listening {}'.format(counter_output))
#                 #counter_output += 1
#                 #csvOutput = csv.writer(csv_l, quotechar='"', quoting=csv.QUOTE_MINIMAL)
#                 header_data = q.get()
#                 #print(header_data)
#                 if header_data == 'kill':
#                     break
#                 # if header_data returns a false value (ie, the vale does not conform to our expectations: ignore the value
#                 if header_data[0]:
#                     print(header_data[1][0])
#                     #header_data returns a list of ordered dicts. Iterate through each one then write
#                     for l_val in header_data[1][0]:
#                         # print('first loop')
#                         # print(l_val)
#                         if l_val:
#                             # print('writing')
#                             writer.writerow(l_val)
#                             print('writing')
#                             csv_l.flush()
#                             print('flush')
                    # else:
                    #     multiple_l.write(header_data[1])
                    #     multiple_l.flush()
                    #     print(header_data[1])
                    # make ABSOLUTELY SURE that flushing to disk is done _immediately_ after a write command
                    # otherwise, the data will hang in memory and may cause the listener to hang
                    # multiple_l.flush()
                    # csv_l.flush()



# def mp_handler_exceptions(*args, **kwargs):
#     try:
#         mp_handler(*args,**kwargs)
#     except (KeyboardInterrupt, SystemExit):
#         raise
#     except:
#         traceback.print_exec(file=sys.stdout)
#
#
# def init_globals(counter):
#     global _COUNTER
#     _COUNTER = counter

#TODO: Birdsnest
def mp_handler():
    #pool = mp.Pool(mp.cpu_count()-2)
    #counter = mp.Value('i', 0)
    counter_main_file = 1
    #ordered_fieldnames_headers = return_blank_ordered_dictionary()
    output_file = '/media/abc-123/EDGAR/simscore_after.csv'
    while os.path.isfile(output_file):
        output_file = os.path.join('/media/abc-123/EDGAR/simscore_' + str(counter_main_file) + '.csv')
        counter_main_file += 1



    with open('/media/abc-123/EDGAR/multiple_files.txt', 'w', errors='ignore', encoding='UTF-8') as multiple_l:
        with open(output_file, 'w', newline='', errors="ignore", encoding='UTF-8') as csv_l:
            #write headers
            writer = csv.DictWriter(csv_l, fieldnames=return_blank_ordered_dictionary())
            writer.writeheader()
            #for header_data in pool.imap(find_articles, tqdm(articles_to_compare),8):
            #much better way when compared with
            #mp.cpu_count()-2
            with futures.ProcessPoolExecutor(max_workers = mp.cpu_count()-1) as executor:
                # if header_data returns a false value (ie, the vale does not conform to our expectations: ignore the value
                #need to iterate the list to submit!
                #start = 70000+2000
                start = 0
                #start = 6292+1274+540+1404+2150+5482+715+1965
                running = {executor.submit(find_articles, article): article for article in tqdm(articles_to_compare)}
                #please work -- if it does this will TAKE the results one by one as they are released and write to the file.
                #please work :(
                for result in futures.as_completed(running):
                   filename = running[result][1]
                   #print('in results')

                   header_data = result.result()
                       # print('%r generated an exception : %s' % (filename, exc))
                       # header_data = (False, False)
                   a = 1
                   if header_data[0]:
                        #print(header_data[1])
                        #header_data returns a list of ordered dicts. Iterate through each one then write
                        for l_val in header_data[1]:
                            # print('first loop')
                            # print(l_val)
                            if l_val:
                                #print(l_val)
                                #print('writing')
                                writer.writerow(l_val)
                                #print('writing')
                                #print('flush')
                   else:
                       pass
                        # multiple_l.write("{} -- {}\n".format(header_data[1][1], header_data[1][2]))
                        # multiple_l.flush()
            # print('MULTIPLE_L {}'.format(header_data[1][2]))
            #csv_l.flush()



# def worker(arg, q):
#     '''main process'''
#     res = find_articles(arg)
#     q.put(res)
#     return res


#
# def main():
#     #must use Manager queue here, or will not work
#     #manager locks the file and creates a queue
#     manager = mp.Manager()
#     q = manager.Queue()
#     pool = mp.Pool(mp.cpu_count() - 1)
#
#     #put listener to work first
#     #we do not want to have data before the listener is ready
#     print('starting listener')
#     pool.apply_async(listener, (q,))
#     #print(watcher)
#
#     #Use the filings list to construct the output
#
#     #create a list of current jobs
#     #fire off all the workers
#     jobs = []
#     print('queueing filings')
#     for filing in tqdm(articles_to_compare):
#         job = pool.apply_async(worker, (filing, q))
#         jobs.append(job)
#
#     #collec results from the workers through the pool result queue
#     print('firing off workers')
#     for job in tqdm(jobs):
#         job.get()
#
#     #we are done. kill the listener
#     q.put('kill')
#     pool.close()
#     pool.join()

if __name__ == '__main__':
    mp_handler()
#main writting of output for multithreading
# results = [pool.map(find_articles, tqdm(articles_to_compare))]
#
# pickle.dump(results, open('results_after.pickle', 'wb'))

# for l_val in results:
#     print(l_val)
#     if l_val:
#         for l_val_2 in l_val:
#             if l_val_2:
#                 for dictionary in l_val_2:
#                     with open(output_file, 'a', errors='ignore', newline='') as f:
#                         writer = csv.DictWriter(f, fieldnames=ordered_fieldnames)
#                         writer.writerow(dictionary)

#sequential code for testing -- slow
# for year in range(2010, 2017):
#     for article in tqdm(articles_to_compare):
#         if counter > 10:
#             break
#         counter += 1
#         ordered_fieldnames_t = find_articles(article)
#         # news_article_location = os.path.join(articles_root, article[1])
#         # with open(news_article_location, 'r', errors='ignore') as news_article:
#         #     news_article_text = news_article.read()
#         #     edgar_article_key = article[2]
#         #     #make sure to cast the year as a string for the key, year is an integer when we do the range above
#         #     edgar_article_match = parsed_articles_dict[str(year)].get(edgar_article_key, None)
#         #     if edgar_article_match:
#         #         for edgar_article_candidate in edgar_article_match:
#         #             edgar_article_fields = edgar_article_candidate[0].split(sep='<>')
#         #             edgar_article_location = os.path.join(parsed_articles_root_dir+edgar_article_candidate[1])
#         #             with open(edgar_article_location, 'r', errors='ignore') as edgar_article:
#         #                 ordered_fieldnames = OrderedDict(
#         #                     [('ACCESSION NUMBER', None), ('ARTICLE FILENAME', None), ('EXHIBIT FILENAME', None), ('SIMSCORE COSINE', None),
#         #                      ('SIMSCORE JACCARD', None), ('EXHIBIT NAME', None), ('INTERNAL ARTICLE FILENAME', None),
#         #                      ('INTERNAL EXHIBIT FILENAME', None), ('ARTICLE TIMESTAMP', None), ('EXHIBIT TIMESTAMP', None),
#         #                      ('TIMESTAMP DISTANCE', None)])
#         #                 edgar_article_text = edgar_article.read()
#         #                 ordered_fieldnames['SIMSCORE COSINE'] = cosine_sim(news_article_text, edgar_article_text)
#         #                 ordered_fieldnames['SIMSCORE JACCARD'] = jaccard_sim(news_article_text, edgar_article_text)
#         #                 ordered_fieldnames['ACCESSION NUMBER'] = edgar_article_key
#         #                 ordered_fieldnames['EXHIBIT FILENAME'] = edgar_article_fields[1]
#         #                 ordered_fieldnames['ARTICLE FILENAME'] = article[1]
#         #                 ordered_fieldnames['EXHIBIT NAME'] = edgar_article_fields[2]
#         #                 ordered_fieldnames['INTERNAL EXHIBIT FILENAME'] = edgar_article_candidate[0]
#         #                 ordered_fieldnames['INTERNAL ARTICLE FILENAME'] = article[1]
#         #                 ordered_fieldnames['ARTICLE TIMESTAMP'] = article[3]
#         #                 ordered_fieldnames['EXHIBIT TIMESTAMP'] = article[4]
#         #                 ordered_fieldnames['TIMESTAMP DISTANCE'] = article[5]
#         with open(output_file, 'a', errors='ignore', newline='') as f:
#             writer = csv.DictWriter(f, fieldnames=ordered_fieldnames_t[0])
#             writer.writerow(ordered_fieldnames_t[0])


print('done')








