#TODO: Remove unused code
#TODO: Declutter the birdsnests.

import csv
import math
import os
import nltk
from nltk.corpus import stopwords
from nltk.stem.snowball import SnowballStemmer
import re
#nltk.download('all')
from article_tools import _construct_fieldnames, _create_dictionary_from_dictionary

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
#set to false to rebuild file list
edgar_8k_pickle = False
news_article_pickle = False

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

def save_raw_text_files(save_location, *files):
    save_location_terminal_directory = 1
    while os.path.isdir(os.path.join(save_location, str(save_location_terminal_directory))):
        save_location_terminal_directory += 1
    save_location = os.path.join(save_location, str(save_location_terminal_directory))
    os.mkdir(save_location)
    for s_file in files:
        with open(os.path.join(save_location, s_file[0]), 'w', errors='ignore') as f_save_raw_files:
            f_save_raw_files.write(s_file[1])

#construct all valid 8-k's
all_8k_headers = {}
with open('/media/abc-123/EDGAR/ALL_8K_HEADER_INFO_2002_2019_RO.csv', 'r', errors='ignore') as all_8k_header_file:
    reader = csv.DictReader(all_8k_header_file)
    for line in reader:
        all_8k_headers[line['ACCESSION NUMBER']] = line
print('8k headers')
#main multiprocessing function --

def save_file(path, filename, file_content):
    with open(os.path.join(path, filename), 'w', errors='ignore') as f:
        f.write(file_content)

def make_directory(root_dir):
    directory_counter = 1
    if os.path.exists(os.path.join(root_dir, str(directory_counter))):
        directory_counter += 1
    else:
        os.mkdir(os.path.join(root_dir, str(directory_counter)))


def find_articles(article_filename):
    #print('spawned')
    #place fieldnames here instead of global. Multiprocessing functions can only take one input and return one output
    #no point refactoring to make a dictionary as this function is a one-off-- just change the variable names here
    #TODO: MAKE A GOD DAMN LABELING METHOD. MANUAL LABELING INSIDE THE MAIN METHOD IS CANCER. THIS POOR CODING COST COUNTLESS HOURS
    #TODO: REMEMBER DRY, REMEMBER THE RULE OF ONE -- FUNCIONS TAKE ONE INPUT, DO ONE THING, RETURN ONE OUTPUT
    #TODO: BREAK APART THIS MAIN FUNCION
    #ThIs Is CaNcEr
    article_filename_keys = ['YEAR',
                            'NEWS_ARTICLE_FILENAME',
                            'EDGAR_ARTICLE_KEY',
                            'ARTICLE_TIMESTAMP',
                            'EXHIBIT_TIMESTAMP',
                            'TIMESTAMP_DISTANCE',
                            'EDGAR_YEAR']
    news_article_keys = ['COMPANY_NAME_NEWS_FILENAME',
                                         'GVKEY',
                                         'FDS',
                                         'CUSIP',
                                         'TICKER',
                                         'NEWS_ARTICLE_DATE_1',
                                           ]
    #TODO: Make a method that creates custom labels
    edgar_header_keys = [
                        'ACCESSION NUMBER',
                        'ACCEPTANCE-DATETIME',
                        'CENTRAL INDEX KEY',
                        'CITY',
                        'COMPANY CONFORMED NAME',
                        'CONFORMED PERIOD OF REPORT',
                        'CONFORMED SUBMISSION TYPE',
                        'EXHIBIT NAME',
                        'FILED AS OF DATE',
                        'FILENAME',
                        'FILING YEAR',
                        'FISCAL YEAR END',
                        'ITEM INFORMATION',
                        'LINK',
                        'PUBLIC DOCUMENT COUNT',
                        'STANDARD INDUSTRIAL CLASSIFICATION',
                        'STATE',
                        'STATE OF INCORPORATION',]
    article_filename_fields = _construct_fieldnames(article_filename, ordered_keys=article_filename_keys, sep=None)
    year = article_filename_fields['YEAR']
    news_article_location = os.path.join(articles_root, article_filename_fields['NEWS_ARTICLE_FILENAME'])
    #news article filename
    #save location
    save_location = '/media/abc-123/EDGAR/parsed_files/'
    article_filename = article_filename_fields['NEWS_ARTICLE_FILENAME']
    final_data_dictionary = []
    with open(news_article_location, 'r', errors='ignore') as news_article:
        #why am i not passing these files as arguments
        news_article_text = news_article.read()
        edgar_8k_identfier_key = article_filename_fields['EDGAR_ARTICLE_KEY']
        #only want those 8-Ks with a cover sheet and the earnings press release
        #if public_doc_count_check(all_8k_headers[edgar_article_key]['PUBLIC DOCUMENT COUNT'], 2):
        #    return (False, article)
        # if edgar_article_key in files_to_skip:
        #     return False
        edgar_article_match = parsed_edgar_files_dict[year].get(edgar_8k_identfier_key, None)
        if edgar_article_match:
            for edgar_article_candidate in edgar_article_match:
                #TODO: improve the indexing names. A huge bug existed because I used numeric indexing
                #TODO: Find a way to make labeled indexing or find a better way to organize the data. Dont use the filename
                edgar_article_fields = edgar_article_candidate[1].split(sep='<>')
                edgar_article_location = os.path.join(parsed_edgar_files_root_dir + edgar_article_candidate[1])
                #edgar article filename
                edgar_filename = edgar_article_candidate[0]
                with open(edgar_article_location, 'r', errors='ignore') as edgar_article:
                    #todo: create ordered_fieldnames class to declutter code
                    #done
                    # if all_8k_headers[edgar_article_key]['LINK'] != 'http://www.sec.gov/Archives/edgar/data/794367/000079436710000154/es8k08112010.htm':
                    #     continue
                    observation_data_dictionary = return_blank_ordered_dictionary()
                    observation_data_dictionary['INTERNAL_EXHIBIT_FILENAME'] = edgar_article_candidate[0]
                    observation_data_dictionary['INTERNAL_ARTICLE_FILENAME'] = article_filename

                    #wsj -- refactor this
                    news_article.seek(0)
                    news_article_first_10_lines = [next(news_article) for i in range(10)]
                    news_article_first_10_lines = ' '.join(news_article_first_10_lines)
                    if re.search('((?:press|immediate|news|media) release|contact[ˆe]+?)', news_article_text, re.I):
                        observation_data_dictionary['ARTICLE_MENTIONS_EA'] = 1
                    else:
                        #continue
                        observation_data_dictionary['ARTICLE_MENTIONS_EA'] = 0
                    if re.search('(?<!of the)(?:\swall street journal)|(?:\swsj\s)',news_article_first_10_lines,re.I):
                        observation_data_dictionary['WSJ_ARTICLE'] = 1
                    else:
                        observation_data_dictionary['WSJ_ARTICLE'] = 0

                    news_article_fields = _construct_fieldnames(article_filename_fields['NEWS_ARTICLE_FILENAME'], ordered_keys=news_article_keys, items_to_match=5)
                    #identifying_fieldnames = article[1].split('_')
                    #
                    edgar_article_text = edgar_article.read()
                    soup = BeautifulSoup(edgar_article_text, "lxml")
                    soup2 = soup.get_text()
                    #keep some tables -- they will be removed by the sentence parser if its pure numbers
                    #some filers put their entire presentation in tabular format
                    #TODO: PREPROCESS BEAUTIFULSOUP
                    soup = remove_tables(soup, .15)
                    edgar_article_text = soup.get_text()

                    #post processing -- guest 2018 -- remove short lines
                    edgar_article_text = remove_short_lines(edgar_article_text, 19)
                    news_article_text = remove_short_lines(news_article_text, 19)
                    #remove stop words & stem
                    edgar_article_text = [w for w in edgar_article_text.split() if w not in stopwords.words('english')]
                    news_article_text = [w for w in news_article_text.split() if w not in stopwords.words('english')]
                    #stem
                    edgar_article_text = [SnowballStemmer("english").stem(w) for w in edgar_article_text]
                    news_article_text = [SnowballStemmer("english").stem(w) for w in news_article_text]
                    #join everything back together again
                    edgar_article_text = ' '.join(edgar_article_text)
                    news_article_text = ' '.join(news_article_text)

                    edgar_article_sentences = parse_sentences(edgar_article_text)
                    news_article_sentences = parse_sentences(news_article_text)

                    #save files
                    #save_raw_text_files(save_location, (edgar_filename, edgar_article_text), (article_filename, news_article_text))

                    edgar_branching_obj = branching()
                    news_branching_obj = branching()
                    simscore_branching_obj = branching()
                    observation_data_dictionary = _create_dictionary_from_dictionary(observation_data_dictionary, article_filename_fields, ordered_keys=article_filename_keys)
                    observation_data_dictionary = _create_dictionary_from_dictionary(observation_data_dictionary, news_article_fields, ordered_keys=news_article_keys)
                    observation_data_dictionary = _create_dictionary_from_dictionary(observation_data_dictionary, all_8k_headers[edgar_8k_identfier_key], ordered_keys=edgar_header_keys)
                    #TODO: make this part of the labeling method. This is extermely cumbersome
                    observation_data_dictionary['EXHIBIT FILENAME'] = edgar_article_fields[1]
                    observation_data_dictionary['EXHIBIT NAME'] = edgar_article_fields[1]
                    qwerty = 1
                    for label in data_labels.label_headers:
                        if 'SIMSCORE_' in label:
                            a = simscore_branching_obj.brancher(label, news_article_text, edgar_article_text, news_article_sentences, edgar_article_sentences)
                            if a or a == 0:
                                observation_data_dictionary[label] = a
                            else:
                                observation_data_dictionary[label] = '-99'
                        elif 'EDGAR' in label:
                            a = edgar_branching_obj.brancher(label, news_article_text, edgar_article_text, news_article_sentences, edgar_article_sentences)
                            if a or a == 0:
                                observation_data_dictionary[label] = a
                            else:
                                observation_data_dictionary[label] = '-99'
                        elif 'NEWS_ARTICLE' in label:
                            a = news_branching_obj.brancher(label, news_article_text, edgar_article_text,
                                                             news_article_sentences, edgar_article_sentences)
                            if a or a == 0:
                                observation_data_dictionary[label] = a
                            else:
                                observation_data_dictionary[label] = '-99'
                    final_data_dictionary.append(observation_data_dictionary)
    return (True, final_data_dictionary)


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
gv_news_article_dict = {}
#k = cusip, v = datetime group, article name pair
cusip_news_article_dict = {}
#k = gvkey, v = datetime group, article name, cusip triple
gv_cusip_news_article_dict = {}

if edgar_8k_pickle == False:
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
        gv_news_article_dict = update_dict(gv_news_article_dict, gvkey, article_fn)
        cusip_news_article_dict = update_dict(cusip_news_article_dict, cusip, article_fn)
        gv_cusip_news_article_dict = update_dict(gv_cusip_news_article_dict, gvkey, article_fn, cusip=cusip)

    pickle.dump(gv_news_article_dict, open('gv_news_article_dict.pickle', 'wb'))
    pickle.dump(cusip_news_article_dict, open('cusip_news_article_dict.pickle', 'wb'))
    pickle.dump(gv_cusip_news_article_dict, open('gv_cusip_news_article_dict.pickle', 'wb'))

if edgar_8k_pickle:
    gv_news_article_dict = pickle.load(open('gv_news_article_dict.pickle', 'rb'))
    cusip_news_article_dict = pickle.load(open('cusip_news_article_dict.pickle', 'rb'))
    gv_cusip_news_article_dict = pickle.load(open('gv_cusip_news_article_dict.pickle', 'rb'))


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
edgar_header_information_dict = {}

if edgar_8k_pickle:
    edgar_header_information_dict = pickle.load(open('header_dict.pickle', 'rb'))

#gets EDGAR header data
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
            if edgar_header_information_dict.get(year, None):
                if edgar_header_information_dict[year].get(key, None):
                    edgar_header_information_dict[year][key].append((accession, time, location))
                else:
                    edgar_header_information_dict[year].update({key: [(accession, time, location)]})
            else:
                edgar_header_information_dict[year] = {key: [(accession, time, location)]}
    pickle.dump(edgar_header_information_dict, open('header_dict.pickle', 'wb'))

#compare articles
#steps
#1) build parsed article list
#2) parse the filenames
#3) compare EACH of the files
#4) report both results

parsed_edgar_files_dict = {}
parsed_edgar_files_root_dir = '/media/abc-123/M2/8K_Output'
#parsed_articles_root_dir = '/media/abc-123/EDGAR/8K_Output'
if edgar_8k_pickle:
    parsed_edgar_files_dict = pickle.load(open('parsed_edgar_articles_dict.pickle', 'rb'))
else:
    for root, dir, files in os.walk(parsed_edgar_files_root_dir):
        for file in files:
            edgar_filename_values = file.split(sep='<>')
            accession = edgar_filename_values[0]
            year = root[-4:]
            location = os.path.join('/'+year, file)
            if parsed_edgar_files_dict.get(year, None):
                if parsed_edgar_files_dict[year].get(accession, None):
                    parsed_edgar_files_dict[year][accession].append((file, location))
                else:
                    parsed_edgar_files_dict[year].update({accession: [(file, location)]})
            else:
                parsed_edgar_files_dict[year] = {accession: [(file, location)]}

    pickle.dump(parsed_edgar_files_dict, open('parsed_edgar_articles_dict.pickle', 'wb'))

news_articles_to_compare_with_edgar_8k = []
#TODO: Refactor -- this is a birdsnest.
if news_article_pickle:
    news_articles_to_compare_with_edgar_8k = pickle.load(open('news_articles_to_compare_with_edgar_8k.pickle', 'rb'))
else:
    for news_article_year in gv_cusip_news_article_dict:
        #for each gvkey in the article dictionary (articles are separated by gvkey time)
        for gvkey in gv_cusip_news_article_dict[news_article_year]:
            for news_article_data_value_container in gv_cusip_news_article_dict[news_article_year][gvkey]:
                #get the article information (from the filename)
                #the timestamp is the first value in this tuple
                news_article_time = news_article_data_value_container[0]
                #grab the cik from the gvkey
                cik = gv_cik_link.get(gvkey, None)
                if cik:
                    #set the initial lowest time value to an arbitrarially large number (in seconds)
                    lowest = 10000000000000000000
                    articles = None
                    #loop through each filing/year. We need to take into consideration
                    #that a filing can be released at the end of the year and an article
                    #will come out in the next year. We cant do simple subtraction
                    for EDGAR_filing_year, v in edgar_header_information_dict.items():
                        #compare the year with the article year, if the filing year < article year, continue
                        if EDGAR_filing_year < news_article_year:
                            continue
                        edgar_filing_values = edgar_header_information_dict[EDGAR_filing_year].get(cik, None)
                        if edgar_filing_values:
                            for value in edgar_filing_values:
                                #check the timevalues for error values
                                if value[1] == '-99' or news_article_data_value_container == '-99':
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
                                    d2 = datetime.strptime(news_article_data_value_container[0], "%Y%m%d%H%M%S")
                                except ValueError as error_text:
                                     print(news_article_data_value_container)
                                     print(len(news_article_data_value_container[0]))
                                     print('value error d2 {}'.format(error_text))
                                     # try:
                                     #     d2 = datetime.strptime(article_values[0][])
                                #compare the datetime formats, keep the lowest
                                if (d2-d1).total_seconds() > 0:
                                    #changed this code -- we want to grab ALL articles that are 5 days or less 432000 seconds
                                    #if lowest > (d2-d1).total_seconds():
                                    if (d2-d1).total_seconds() <= 432000:
                                        seconds = (d2-d1).total_seconds()
                                        #uncomment below out to grab closest article
                                        #lowest = (d2-d1).total_seconds()
                                        #year, hand collected article filename, exhibit filename, hand collected article timestamp,
                                        #exhibit timestamp, distance between timestamps.
                                        #comment below out to grab closest article
                                        articles = (news_article_year, news_article_data_value_container[1], value[0], news_article_data_value_container[0], value[1], seconds,
                                                    EDGAR_filing_year)
                                        news_articles_to_compare_with_edgar_8k.append(articles)

                    #if a match is found, append the match to the articles to compare
                    #commented out -- remove to grab the closest article
                    #if articles is not None:
                        #articles_to_compare.append(articles)
    pickle.dump(news_articles_to_compare_with_edgar_8k, open('news_articles_to_compare_with_edgar_8k.pickle', 'wb'))


#compare the files

#match parsed articles with articles to compare
#perform cosine similarity
output_file = '/media/abc-123/EDGAR/simscore_after.csv'
counter = 1

#TODO: Birdsnest
def mp_handler():
    #pool = mp.Pool(mp.cpu_count()-2)
    #counter = mp.Value('i', 0)
    counter_main_file = 1
    #ordered_fieldnames_headers = return_blank_ordered_dictionary()
    output_file = '/media/abc-123/EDGAR/simscore_after.csv'
    while os.path.isfile(output_file):
        #create output file -- if it exists, increment the counter by one
        output_file = os.path.join('/media/abc-123/EDGAR/simscore_' + str(counter_main_file) + '.csv')
        counter_main_file += 1
    with open('/media/abc-123/EDGAR/multiple_files.txt', 'w', errors='ignore', encoding='UTF-8') as multiple_l:
        with open(output_file, 'w', newline='', errors="ignore", encoding='UTF-8') as csv_l:
            # write headers
            #much better way when compared with
            write_headers = True
            #for header_data in pool.imap(find_articles, tqdm(articles_to_compare),8):
            cpu = mp.cpu_count()-1
            cpu_1 = 1
            with futures.ProcessPoolExecutor(max_workers = cpu_1) as executor:
                # if header_data returns a false value (ie, the vale does not conform to our expectations: ignore the value
                #need to iterate the list to submit!
                running = {executor.submit(find_articles, article): article for article in tqdm(news_articles_to_compare_with_edgar_8k)}
                #This will TAKE the results one by one as they are released and write to the file.
                for result in futures.as_completed(running):
                   filename = running[result][1]

                   header_data = result.result()
                       # print('%r generated an exception : %s' % (filename, exc))
                       # header_data = (False, False)
                   a = 1
                   if header_data[0]:
                       #header_data returns a list of ordered dicts. Iterate through each one then write
                       for l_val in header_data[1]:
                           if l_val:
                               if write_headers:
                                   writer = csv.DictWriter(csv_l, fieldnames=l_val.keys())
                                   writer.writeheader()
                                   write_headers=False
                               writer.writerow(l_val)


if __name__ == '__main__':
    mp_handler()

print('done')








