import csv
import math
import os, sys
import nltk, string
from sklearn.feature_extraction.text import TfidfVectorizer
from collections import OrderedDict
from tqdm import tqdm
import multiprocessing as mp
import pickle
from datetime import datetime

use_pickle = True
use_pickle_article = True

nltk.download('punkt') # if necessary...


stemmer = nltk.stem.porter.PorterStemmer()
remove_punctuation_map = dict((ord(char), None) for char in string.punctuation)

stemming_dictionary = {}

def stem_tokens(tokens):
    ret_list = []
    if tokens == [] or tokens == None or tokens == '':
        return []
    for item in tokens:
        if stemming_dictionary.get(item, None):
            a = stemming_dictionary[item]
        else:
            a = stemmer.stem(item)
            stemming_dictionary[item] = a
        ret_list.append(a)
    return ret_list

'''remove punctuation, lowercase, stem'''
def normalize(text):
    return stem_tokens(nltk.word_tokenize(text.lower().translate(remove_punctuation_map)))

vectorizer = TfidfVectorizer(tokenizer=normalize, stop_words='english')


def cosine_sim(text1, text2):
    tfidf = vectorizer.fit_transform([text1, text2])
    return ((tfidf * tfidf.T).A)[0,1]


def jaccard_sim(text1,text2):
    jaccard = nltk.jaccard_distance(text1, text2)
    return jaccard

class ViviDict(dict):
    def __missing__(self, key):
        value = self[key] = type(self)()
        return value

class AutoVivication(dict):
    """implementation of perl's autovivification feature."""
    def __getitem__(self, item):
        try:
            return dict.__getitem__(self, item)
        except KeyError:
            value = self[item] = type(self)()
            return value

def find_articles(article):
    #print('spawned')
    year = article[0]
    article_1_location = os.path.join(articles_root, article[1])
    r_val = []
    with open(article_1_location, 'r', errors='ignore') as article_1:
        article_1_text = article_1.read()
        article_2_key = article[2]
        article_2_match = parsed_articles_dict[year].get(article_2_key, None)
        if article_2_match:
            for article_2_candidate in article_2_match:
                article_2_fields = article_2_candidate[0].split(sep='<>')
                article_2_location = os.path.join(parsed_articles_root_dir+article_2_candidate[1])
                with open(article_2_location, 'r', errors='ignore') as article_2:
                    ordered_fieldnames_1 = OrderedDict(
                        [('ACCESSION NUMBER', None), ('ARTICLE FILENAME', None), ('EXHIBIT FILENAME', None), ('SIMSCORE COSINE', None),
                         ('SIMSCORE JACCARD', None), ('EXHIBIT NAME', None), ('INTERNAL ARTICLE FILENAME', None),
                         ('INTERNAL EXHIBIT FILENAME', None), ('ARTICLE TIMESTAMP', None), ('EXHIBIT TIMESTAMP', None),
                         ('TIMESTAMP DISTANCE', None)])
                    article_2_text = article_2.read()
                    ordered_fieldnames_1['SIMSCORE COSINE'] = cosine_sim(article_1_text, article_2_text)
                    ordered_fieldnames_1['SIMSCORE JACCARD'] = jaccard_sim(article_1_text, article_2_text)
                    ordered_fieldnames_1['ACCESSION NUMBER'] = article_2_key
                    ordered_fieldnames_1['EXHIBIT FILENAME'] = article_2_fields[1]
                    ordered_fieldnames_1['ARTICLE FILENAME'] = article[1]
                    ordered_fieldnames_1['EXHIBIT NAME'] = article_2_fields[2]
                    ordered_fieldnames_1['INTERNAL EXHIBIT FILENAME'] = article_2_candidate[0]
                    ordered_fieldnames_1['INTERNAL ARTICLE FILENAME'] = article[1]
                    ordered_fieldnames_1['ARTICLE TIMESTAMP'] = article[3]
                    ordered_fieldnames_1['EXHIBIT TIMESTAMP'] = article[4]
                    ordered_fieldnames_1['TIMESTAMP DISTANCE'] = article[5]
                    # with open(output_file, 'a', errors='ignore', newline='') as f:
                    #     writer = csv.DictWriter(f, fieldnames=ordered_fieldnames)
                    #     writer.writerow(ordered_fieldnames)
                    #print(ordered_fieldnames_1['SIMSCORE'])
                    r_val.append(ordered_fieldnames_1)
    return r_val


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
articles_root = os.path.join('/media/abc-123/EDGAR/Zahn/Output_2')

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
parsed_articles_root_dir = '/media/pikakilla/EDGAR/8K_Output'
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
                                     print(article_values[0])
                                     print(len(article_values[0]))
                                     print('value error d2 {}'.format(error_text))
                                #compare the datetime formats, keep the lowest
                                if (d2-d1).total_seconds() > 0:
                                    #changed this code -- we want to grab ALL articles that are 5 days or less 432000 seconds
                                    #if lowest > (d2-d1).total_seconds():
                                    if (d2-d1).total_seconds() <= 432000:
                                        #lowest = (d2-d1).total_seconds()
                                        #change this in the future, horrible HORRIBLE code.
                                        #year, hand collected article filename, exhibit filename, hand collected article timestamp,
                                        #exhibit timestamp, distance between timestamps.
                                        #comment below out to grab closest article
                                        articles = (article_year, article_values[1], value[0], article_values[0], value[1], lowest,
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
while os.path.isfile(output_file):
    output_file = os.path.join('/media/abc-123/EDGAR/simscore_'+str(counter)+'.csv')
    counter += 1
ordered_fieldnames = OrderedDict(
                        [('ACCESSION NUMBER', None), ('ARTICLE FILENAME', None), ('EXHIBIT FILENAME', None), ('SIMSCORE COSINE', None),
                         ('SIMSCORE JACCARD', None), ('EXHIBIT NAME', None), ('INTERNAL ARTICLE FILENAME', None),
                         ('INTERNAL EXHIBIT FILENAME', None), ('ARTICLE TIMESTAMP', None), ('EXHIBIT TIMESTAMP', None),
                         ('TIMESTAMP DISTANCE', None)])
with open(output_file, 'w', errors='ignore', newline='') as f:
    writer = csv.DictWriter(f, fieldnames=ordered_fieldnames)
    writer.writeheader()
counter = 0
pool = mp.Pool(mp.cpu_count() - 1)
results = []

#articles_to_compare = articles_to_compare[1:100]



# results = [pool.map(find_articles, tqdm(articles_to_compare))]
#
# pickle.dump(results, open('results_after.pickle', 'wb'))

# for l_val in results:
#     for l_val_2 in l_val:
#         for dictionary in l_val_2:
#             with open(output_file, 'a', errors='ignore', newline='') as f:
#                 writer = csv.DictWriter(f, fieldnames=ordered_fieldnames)
#                 writer.writerow(dictionary)


for article in tqdm(articles_to_compare):
    if counter > 700:
        break
    counter += 1

    article_1_location = os.path.join(articles_root, article[1])
    with open(article_1_location, 'r', errors='ignore') as article_1:
        article_1_text = article_1.read()
        article_2_key = article[2]
        article_2_match = parsed_articles_dict[year].get(article_2_key, None)
        if article_2_match:
            for article_2_candidate in article_2_match:
                article_2_fields = article_2_candidate[0].split(sep='<>')
                article_2_location = os.path.join(parsed_articles_root_dir+article_2_candidate[1])
                with open(article_2_location, 'r', errors='ignore') as article_2:
                    ordered_fieldnames = OrderedDict(
                        [('ACCESSION NUMBER', None), ('ARTICLE FILENAME', None), ('EXHIBIT FILENAME', None), ('SIMSCORE COSINE', None),
                         ('SIMSCORE JACCARD', None), ('EXHIBIT NAME', None), ('INTERNAL ARTICLE FILENAME', None),
                         ('INTERNAL EXHIBIT FILENAME', None), ('ARTICLE TIMESTAMP', None), ('EXHIBIT TIMESTAMP', None),
                         ('TIMESTAMP DISTANCE', None)])
                    article_2_text = article_2.read()
                    ordered_fieldnames['SIMSCORE COSINE'] = cosine_sim(article_1_text, article_2_text)
                    ordered_fieldnames['SIMSCORE JACCARD'] = jaccard_sim(article_1_text, article_2_text)
                    ordered_fieldnames['ACCESSION NUMBER'] = article_2_key
                    ordered_fieldnames['EXHIBIT FILENAME'] = article_2_fields[1]
                    ordered_fieldnames['ARTICLE FILENAME'] = article[1]
                    ordered_fieldnames['EXHIBIT NAME'] = article_2_fields[2]
                    ordered_fieldnames['INTERNAL EXHIBIT FILENAME'] = article_2_candidate[0]
                    ordered_fieldnames['INTERNAL ARTICLE FILENAME'] = article[1]
                    ordered_fieldnames['ARTICLE TIMESTAMP'] = article[3]
                    ordered_fieldnames['EXHIBIT TIMESTAMP'] = article[4]
                    ordered_fieldnames['TIMESTAMP DISTANCE'] = article[5]
                    with open(output_file, 'a', errors='ignore', newline='') as f:
                        writer = csv.DictWriter(f, fieldnames=ordered_fieldnames)
                        writer.writerow(ordered_fieldnames)


print('done')








