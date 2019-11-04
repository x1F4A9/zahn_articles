import csv
import math
import os, sys
import nltk, string
nltk.download('cmudict')
nltk.download('punkt')
from nltk.tokenize import sent_tokenize
from nltk.corpus import cmudict
from sklearn.feature_extraction.text import TfidfVectorizer
from collections import OrderedDict
from tqdm import tqdm
import multiprocessing as mp
import pickle
from datetime import datetime
from bs4 import BeautifulSoup

import spacy
nlp = spacy.load('en_core_web_sm')

list_directory_root = '/media/abc-123/EDGAR'
d = cmudict.dict()

positive_word_list = []

#END OF FILES TO SKIP

with open(os.path.join(list_directory_root, 'lm_positive.txt'), 'r') as f:
    for line in f:
        positive_word_list.append(line.upper().rstrip())

negative_word_list = []

with open(os.path.join(list_directory_root, 'lm_negative.txt'), 'r') as f:
    for line in f:
        negative_word_list.append(line.upper().rstrip())

forward_word_list = []

with open(os.path.join(list_directory_root, 'forward_looking.txt'), 'r') as f:
    for line in f:
        forward_word_list.append(line.upper().rstrip())

earnings_word_list = []

with open(os.path.join(list_directory_root, 'BVS_EARNINGS.txt'), 'r') as f:
    for line in f:
        earnings_word_list.append(line.upper().rstrip())


def nsyl(word):
    try:
        syllable_count = [len(list( y for y in x if y[-1].isdigit())) for x in d[word.lower()]]
        syllable_count = syllable_count[0]
    except KeyError:
        syllable_count = syllables(word)
    return syllable_count

def syllables(word):
#referred from stackoverflow.com/questions/14541303/count-the-number-of-syllables-in-a-word
    count = 0
    vowels = 'aeiouy'
    word = word.lower()
    if word[0] in vowels:
        count +=1
    for index in range(1,len(word)):
        if word[index] in vowels and word[index-1] not in vowels:
            count +=1
    if word.endswith('e'):
        count -= 1
    if word.endswith('le'):
        count+=1
    if count == 0:
        count +=1
    return count

def detect_past_sentence(sentence):
    sent = list(nlp(sentence).sents)[0]
    return (
        sent.root.tag_ == "VBD" or
        any(w.dep_ == "aux" and w.tag_ == "VBD" for w in sent.root.children))

def detect_present_sentence(sentence):
    sent = list(nlp(sentence).sents)[0]
    return (
        sent.root.tag_ == "VB" or
        any(w.dep_ == "aux" and w.tag_ == "VB" for w in sent.root.children))

def determine_words(sentence_list, word_list):
    number_of_words = 0
    for sentence in sentence_list:
        for word in sentence.split():
            if word.upper().rstrip() in word_list:
                number_of_words += 1
    return number_of_words

def determine_sentences(sentence_list, word_list):
    number_of_sentences = 0
    for sentence in sentence_list:
        for word in sentence.split():
            if word.upper().rstrip() in word_list:
                number_of_sentences += 1
                break
    return number_of_sentences


def determine_sentence_negative_words(sentence):
    pass

def _count_words(sentence_list):
    number_of_words = 0
    for sentence in sentence_list:
        word_list = sentence.split()
        number_of_words += len(word_list)
    return number_of_words

def parse_sentences(document):
    sentences = []
    try:
        for sentence in sent_tokenize(document):
            #Remove sentences that have greater than 50% alpha-numeric characters
            numofnumbers = len([i for i in sentence if i.isdigit() is True])
            sentencelength = len(sentence)
            if numofnumbers / sentencelength < .5:
                sentences.append(sentence)
    except (TypeError, ZeroDivisionError):
        sentences = ['abc.']
    return sentences

def _calculate_fog(sentences):
    sentence_string = ''.join(sentences)
    total_words = len(sentence_string.split())
    try:
        average_words_per_sentence = total_words / len(sentences)
    except ZeroDivisionError:
        average_words_per_sentence = 0
    #Calculate the percentage of complex words within the document
    complex_words = 0
    for word in sentence_string:
        if nsyl(word) > 2:
            complex_words += 1
    try:
        percentage_complex_words = 100 * (complex_words / total_words)
    except ZeroDivisionError:
        percentage_complex_words = 0
    #calculate the FOG Score
    document_fog = (.4 * (average_words_per_sentence + percentage_complex_words))

    return document_fog

#todo: turn into a class

def count_sentences(document):
    return len(document)

def detect_present_tense(sentences):
    present_tense = 0
    for sentence in sentences:
        if detect_present_sentence(sentence):
            present_tense += 1
    return present_tense

def detect_past_tense(sentences):
    past_tense = 0
    for sentence in sentences:
        if detect_past_sentence(sentence):
            past_tense += 1
    return past_tense

def calculate_fog(sentences):
    return _calculate_fog(sentences)

def count_positive_words(sentences):
    return determine_words(sentences, positive_word_list)

def count_negative_words(sentences):
    return determine_words(sentences, negative_word_list)

def count_forward_looking_sentences(sentences):
    return determine_sentences(sentences, forward_word_list)

def count_earnings_sentences(sentences):
    return determine_sentences(sentences, earnings_word_list)

def count_words_in_document(sentences):
    return _count_words(sentences)

def count_numeric_sentences(sentences):
    number_of_numeric_sentences = 0
    for sentence in sentences:
        for word in sentence:
            if word.isdigit():
                number_of_numeric_sentences += 1
                break
    return number_of_numeric_sentences

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
    #Unlike Edit Distance, you cannot just run Jaccard Distance on the strings directly; you must first convert them to the set type.
    #to run jaccard, the inputs MUST be sets -- remember the math behind jaccard distances
    jaccard = nltk.jaccard_distance(set(text1), set(text2))
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

#construct all valid 8-k's
all_8k_headers = {}
with open('/media/abc-123/EDGAR/ALL_8K_HEADER_INFO_2002_2019_RO.csv', 'r', errors='ignore') as all_8k_header_file:
    reader = csv.DictReader(all_8k_header_file)
    for line in reader:
        all_8k_headers[line['ACCESSION NUMBER']] = line

def find_articles(article):
    #print('spawned')
    year = article[0]
    article_1_location = os.path.join(articles_root, article[1])
    r_val = []
    with open(article_1_location, 'r', errors='ignore') as article_1:
        article_1_text = article_1.read()
        article_2_key = article[2]
        #only want those 8-Ks with a cover sheet and the earnings press release
        if all_8k_headers[article_2_key]['PUBLIC DOCUMENT COUNT'] != '2':
            return False
        # if article_2_key in files_to_skip:
        #     return False
        article_2_match = parsed_articles_dict[year].get(article_2_key, None)
        if article_2_match:
            for article_2_candidate in article_2_match:
                article_2_fields = article_2_candidate[0].split(sep='<>')
                article_2_location = os.path.join(parsed_articles_root_dir+article_2_candidate[1])
                with open(article_2_location, 'r', errors='ignore') as article_2:
                    ordered_fieldnames_1 = OrderedDict(
                        [('ACCESSION NUMBER', None), ('GVKEY', None), ('FDS', None), ('CUSIP', None), ('TICKER', None),
                         ('ARTICLE FILENAME', None), ('EXHIBIT FILENAME', None), ('SIMSCORE COSINE', None),
                         ('SIMSCORE JACCARD', None), ('EXHIBIT NAME', None), ('INTERNAL ARTICLE FILENAME', None),
                         ('INTERNAL EXHIBIT FILENAME', None), ('ARTICLE TIMESTAMP', None), ('EXHIBIT TIMESTAMP', None),
                         ('TIMESTAMP DISTANCE', None),('ACCEPTANCE-DATETIME', None),	('CENTRAL INDEX KEY', None),
                         ('CITY', None),	('COMPANY CONFORMED NAME', None),	('CONFORMED PERIOD OF REPORT', None),
                         ('CONFORMED SUBMISSION TYPE', None),	('FILED AS OF DATE', None),	('FILENAME', None),	('FILING YEAR', None),
                         ('FISCAL YEAR END', None),	('ITEM INFORMATION', None),	('LINK', None),	('PUBLIC DOCUMENT COUNT', None),
                         ('STANDARD INDUSTRIAL CLASSIFICATION', None),	('STATE', None),	('STATE OF INCORPORATION', None),
                         ('NEWS ARTICLE PRESENT SENTENCE COUNT', None), ('NEWS ARTICLE PAST SENTENCE COUNT', None),
                         ('NEWS ARTICLE NUMERIC SENTENCE COUNT', None), ('NEWS ARTICLE EARNINGS SENTENCE COUNT', None),
                         ('NEWS ARTICLE FORWARD LOOKING SENTENCE COUNT', None), ('NEWS ARTICLE TOTAL SENTENCE COUNT', None),
                         ('EDGAR PRESENT SENTENCE COUNT', None), ('EDGAR PAST SENTENCE COUNT', None),
                         ('EDGAR NUMERIC SENTENCE COUNT', None), ('EDGAR EARNINGS SENTENCE COUNT', None),
                         ('EDGAR FORWARD LOOKING SENTENCE COUNT', None), ('EDGAR TOTAL SENTENCE COUNT', None),
                         ('NEWS ARTICLE POSITIVE WORDS', None), ('NEWS ARTICLE NEGATIVE WORDS', None),
                         ('NEWS ARTICLE TOTAL WORDS', None), ('NEWS ARTICLE FOG', None),
                         ('EDGAR POSITIVE WORDS', None), ('EDGAR NEGATIVE WORDS', None),
                         ('EDGAR TOTAL WORDS', None), ('EDGAR FOG', None),])

                    identifying_fieldnames = article[1].split('_')

                    article_2_text = article_2.read()
                    soup = BeautifulSoup(article_2_text, "lxml")
                    #keep some tables -- they will be removed by the sentence parser if its pure numbers
                    #some filers put their entire presentation in tabular format
                    for table in soup.findAll('table'):
                        count_number = 0
                        text = table.get_text().strip()
                        for character in text:
                            if character.isnumeric():
                                count_number += 1
                        try:
                            if count_number / len(text) < .8:
                                table.decompose()
                        except ZeroDivisionError:
                            pass

                    article_2_text = soup.get_text()

                    #post processing -- guest 2018 -- remove short lines
                    article_2_pre_clean = article_2_text.split('\n')
                    article_2_post_clean = []
                    for line in article_2_pre_clean:
                        if len(line) > 19:
                            article_2_post_clean.append(line)
                    article_2_text = ''.join(article_2_post_clean)


                    article_1_pre_clean = article_1_text.split('\n')
                    article_1_post_clean = []
                    for line in article_1_pre_clean:
                        if len(line) > 19:
                            article_1_post_clean.append(line)
                    article_1_text = ''.join(article_1_post_clean)


                    article_2_sentences = parse_sentences(article_2_text)
                    article_1_sentences = parse_sentences(article_1_text)



                    ordered_fieldnames_1['GVKEY'] = identifying_fieldnames[1]
                    ordered_fieldnames_1['FDS'] = identifying_fieldnames[2]
                    ordered_fieldnames_1['CUSIP'] = identifying_fieldnames[3]
                    ordered_fieldnames_1['TICKER'] = identifying_fieldnames[4]
                    try:
                        ordered_fieldnames_1['NEWS ARTICLE PRESENT SENTENCE COUNT'] = detect_present_tense(article_1_sentences)
                    except:
                        ordered_fieldnames_1['NEWS ARTICLE PRESENT SENTENCE COUNT'] = '-99'
                    try:
                        ordered_fieldnames_1['EDGAR PRESENT SENTENCE COUNT'] = detect_present_tense(article_2_sentences)
                    except:
                        ordered_fieldnames_1['EDGAR PRESENT SENTENCE COUNT'] = '-99'
                    try:
                        ordered_fieldnames_1['NEWS ARTICLE PAST SENTENCE COUNT'] = detect_past_tense(article_1_sentences)
                    except:
                        ordered_fieldnames_1['NEWS ARTICLE PAST SENTENCE COUNT'] = '-99'
                    try:
                        ordered_fieldnames_1['EDGAR PAST SENTENCE COUNT'] = detect_past_tense(article_2_sentences)
                    except:
                        ordered_fieldnames_1['EDGAR PAST SENTENCE COUNT'] = '-99'
                    ordered_fieldnames_1['NEWS ARTICLE TOTAL SENTENCE COUNT'] = count_sentences(article_1_sentences)
                    ordered_fieldnames_1['EDGAR TOTAL SENTENCE COUNT'] = count_sentences(article_2_sentences)
                    ordered_fieldnames_1['NEWS ARTICLE FORWARD LOOKING SENTENCE COUNT'] = count_forward_looking_sentences(article_1_sentences)
                    ordered_fieldnames_1['EDGAR FORWARD LOOKING SENTENCE COUNT'] = count_forward_looking_sentences(article_2_sentences)
                    ordered_fieldnames_1['NEWS ARTICLE EARNINGS SENTENCE COUNT'] = count_earnings_sentences(article_1_sentences)
                    ordered_fieldnames_1['EDGAR EARNINGS SENTENCE COUNT'] = count_earnings_sentences(article_2_sentences)
                    ordered_fieldnames_1['NEWS ARTICLE NUMERIC SENTENCE COUNT'] = count_numeric_sentences(article_1_sentences)
                    ordered_fieldnames_1['EDGAR NUMERIC SENTENCE COUNT'] = count_numeric_sentences(article_1_sentences)
                    ordered_fieldnames_1['NEWS ARTICLE FOG'] = calculate_fog(article_1_sentences)
                    ordered_fieldnames_1['EDGAR FOG'] = calculate_fog(article_2_sentences)
                    ordered_fieldnames_1['NEWS ARTICLE POSITIVE WORDS'] = count_positive_words(article_1_sentences)
                    ordered_fieldnames_1['EDGAR POSITIVE WORDS'] = count_positive_words(article_2_sentences)
                    ordered_fieldnames_1['NEWS ARTICLE NEGATIVE WORDS'] = count_negative_words(article_1_sentences)
                    ordered_fieldnames_1['EDGAR NEGATIVE WORDS'] = count_negative_words(article_2_sentences)
                    ordered_fieldnames_1['NEWS ARTICLE TOTAL WORDS'] = count_words_in_document(article_1_sentences)
                    ordered_fieldnames_1['EDGAR TOTAL WORDS'] = count_words_in_document(article_2_sentences)
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
                    ordered_fieldnames_1['ACCEPTANCE-DATETIME'] = all_8k_headers[article_2_key]['ACCEPTANCE-DATETIME']
                    ordered_fieldnames_1['CENTRAL INDEX KEY'] = all_8k_headers[article_2_key]['CENTRAL INDEX KEY']
                    ordered_fieldnames_1['CITY'] = all_8k_headers[article_2_key]['CITY']
                    ordered_fieldnames_1['COMPANY CONFORMED NAME'] = all_8k_headers[article_2_key]['COMPANY CONFORMED NAME']
                    ordered_fieldnames_1['CONFORMED PERIOD OF REPORT'] = all_8k_headers[article_2_key]['CONFORMED PERIOD OF REPORT']
                    ordered_fieldnames_1['CONFORMED SUBMISSION TYPE'] = all_8k_headers[article_2_key]['CONFORMED SUBMISSION TYPE']
                    ordered_fieldnames_1['FILED AS OF DATE'] = all_8k_headers[article_2_key]['FILED AS OF DATE']
                    ordered_fieldnames_1['FILENAME'] = all_8k_headers[article_2_key]['FILENAME']
                    ordered_fieldnames_1['FILING YEAR'] = all_8k_headers[article_2_key]['FILING YEAR']
                    ordered_fieldnames_1['FISCAL YEAR END'] = all_8k_headers[article_2_key]['FISCAL YEAR END']
                    ordered_fieldnames_1['ITEM INFORMATION'] = all_8k_headers[article_2_key]['ITEM INFORMATION']
                    ordered_fieldnames_1['LINK'] = all_8k_headers[article_2_key]['LINK']
                    ordered_fieldnames_1['PUBLIC DOCUMENT COUNT'] = all_8k_headers[article_2_key]['PUBLIC DOCUMENT COUNT']
                    ordered_fieldnames_1['STANDARD INDUSTRIAL CLASSIFICATION'] = all_8k_headers[article_2_key]['STANDARD INDUSTRIAL CLASSIFICATION']
                    ordered_fieldnames_1['STATE'] = all_8k_headers[article_2_key]['STATE']
                    ordered_fieldnames_1['STATE OF INCORPORATION'] = all_8k_headers[article_2_key]['STATE OF INCORPORATION']

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
parsed_articles_root_dir = '/media/abc-123/EDGAR/8K_Output'
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
                                        print((d2-d1).total_seconds())
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
counter = 0
pool = mp.Pool(mp.cpu_count() - 1)
results = []

#uncomment this to test writting the output of a subset of the articles in multithreading
#articles_to_compare = articles_to_compare[1:100]


def worker(arg, q):
    '''main process'''
    res = find_articles(arg)
    q.put(res)
    return res

#write everything asyncronously
#The listener WILL SILENTLY CRASH. IF THERE IS NOTHING WRITTEN, THEN THE LISTENER CRASHED! BAD BAD BAD!
def listener(q):
    ''' listens for messages on the q'''
    counter = 1
    output_file = '/media/abc-123/EDGAR/simscore_after.csv'
    while os.path.isfile(output_file):
        output_file = os.path.join('/media/abc-123/EDGAR/simscore_' + str(counter) + '.csv')
        counter += 1
        ordered_fieldnames_headers = OrderedDict(
            [('ACCESSION NUMBER', None), ('GVKEY', None), ('FDS', None), ('CUSIP', None), ('TICKER', None),
             ('ARTICLE FILENAME', None), ('EXHIBIT FILENAME', None), ('SIMSCORE COSINE', None),
             ('SIMSCORE JACCARD', None), ('EXHIBIT NAME', None), ('INTERNAL ARTICLE FILENAME', None),
             ('INTERNAL EXHIBIT FILENAME', None), ('ARTICLE TIMESTAMP', None), ('EXHIBIT TIMESTAMP', None),
             ('TIMESTAMP DISTANCE', None), ('ACCEPTANCE-DATETIME', None), ('CENTRAL INDEX KEY', None),
             ('CITY', None), ('COMPANY CONFORMED NAME', None), ('CONFORMED PERIOD OF REPORT', None),
             ('CONFORMED SUBMISSION TYPE', None), ('FILED AS OF DATE', None), ('FILENAME', None), ('FILING YEAR', None),
             ('FISCAL YEAR END', None), ('ITEM INFORMATION', None), ('LINK', None), ('PUBLIC DOCUMENT COUNT', None),
             ('STANDARD INDUSTRIAL CLASSIFICATION', None), ('STATE', None), ('STATE OF INCORPORATION', None),
             ('NEWS ARTICLE PRESENT SENTENCE COUNT', None), ('NEWS ARTICLE PAST SENTENCE COUNT', None),
             ('NEWS ARTICLE NUMERIC SENTENCE COUNT', None), ('NEWS ARTICLE EARNINGS SENTENCE COUNT', None),
             ('NEWS ARTICLE FORWARD LOOKING SENTENCE COUNT', None), ('NEWS ARTICLE TOTAL SENTENCE COUNT', None),
             ('EDGAR PRESENT SENTENCE COUNT', None), ('EDGAR PAST SENTENCE COUNT', None),
             ('EDGAR NUMERIC SENTENCE COUNT', None), ('EDGAR EARNINGS SENTENCE COUNT', None),
             ('EDGAR FORWARD LOOKING SENTENCE COUNT', None), ('EDGAR TOTAL SENTENCE COUNT', None),
             ('NEWS ARTICLE POSITIVE WORDS', None), ('NEWS ARTICLE NEGATIVE WORDS', None),
             ('NEWS ARTICLE TOTAL WORDS', None), ('NEWS ARTICLE FOG', None),
             ('EDGAR POSITIVE WORDS', None), ('EDGAR NEGATIVE WORDS', None),
             ('EDGAR TOTAL WORDS', None), ('EDGAR FOG', None), ])

    with open(output_file, 'w', newline='', errors="ignore", encoding='UTF-8') as csv_l:
        writer = csv.DictWriter(csv_l, fieldnames=ordered_fieldnames_headers)
        writer.writeheader()
        #counter_output = 1
        while 1:
            #print('listening {}'.format(counter_output))
            #counter_output += 1
            #csvOutput = csv.writer(csv_l, quotechar='"', quoting=csv.QUOTE_MINIMAL)
            header_data = q.get()
            #print(header_data)
            if header_data == 'kill':
                break
            # if header_data returns a false value (ie, the vale does not conform to our expectations: ignore the value
            if header_data:
                #header_data returns a list of ordered dicts. Iterate through each one then write
                for l_val in header_data:
                    # print('first loop')
                    # print(l_val)
                    if l_val:
                        # print('writing')
                        writer.writerow(l_val)
                        #make ABSOLUTELY SURE that flushing to disk is done _immediately_ after a write command
                        #otherwise, the data will hang in memory and may cause the listener to hang
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


    #Use the filings list to construct the output

    #create a list of current jobs
    #fire off all the workers
    jobs = []
    for filing in tqdm(articles_to_compare):
        job = pool.apply_async(worker, (filing, q))
        jobs.append(job)

    #collec results from the workers through the pool result queue
    for job in tqdm(jobs):
        job.get()

    #we are done. kill the listener
    q.put('kill')
    pool.close()
    pool.join()

if __name__ == '__main__':
    main()
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
#         # article_1_location = os.path.join(articles_root, article[1])
#         # with open(article_1_location, 'r', errors='ignore') as article_1:
#         #     article_1_text = article_1.read()
#         #     article_2_key = article[2]
#         #     #make sure to cast the year as a string for the key, year is an integer when we do the range above
#         #     article_2_match = parsed_articles_dict[str(year)].get(article_2_key, None)
#         #     if article_2_match:
#         #         for article_2_candidate in article_2_match:
#         #             article_2_fields = article_2_candidate[0].split(sep='<>')
#         #             article_2_location = os.path.join(parsed_articles_root_dir+article_2_candidate[1])
#         #             with open(article_2_location, 'r', errors='ignore') as article_2:
#         #                 ordered_fieldnames = OrderedDict(
#         #                     [('ACCESSION NUMBER', None), ('ARTICLE FILENAME', None), ('EXHIBIT FILENAME', None), ('SIMSCORE COSINE', None),
#         #                      ('SIMSCORE JACCARD', None), ('EXHIBIT NAME', None), ('INTERNAL ARTICLE FILENAME', None),
#         #                      ('INTERNAL EXHIBIT FILENAME', None), ('ARTICLE TIMESTAMP', None), ('EXHIBIT TIMESTAMP', None),
#         #                      ('TIMESTAMP DISTANCE', None)])
#         #                 article_2_text = article_2.read()
#         #                 ordered_fieldnames['SIMSCORE COSINE'] = cosine_sim(article_1_text, article_2_text)
#         #                 ordered_fieldnames['SIMSCORE JACCARD'] = jaccard_sim(article_1_text, article_2_text)
#         #                 ordered_fieldnames['ACCESSION NUMBER'] = article_2_key
#         #                 ordered_fieldnames['EXHIBIT FILENAME'] = article_2_fields[1]
#         #                 ordered_fieldnames['ARTICLE FILENAME'] = article[1]
#         #                 ordered_fieldnames['EXHIBIT NAME'] = article_2_fields[2]
#         #                 ordered_fieldnames['INTERNAL EXHIBIT FILENAME'] = article_2_candidate[0]
#         #                 ordered_fieldnames['INTERNAL ARTICLE FILENAME'] = article[1]
#         #                 ordered_fieldnames['ARTICLE TIMESTAMP'] = article[3]
#         #                 ordered_fieldnames['EXHIBIT TIMESTAMP'] = article[4]
#         #                 ordered_fieldnames['TIMESTAMP DISTANCE'] = article[5]
#         with open(output_file, 'a', errors='ignore', newline='') as f:
#             writer = csv.DictWriter(f, fieldnames=ordered_fieldnames_t[0])
#             writer.writerow(ordered_fieldnames_t[0])


print('done')








