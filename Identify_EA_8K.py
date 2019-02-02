from bs4 import BeautifulSoup
import lxml
import nltk
nltk.download('wordnet')
nltk.download('stopwords')
en_stop = set(nltk.corpus.stopwords.words('english'))
from gensim import corpora
import gensim
import pickle

from nltk.corpus import wordnet as wn
from nltk.stem.wordnet import WordNetLemmatizer
import random
import os
import re
import gc
from tqdm import tqdm
from itertools import islice
from collections import defaultdict

text_data = []

def get_lemma(word):
    lemma = wn.morphy(word)
    if lemma is None:
        return word
    else:
        return lemma

def get_lemma2(word):
    return WordNetLemmatizer().lemmatize(word)

def prepare_text_for_lda(text):
    tokens = text.split()
    tokens = [token for token in tokens if len(token) > 4]
    tokens = [token for token in tokens if token not in en_stop]
    tokens = [get_lemma(token) for token in tokens]
    return tokens

#run 1 filing path
#filing_path = os.path.join('E:\\','ADAMS','8-K')
#run 2 filing path
filing_path = os.path.join('C:\\''Zahn','RUN_1','Not_Earnings')
#this is the wrong way. dont do this
good_words = ['revenue','earning','announcement','income','announce','result','results','quarter','sales','quarterly','fiscal']
bad_words = ['forecast','expect','anticipate','foreclosure','director','resign','agreement','acquisition','acquire','incentive','IRS','examinations','distribution','call'
             'teleconference','speaker','speakers','status','extension','Catastrophe','board','repurchase','dividend'
             'conference']
years = os.listdir(filing_path)
#
# for year in years:
#     # if year != '2010':
#     #     continue
#     print(year)
#     filings = os.listdir(os.path.join(filing_path, year))
#     for filing in tqdm(filings):
#         if random.random() > .97:
#             with open(os.path.join(filing_path,year,filing),'r',errors='ignore') as f:
#                 #soup = BeautifulSoup(f.read(),'lxml')
#
#                 #filing_text_lines = filing_text.split('\n')
#
#                 filing_text_lines = islice(f, 1000)
#                 text = ''
#                 for line in filing_text_lines:
#                     text += ''.join(line)
#                 soup = BeautifulSoup(text, 'lxml')
#                 text = soup.get_text()
#                 text_data.append(prepare_text_for_lda(text))
#                 del soup
#                 del text
#                 gc.collect()
#
# dictionary = corpora.Dictionary(text_data)
# corpus = [dictionary.doc2bow(text) for text in text_data]
#
# del text_data
# gc.collect()
#
# pickle.dump(corpus, open('zahn_lda_run_2_not_earnings.pk1','wb'))
# dictionary.save('dictionary_run_2_not_earnings.gensim')

#construct model
# dictionary = gensim.corpora.Dictionary.load('dictionary_run_2_not_earnings.gensim')
# corpus = pickle.load(open('zahn_lda_run_2_not_earnings.pk1','rb'))
# NUM_TOPICS = 5
# ldamodel_5 = gensim.models.ldamodel.LdaModel(corpus, num_topics = NUM_TOPICS,
#                                            id2word=dictionary, passes=20)
# ldamodel_5.save('model5_zahn_run_2_not_earnings.gensim')
# print('model 5 saved')
# NUM_TOPICS = 10
# ldamodel_10 = gensim.models.ldamodel.LdaModel(corpus, num_topics = NUM_TOPICS,
#                                            id2word=dictionary, passes=20)
# ldamodel_10.save('model10_zahn_run_2_not_earnings.gensim')
# print('model 10 saved')
#
# topics_5 = ldamodel_5.print_topics(num_words=4)
# topics_10 = ldamodel_10.print_topics(num_words=4)
#
# #verify model accuracy
# for topic in topics_5:
#     print(topic)
#
# for topic in topics_10:
#    print(topic)


#######################################
#run model through year and classify each document
#######################################

# dictionary = gensim.corpora.Dictionary.load('dictionary_run_2_not_earnings.gensim')
# ldamodel_10 = gensim.models.ldamodel.LdaModel.load('model10_zahn_run_2_not_earnings.gensim')
# ldamodel_5 = gensim.models.ldamodel.LdaModel.load('model5_zahn_run_2_not_earnings.gensim')
# corpus = pickle.load(open('zahn_lda.pk1','rb'))
#
# topics_5 = ldamodel_5.print_topics(num_words=10)
# topics_10 = ldamodel_10.print_topics(num_words=10)
#
# for topic in topics_5:
#     print(topic)
#
# for topic in topics_10:
#    print(topic)


def write(file, outpath, filename):
    file.seek(0)
    os.makedirs(outpath, exist_ok=True)
    with open(os.path.join(outpath, filename), 'w', errors='ignore') as g:
        g.write(file.read())

# for year in years:
#     print(year)
#     filings = os.listdir(os.path.join(filing_path, year))
#     for filing in filings:
#         with open(os.path.join(filing_path, year, filing)) as f:
#             filing_text_lines = islice(f, 1000)
#             text = ''
#             for line in filing_text_lines:
#                 text += ''.join(line)
#             soup = BeautifulSoup(text, 'lxml')
#             text = soup.get_text()
#             text_data = prepare_text_for_lda(text)
#             new_text_data = dictionary.doc2bow(text_data)
#             topic_dict = {}
#             for topic, value in ldamodel_10.get_document_topics(new_text_data):
#                 topic_dict[topic] = value
#
#             if max(topic_dict, key=topic_dict.get) == 0:
#                 f.seek(0)
#                 write(f.read(), os.path.join('C:\\', 'Zahn', 'RUN_2', 'Earnings', year), filing)
#             elif max(topic_dict, key=topic_dict.get) == 2:
#                 f.seek(0)
#                 write(f.read(), os.path.join('C:\\', 'Zahn', 'RUN_2', 'Maybe_Earnings', year), filing)
#             else:
#                 f.seek(0)
#                 write(f.read(), os.path.join('C:\\', 'Zahn', 'RUN_2', 'Not_Earnings', year), filing)

#########################
#regular expression search:

expression = re.compile(r'(?:reports?|announces?) (?:first|second|third|fourth)?\s*(?:quarter|year end|annual)?\s*(?:20[0-9][0-9])?\s*(?:first|second|third|fourth)?\s*(?:quarter|year end|annual)?\s*(?:revenue|results?|earning|eps|sales)',re.I)
expression_2 = re.compile(r'(?:reports?|announces?) (?:[0-9]{1,3}\.?[0-9]?%?)\s*(?:increase|decrease)', re.I)

for year in years:
    print(year)
    filings = os.listdir(os.path.join(filing_path, year))
    for filing in filings:
        with open(os.path.join(filing_path, year, filing)) as f:
            filing_text_lines = islice(f, 100)
            text = ''
            for line in filing_text_lines:
                text += ''.join(line)
            soup = BeautifulSoup(text, 'lxml')
            text = soup.get_text()

            if expression.search(text):
                if re.search('(?:operator|moderator|shareholder vote)', text):
                    write(f, os.path.join('C:\\', 'Zahn', 'RUN_1', 'Regex_Search_Not_Earnings', year), filing)
                else:
                    write(f, os.path.join('C:\\', 'Zahn', 'RUN_1', 'Regex_Search_Earnings', year), filing)
            elif expression_2.search(text):
                if re.search('(?:operator|moderator|shareholder vote)', text):
                    write(f, os.path.join('C:\\', 'Zahn', 'RUN_1', 'Regex_Search_Not_Earnings', year), filing)
                else:
                    write(f, os.path.join('C:\\', 'Zahn', 'RUN_1', 'Regex_Search_Earnings', year), filing)
            else:
                write(f, os.path.join('C:\\', 'Zahn', 'RUN_1', 'Regex_Search_Not_Earnings', year), filing)

#determine topics for 8-K

    #         line_count = 0
    #         good_word_count = 0
    #         bad_word_count = 0
    #         word_dict = defaultdict(int)
    #         word_count = 0
    #         text = ''
    #         for line in filing_text_lines:
    #              text += ''.join(line)
    #         soup = BeautifulSoup(text, 'lxml')
    #         soup_text = soup.get_text()
    #
    #         for word in soup_text.split():
    #             word_dict[word.lower()] += 1
    #             word_count += 1
    #             # done = False
    #             # word_count += 1
    #             # for good_word in words:
    #             #     if good_word in word.lower():
    #             #         good_word_count += 1
    #             #         done = True
    #             #         break
    #             # if done == True:
    #             #     continue
    #             # else:
    #             #     for bad_word in bad_words:
    #             #         if bad_word in word.lower():
    #             #             bad_word_count += 1
    #             #             done = True
    #             #             break
    #             # if done == True:
    #             #     continue
    #         for word in bad_words:
    #             bad_word_count += word_dict.get(word, 0)
    #         for word in good_words:
    #             good_word_count += word_dict.get(word, 0)
    #         if bad_word_count >= good_word_count:
    #             continue
    #         if (bad_word_count/word_count) > .075:
    #             continue
    #         if (good_word_count/word_count) > .01:
    #             f.seek(0)
    #             with open(os.path.join('E:\\','Zahn_Test',filing),'w',errors='ignore') as g:
    #                 g.write(f.read())
    # print(counter)