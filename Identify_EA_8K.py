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

filing_path = os.path.join('E:\\','ADAMS','8-K')
good_words = ['revenue','earning','announcement','income','announce','result','results','quarter','sales','quarterly','fiscal']
bad_words = ['forecast','expect','anticipate','foreclosure','director','resign','agreement','acquisition','acquire','incentive','IRS','examinations','distribution','call'
             'teleconference','speaker','speakers','status','extension','Catastrophe','board','repurchase','dividend'
             'conference']
years = os.listdir(filing_path)

# for year in years:
#     if year != '2010':
#         continue
#     print(year)
#     filings = os.listdir(os.path.join(filing_path,year))
#     counter = 0
#     for filing in tqdm(filings):
#
#         with open(os.path.join(filing_path,year,filing),'r',errors='ignore') as f:
#             #soup = BeautifulSoup(f.read(),'lxml')
#
#             #filing_text_lines = filing_text.split('\n')
#             if random.random() > .75:
#                 filing_text_lines = islice(f, 200)
#                 text = ''
#                 for line in filing_text_lines:
#                     text += ''.join(line)
#                 soup = BeautifulSoup(text, 'lxml')
#                 text = soup.get_text()
#                 text_data.append(prepare_text_for_lda(text))
# dictionary = corpora.Dictionary(text_data)
# corpus = [dictionary.doc2bow(text) for text in text_data]
#
# pickle.dump(corpus, open('zahn_lda.pk1','wb'))
# dictionary.save('dictionary.gensim')

#construct model
# dictionary = gensim.corpora.Dictionary.load('dictionary.gensim')
# corpus = pickle.load(open('zahn_lda.pk1','rb'))
# NUM_TOPICS = 5
# ldamodel_5 = gensim.models.ldamodel.LdaModel(corpus, num_topics = NUM_TOPICS,
#                                            id2word=dictionary, passes=20)
# ldamodel_5.save('model5_zahn.gensim')
# print('model 5 saved')
# NUM_TOPICS = 10
# ldamodel_10 = gensim.models.ldamodel.LdaModel(corpus, num_topics = NUM_TOPICS,
#                                            id2word=dictionary, passes=20)
# ldamodel_10.save('model10_zahn.gensim')
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
    print(topic)

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