import os
import nltk, string
import re
nltk.download('cmudict')
nltk.download('punkt')
from nltk.tokenize import sent_tokenize
from nltk.corpus import cmudict
from nltk import word_tokenize, pos_tag
from nltk.parse import CoreNLPParser
from sklearn.feature_extraction.text import TfidfVectorizer
from itertools import islice

#move this exception to another file (exceptions file)
#import linguistic_error_handling

class Error(Exception):
    """Base class for exceptions in this module"""
    pass

class LabelError(Error):
    def __init__(self, label_value):
        self.label_value = label_value
    def __str__(self):
        return "Error in label. Key not found in label_dictionary. Value of label: {}".format(self.label_value)


#End error handling

tense_tags = {
                   'PAST':['VBD', 'VBN',],
                   'PRESENT':['VBP', 'VBZ', 'VBG',],
                   'FUTURE':['VBC', 'VBF',],
                   'MODAL':['MD',],
                   }


list_directory_root = '/media/abc-123/EDGAR'
d = cmudict.dict()

positive_word_list = []

quantitative_symbols = re.compile(r'[$%]')
quantitative_numbers = re.compile(r'\d\s*(K|M|B|MM)\b', re.I)
quantitative_words = re.compile(r'\b(cent|dollar|thousand|million|billion)\b', re.I)

regex_list = (quantitative_words, quantitative_symbols, quantitative_numbers)

#local preloading functions/tasks
nltk.download('punkt') # if necessary...


stemmer = nltk.stem.porter.PorterStemmer()
remove_punctuation_map = dict((ord(char), None) for char in string.punctuation)

stemming_dictionary = {}


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
        count += 1
    if count == 0:
        count +=1
    return count

def get_words(sentence):
    return sentence.split()
    #return word_tokenize(sentence)

def _detect_tense(tagged_words, tags):
    """
    Helper function for detect_sentence_tense
    :param tagged_words: pos tagged words
    :param tags: list of pos tags
    :return: True or False
    """
    if [word for word in tagged_words if word[1] in tags]:
        return True
    return False

def _detect_sentence_tense(sentence, tense, modal = False, nonmodal = False):
    """
    Detects the tense of a sentence
    :param sentence: A sentence of string type
    :param tense: 'PAST', 'PRESENT', 'FUTURE' -- Uses the tense_tag_dictionary variable
    :param modal: Boolean. If true, adds modal search
    :return: True or False
    """
    text = get_words(sentence)
    tagged_words = pos_tag(text)
    #if modal flag
    if modal:
        #if tense is present, check for both modal AND tense
        if tense:
            if _detect_tense(tagged_words, tense_tags[tense]):
                if _detect_tense(tagged_words, tense_tags['MODAL']):
                    return True
                else:
                    return False
        #otherwise just check for modal
        else:
            if _detect_tense(tagged_words, tense_tags['MODAL']):
                return True
            else:
                return False
    #same as above, but for nonmodal
    if nonmodal:
        if tense:
            if _detect_tense(tagged_words, tense_tags[tense]):
                if _detect_tense(tagged_words, tense_tags['MODAL']):
                    return False
                else:
                    return True
        else:
            if _detect_tense(tagged_words, tense_tags['MODAL']):
                return False
            else:
                return True

    else:
        if _detect_tense(tagged_words, tense_tags[tense]):
            return True
        return False

def count_words(sentence_list, word_list):
    """
    Determines if a word is in a word list
    Requires nltk get_words function
    :param sentence_list: List of sentences
    :param word_list: list of words to identify
    :return: Number of words in document that match the words contained in the wordlist
    """
    number_of_words = 0
    for sentence in sentence_list:
        for word in get_words(sentence):
            if word.upper().rstrip() in word_list:
                number_of_words += 1
    return number_of_words

def regex_search(sentence, regex_searches=None):
    for regex in regex_searches:
        if regex.search(sentence):
            return True
    return False



def wordlist_search(sentence: object, wordlists: object = None) -> object:
    wordlist_flag = True
    #construct a bad character translation table so we can QUICKLY and CORRECTLY identify words.
    bad_char = str.maketrans({'.':None, ',':None, '"':None, "'":None,'"':None, ';':None, ':':None})
    for wordlist in wordlists:
        #bug quashing!
        #wordlist flag check at the beginning is required -- otherwise we can FAIL, then PASS and count the sentence in
        #a multiple wordlist check. Subtle fencepost bug. This is an AND condition -- if a single wordlist fails, then
        #we do not need to check the remaining wordlists.
        if wordlist_flag:
            wordlist_flag = False
            #reset wordlist flag after each iteration -- if the word is found then it will be true at the end, no matter what
            for word in get_words(sentence):
                if word.upper().translate(bad_char) in wordlist:
                    wordlist_flag = True
                    break
    if wordlist_flag:
        return True
    return False

# #TODO
# #i need to make this a class
# def word_difference(text, *wordlist):
#     if len(wordlist) < 2:
#         raise Exception("word_difference requires at least two wordlists")
#     wordlist_flag = True
#     #construct a bad character translation table so we can QUICKLY and CORRECTLY identify words.
#     bad_char = str.maketrans({'.':None, ',':None, '"':None, "'":None,'"':None, ';':None, ':':None})



def determine_sentences(sentence_list, **kwargs):
    """
    Determines if a sentence contains a word in the wordlist
    Requires nltk get_words function
    :param sentence_list: List of sentences
    :param word_list: List of words to identify
    :return: Number of sentences that contain any word in the wordlist
    """
    wordlists = kwargs.get('wordlist')
    regex = kwargs.get('regex')
    number_of_sentences = 0
    wordlist_flag = True
    regex_flag = True
    for sentence in sentence_list:
        if wordlists:
            wordlist_flag = False
            if wordlist_search(sentence, wordlists=wordlists):
                wordlist_flag = True
        if regex:
            regex_flag = False
            if regex_search(sentence, regex_searches=regex):
                regex_flag = True
        if wordlist_flag and regex_flag:
            number_of_sentences += 1
    return number_of_sentences


def determine_sentence_negative_words(sentence):
    pass

def _count_words(sentence_list):
    """
    Internal function -- counts number of words in a sentence
    Requires nltk get_words function
    :param sentence_list: List of sentences
    :return: Number of words in the sentences (document)
    """
    number_of_words = 0
    for sentence in sentence_list:
        # word_list = get_words(sentence)
        word_list = sentence.split()
        number_of_words += len(word_list)
    return number_of_words

def parse_sentences(document):
    """
    Not used
    :param document:
    :return:
    """
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
    """
    Internal function. Calculates fog index for document
    Requires nltk get_words function
    :param sentences: List of sentences
    :return: Fog index
    """
    sentence_string = ''.join(sentences)
    total_words = len(get_words(sentence_string))
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



def _scan_tense_type(type):
    """
    Internal function that scans the data label for a tense word (_PAST_, _PRESENT_, _FUTURE_ [tenses]).
    Data label is used to determine which tense to search
    :param type: Data label. String
    :return: On Success: String that is used in the _detect_sentence_tense() function to determine which tense to search.
    On Failure: None
    """
    if '_PAST_' in type:
        return 'PAST'
    elif '_FUTURE_' in type:
        return 'FUTURE'
    elif '_PRESENT_' in type:
        return 'PRESENT'
    else:
        return None

def _scan_modal(type):
    """
    Internal function that scans the data label for the text _MODAL_.
    Uses the data label to determine if the data requests modal conditionals.
    :param type: Data label. String.
    :return: Boolean value that is used in the _detect_sentence_tense() function to determine if modal is searched.
    """
    if '_MODAL_' in type:
        return True
    else:
        return False

def _past_present_detection(sentence, modal=False, nonmodal=False):
    if _detect_sentence_tense(sentence, 'PAST'):
        if _detect_sentence_tense(sentence, 'PRESENT'):
            if modal:
                if _detect_sentence_tense(sentence, False, modal=modal):
                    return True
                else:
                    return False
            elif nonmodal:
                if _detect_sentence_tense(sentence, False, nonmodal=nonmodal):
                    return True
                else:
                    return False
            else:
                return True
        else:
            return False
    else:
        return False

def detect_sentence(sentences, type, modal = False, nonmodal = False, use_data_label_for_tense = False):
    """
    Flexible function that identifies the tense and modal quality of a sentence. Can use the data label for loops! Fails cleanly.
    If there is no tense found then nothing will be returned
    :param sentences: List of sentences
    :param type: Tense. String. Requires 'PAST', 'PRESENT', or 'FUTURE' or the Datalabel.
    If using datalabel, the datalabel must have the above tenses in the following form: '_PAST_', '_PRESENT_', or '_FUTURE_'
    :param modal: Default False. Override if not using the data label to identify the tense.
    :param use_data_label_for_tense: Default True. Set to False if you are not using the data label to identify the
    tense of the sentence
    :return: On Success: Tuple. Sentence count of tense -- On Failure: False
    """
    sentence_count = 0
    sentence_list = []
    if use_data_label_for_tense == True:
        tense = _scan_tense_type(type)
        modal = _scan_modal(type)
    else:
        tense = type

    #modal sentences only
    if tense is None and nonmodal:
        for sentence in sentences:
            if _detect_sentence_tense(sentence, tense, nonmodal=True):
                sentence_count += 1
                sentence_list.append(sentence)
        return (sentence_count, sentence_list)
    elif tense is None and modal:
        for sentence in sentences:
            if _detect_sentence_tense(sentence, False, modal=modal):
                sentence_count += 1
                sentence_list.append(sentence)
        return (sentence_count, sentence_list)
        #TODO: why is this here?
        return False

    #nonmodal past/present sentences

    #detect tense
    if 'PAST_PRESENT' in type:
        for sentence in sentences:
            if _past_present_detection(sentence, modal=modal, nonmodal=nonmodal):
                sentence_count += 1
                sentence_list.append(sentence)
        return (sentence_count, sentence_list)


    for sentence in sentences:
        if _detect_sentence_tense(sentence, tense, modal=modal, nonmodal=nonmodal):
            sentence_count += 1
            sentence_list.append(sentence)
    return (sentence_count, sentence_list)


#TODO:
#This needs to be a class
def calculate_fog(sentences):
    return _calculate_fog(sentences)

def count_positive_words(sentences):
    return count_words(sentences, positive_word_list)

def count_negative_words(sentences):
    return count_words(sentences, negative_word_list)

def count_words_in_document(sentences):
    return _count_words(sentences)

def count_wordlist_sentences(sentences, wordlist=None, regex=None):
    return determine_sentences(sentences, wordlist=wordlist, regex=regex)

def count_numeric_sentences(sentences, *wordlists):
    number_of_sentences = 0
    #(()) is false
    #((),) is true
    #the tuple passed creates the second situation
    if wordlists:
        wordlists, *dump = wordlists
    if wordlists:
        for sentence in sentences:
            wordlist_flag = False
            numeric_flag = False
            if wordlist_search(sentence, wordlists=wordlists):
                wordlist_flag = True
            for word in get_words(sentence):
                if any(ch.isdigit() for ch in word):
                    numeric_flag = True
                    break
            if wordlist_flag and numeric_flag:
                    number_of_sentences += 1
    else:
        for sentence in sentences:
            for word in get_words(sentence):
                if any(ch.isdigit() for ch in word):
                    number_of_sentences += 1
                    break
    return number_of_sentences


def stem_tokens(tokens):
    # ret_list = []
    # if tokens == [] or tokens == None or tokens == '':
    #     return []
    # for item in tokens:
    #     if stemming_dictionary.get(item, None):
    #         a = stemming_dictionary[item]
    #     else:
    #         a = stemmer.stem(item)
    #         stemming_dictionary[item] = a
    #     ret_list.append(a)
    #return ret_list
    return tokens

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
    jaccard = nltk.jaccard_distance(set(text1.split()), set(text2.split()))
    a = 1
    return jaccard

#TODO: Add ability to find complex sentence tenses. IE: past/present & modal

class branching(object):
    def __init__(self):
        #Cache for the sentences
        #prevents recalculating sentence counts unnecessarially.
        self.present_sentences = False
        self.past_sentences = False
        self.modal_sentences = False
        self.present_modal_sentences = False
        self.past_modal_sentences = False
        self.past_nonmodal_sentences = False
        self.nonmodal_sentences = False
        self.present_nonmodal_sentences = False
        self.past_present_sentences = False
        self.past_present_modal_sentences = False
        self.past_present_nonmodal_sentences = False
        self.label_dict = {
                1:['MODAL', self.past_modal_sentences],
                2:['NONMODAL', self.past_nonmodal_sentences],
                4:['PAST', self.past_sentences],
                5:['PAST', self.past_modal_sentences],
                6:['PAST', self.past_nonmodal_sentences],
                8:['PRESENT', self.present_sentences],
                9:['PRESENT', self.present_modal_sentences],
                10:['PRESENT', self.present_nonmodal_sentences],
                12:['PAST_PRESENT', self.past_present_sentences],
                13:['PAST_PRESENT', self.past_present_modal_sentences],
                14:['PAST_PRESENT', self.past_present_nonmodal_sentences],
            }

    def brancher(self, label, news_article_text, edgar_article_text, news_article_sentences, edgar_article_sentences, **kwargs):
        """
        The number 2 is impossible. https://wiki.c2.com/?ZeroOneInfinityRule. Traffic control for the labels. Directs the label to the correct
        function so we can loop through an arbitrary number of labels.
        :param label: data label
        :param input: function input
        :return: relevant value
        """
        if '_JACCARD' in label:
            jaccard_diff = jaccard_sim(news_article_text, edgar_article_text)
            return 1 - jaccard_diff
        elif '_COSINE' in label:
            return cosine_sim(news_article_text, edgar_article_text)
        #set the variables to the correct values given the label
        sentences = None
        text = None
        number_of_sentences = None
        if 'EDGAR_' in label:
            sentences = edgar_article_sentences
            text = edgar_article_text
        else:
            sentences = news_article_sentences
            text = news_article_text
        #document level measurements
        #make this a better class -- you need more study
        if '_FOG' in label:
            return calculate_fog(sentences)
        elif '_TOTAL_WORDS' in label:
            return count_words_in_document(sentences)
        elif '_NEGATIVE_WORDS' in label:
            return count_negative_words(sentences)
        elif '_POSITIVE_WORDS' in label:
            return count_positive_words(sentences)
        elif '_DOCUMENT_POS_WORDS' in label:
            document = ' '.join(sentences)
            return count_positive_words(document)
        elif '_DOCUMENT_NEG_WORDS' in label:
            document = ' ' .join(sentences)
            return count_negative_words(document)
        #sentence level measurements
        #TODO: Refactor this
        elif '_SENTENCE_' in label:
            #tense = _scan_tense_type(label)
            #modal = _scan_modal(label)
            #nonmodal = False

            #construct a binary counter to determine the tense 1-16
            #each sum maps one to one to a unique tense value
            tense_value = 0

            #if the term is in the label: add it to the counter
            if 'MODAL' in label:
                tense_value += 1
            if 'NONMODAL' in label:
                tense_value += 2
            if 'PAST' in label:
                tense_value += 4
            if 'PRESENT' in label:
                tense_value += 8

            #grab the unique value from the dictionary and automagically check the sentence
            label_items = self.label_dict.get(tense_value)

            #this is a bit ugly -- but it works (for now)
            if label_items is None:
                try:
                    pass
                except:
                    raise LabelError(label)
            # this is where the automagic happens
            else:
                tense_label = label_items[0]
                tense_sentences = label_items[1]
                if tense_sentences:
                    pass
                elif tense_value%4 == 0:
                    tense_sentences = detect_sentence(sentences, tense_label)
                    self.label_dict[tense_value][1] = tense_sentences
                elif tense_value%4 == 1:
                    tense_sentences = detect_sentence(sentences, tense_label, modal=True)
                    self.label_dict[tense_value][1] = tense_sentences
                elif tense_value%4 == 2:
                    tense_sentences = detect_sentence(sentences, tense_label, nonmodal=True)
                    self.label_dict[tense_value][1] = tense_sentences
                if tense_sentences:
                    sentences = tense_sentences[1]
                    number_of_sentences = tense_sentences[0]
                else:
                    return -99
            #
            #
            #
            #
            #
            #
            #
            # #basic model. Sentences can have multiple tenses! (complex sentences)
            # if modal:
            #     if 'PAST_PRESENT' in label:
            #         if not self.past_present_modal_sentences:
            #             self.past_present_modal_sentences = detect_sentence(sentences, label, modal=modal)
            #         sentences = self.past_present_modal_sentences[1]
            #         number_of_sentences = self.past_present_modal_sentences[0]
            #     elif tense == 'PAST':
            #         if not self.past_modal_sentences:
            #             self.past_modal_sentences = detect_sentence(sentences, label, modal=modal)
            #         sentences = self.past_modal_sentences[1]
            #         #print(sentences)
            #         number_of_sentences = self.past_modal_sentences[0]
            #     elif tense == 'PRESENT':
            #         if not self.present_modal_sentences:
            #             self.present_modal_sentences = detect_sentence(sentences, label, modal=modal)
            #         sentences = self.present_modal_sentences[1]
            #         #print(sentences)
            #         number_of_sentences = self.present_modal_sentences[0]
            #     else:
            #         if not self.modal_sentences:
            #             self.modal_sentences = detect_sentence(sentences, label, modal=modal)
            #         sentences = self.modal_sentences[1]
            #         #print(sentences)
            #         number_of_sentences = self.modal_sentences[0]
            # elif nonmodal:
            #     if 'PAST_PRESENT' in label:
            #         if not self.past_present_nonmodal_sentences:
            #             self.past_present_nonmodal_sentences = detect_sentence(sentences, label, nonmodal=nonmodal)
            #         sentences = self.past_present_nonmodal_sentences[1]
            #         number_of_sentences = self.past_present_nonmodal_sentences[0]
            #
            #     elif tense == 'PAST':
            #         if not self.past_nonmodal_sentences:
            #             self.past_nonmodal_sentences = detect_sentence(sentences, label, nonmodal=nonmodal)
            #         sentences = self.past_nonmodal_sentences[1]
            #         number_of_sentences = self.past_nonmodal_sentences[0]
            #     elif tense == 'PRESENT':
            #         if not self.present_nonmodal_sentences:
            #             self.present_nonmodal_sentences = detect_sentence(sentences, label, nonmodal=nonmodal)
            #         sentences = self.present_nonmodal_sentences[1]
            #         number_of_sentences = self.present_nonmodal_sentences[0]
            #     else:
            #         if not self.nonmodal_sentences:
            #             self.nonmodal_sentences = detect_sentence(sentences, label, nonmodal=nonmodal)
            #         sentences = self.nonmodal_sentences[1]
            #         number_of_sentences = self.nonmodal_sentences[0]
            # else:
            #     if 'PAST_PRESENT' in label:
            #         if not self.past_present_sentences:
            #             self.past_present_sentences = detect_sentence(sentences, label)
            #         sentences = self.past_present_sentences[1]
            #         number_of_sentences = self.past_present_sentences[0]
            #     elif tense == 'PAST':
            #         if not self.past_sentences:
            #             self.past_sentences = detect_sentence(sentences, label)
            #         sentences = self.past_sentences[1]
            #         number_of_sentences = self.past_sentences[0]
            #     elif tense == 'PRESENT':
            #         if not self.present_sentences:
            #             self.present_sentences = detect_sentence(sentences, label)
            #         sentences = self.present_sentences[1]
            #         number_of_sentences = self.present_sentences[0]

#TODO: Refactor this whole mess -- consolidate it into a single function
            wordlist = []
            regex = False
            if '_QUANTITATIVE_' in label:
                regex = True
            if '_EARNINGS_' in label:
                wordlist.append(earnings_word_list)
            if '_FORWARD_' in label:
                wordlist.append(forward_word_list)
            if '_POSITIVE_' in label:
                wordlist.append(positive_word_list)
            if '_NEGATIVE_' in label:
                wordlist.append(negative_word_list)
            if '_NUMERIC_' in label:
                return count_numeric_sentences(sentences, wordlist)
            elif regex:
                return count_wordlist_sentences(sentences, wordlist=wordlist, regex=regex_list)
            elif wordlist:
                return count_wordlist_sentences(sentences, wordlist=wordlist)
            elif '_TOTAL_' in label:
                return count_sentences(sentences)
            else:
                return number_of_sentences
        #default value
        else:
            return '-99'

#def skip_lines(file):
    



    # try:
    #     ordered_fieldnames_1['NEWS ARTICLE PRESENT SENTENCE COUNT'] = detect_present_tense(news_article_sentences)
    # except:
    #     ordered_fieldnames_1['NEWS ARTICLE PRESENT SENTENCE COUNT'] = '-99'
    # try:
    #     ordered_fieldnames_1['EDGAR PRESENT SENTENCE COUNT'] = detect_present_tense(edgar_article_sentences)
    # except:
    #     ordered_fieldnames_1['EDGAR PRESENT SENTENCE COUNT'] = '-99'
    # try:
    #     ordered_fieldnames_1['NEWS ARTICLE PAST SENTENCE COUNT'] = detect_past_tense(news_article_sentences)
    # except:
    #     ordered_fieldnames_1['NEWS ARTICLE PAST SENTENCE COUNT'] = '-99'
    # try:
    #     ordered_fieldnames_1['EDGAR PAST SENTENCE COUNT'] = detect_past_tense(edgar_article_sentences)
    # except:
    #     ordered_fieldnames_1['EDGAR PAST SENTENCE COUNT'] = '-99'
    # ordered_fieldnames_1['NEWS ARTICLE TOTAL SENTENCE COUNT'] = count_sentences(news_article_sentences)
    # ordered_fieldnames_1['EDGAR TOTAL SENTENCE COUNT'] = count_sentences(edgar_article_sentences)
    # ordered_fieldnames_1['NEWS ARTICLE FORWARD LOOKING SENTENCE COUNT'] = count_forward_looking_sentences(
    #     news_article_sentences)
    # ordered_fieldnames_1['EDGAR FORWARD LOOKING SENTENCE COUNT'] = count_forward_looking_sentences(
    #     edgar_article_sentences)
    # ordered_fieldnames_1['NEWS ARTICLE EARNINGS SENTENCE COUNT'] = count_earnings_sentences(news_article_sentences)
    # ordered_fieldnames_1['EDGAR EARNINGS SENTENCE COUNT'] = count_earnings_sentences(edgar_article_sentences)
    # ordered_fieldnames_1['NEWS ARTICLE NUMERIC SENTENCE COUNT'] = count_numeric_sentences(news_article_sentences)
    # ordered_fieldnames_1['EDGAR NUMERIC SENTENCE COUNT'] = count_numeric_sentences(news_article_sentences)
    # ordered_fieldnames_1['NEWS ARTICLE POSITIVE WORDS'] = count_positive_words(news_article_sentences)
    # ordered_fieldnames_1['EDGAR POSITIVE WORDS'] = count_positive_words(edgar_article_sentences)
    # ordered_fieldnames_1['NEWS ARTICLE NEGATIVE WORDS'] = count_negative_words(news_article_sentences)
    # ordered_fieldnames_1['EDGAR NEGATIVE WORDS'] = count_negative_words(edgar_article_sentences)
    # ordered_fieldnames_1['NEWS ARTICLE TOTAL WORDS'] = count_words_in_document(news_article_sentences)
    # ordered_fieldnames_1['EDGAR TOTAL WORDS'] = count_words_in_document(edgar_article_sentences)
    # 'ACCESSION_NUMBER',
    # 'GVKEY',
    # 'FDS',
    # 'CUSIP',
    # 'TICKER',
    # 'ARTICLE_FILENAME',
    # 'EXHIBIT_FILENAME',
    # 'SIMSCORE_COSINE',
    # 'SIMSCORE_JACCARD',
    # 'EXHIBIT_NAME',
    # 'INTERNAL_ARTICLE_FILENAME',
    # 'INTERNAL_EXHIBIT_FILENAME',
    # 'ARTICLE_TIMESTAMP',
    # 'EXHIBIT_TIMESTAMP',
    # 'TIMESTAMP_DISTANCE',
    # 'ACCEPTANCE-DATETIME',
    # 'CENTRAL_INDEX_KEY',
    # 'CITY',
    # 'COMPANY_CONFORMED_NAME',
    # 'CONFORMED_PERIOD_OF_REPORT',
    # 'CONFORMED_SUBMISSION_TYPE',
    # 'FILED_AS_OF_DATE',
    # 'FILENAME',
    # 'FILING_YEAR',
    # 'FISCAL_YEAR_END',
    # 'ITEM_INFORMATION',
    # 'LINK',
    # 'PUBLIC_DOCUMENT_COUNT',
    # 'STANDARD_INDUSTRIAL_CLASSIFICATION',
    # 'STATE',
    # 'STATE_OF_INCORPORATION',
    # 'NEWS_ARTICLE_PRESENT_SENTENCE_COUNT',
    # 'NEWS_ARTICLE_PAST_SENTENCE_COUNT',
    # 'NEWS_ARTICLE_OTHER_SENTENCE_COUNT',
    # 'NEWS_ARTICLE_NUMERIC_SENTENCE_COUNT',
    # 'NEWS_ARTICLE_NON_NUMERIC_SENTENCE_COUNT',
    # 'NEWS_ARTICLE_EARNINGS_SENTENCE_COUNT',
    # 'NEWS_ARTICLE_MODAL_SENTENCE_COUNT',
    # 'NEWS_ARTICLE_FUTURE_MODAL_SENTENCE_COUNT',
    # 'NEWS_ARTICLE_PAST_MODAL_SENTENCE_COUNT',
    # 'NEWS_ARTICLE_PAST_MODAL_SENTENCE_COUNT',
    # 'NEWS_ARTICLE_FUTURE_NONMODAL_SENTENCE_COUNT',
    # 'NEWS_ARTICLE_PRESENT_NONMODAL_SENTENCE_COUNT',
    # 'NEWS_ARTICLE_PAST_NONMODAL_SENTENCE_COUNT',
    # 'NEWS_ARTICLE_FORWARD_LOOKING_SENTENCE_COUNT',
    # 'NEWS_ARTICLE_NON_EARNINGS_SENTENCE_COUNT',
    # 'NEWS_ARTICLE_PAST_EARNINGS_SENTENCE_COUNT',
    # 'NEWS_ARTICLE_PRESENT_EARNINGS_SENTENCE_COUNT',
    # 'NEWS_ARTICLE_FORWARD_EARNINGS_SENTENCE_COUNT',
    # 'NEWS_ARTICLE_PAST_NUMERIC_SENTENCE_COUNT',
    # 'NEWS_ARTICLE_PRESENT_NUMERIC_SENTENCE_COUNT',
    # 'NEWS_ARTICLE_FORWARD_NUMERIC_SENTENCE_COUNT',
    # 'NEWS_ARTICLE_PAST_POSTIIVE_SENTENCE_COUNT',
    # 'NEWS_ARTICLE_PAST_NEGATIVE_SENTENCE_COUNT',
    # 'NEWS_ARTICLE_PAST_OTHER_SENTENCE_COUNT',
    # 'NEWS_ARTICLE_TOTAL_SENTENCE_COUNT',
    # 'EDGAR_PRESENT_SENTENCE_COUNT',
    # 'EDGAR_PAST_SENTENCE_COUNT',
    # 'EDGAR_OTHER_SENTENCE_COUNT',
    # 'EDGAR_NUMERIC_SENTENCE_COUNT',
    # 'EDGAR_NON_NUMERIC_SENTENCE_COUNT',
    # 'EDGAR_EARNINGS_SENTENCE_COUNT',
    # 'EDGAR_SENTENCE_MODAL_COUNT',
    # 'EDGAR_FUTURE_MODAL_SENTENCE_COUNT',
    # 'EDGAR_PAST_MODAL_SENTENCE_COUNT',
    # 'EDGAR_PAST_MODAL_SENTENCE_COUNT',
    # 'EDGAR_FUTURE_NONMODAL_SENTENCE_COUNT',
    # 'EDGAR_PRESENT_NONMODAL_SENTENCE_COUNT',
    # 'EDGAR_PAST_NONMODAL_SENTENCE_COUNT',
    # 'EDGAR_PAST_EARNINGS_SENTENCE_COUNT',
    # 'EDGAR_PRESENT_EARNINGS_SENTENCE_COUNT',
    # 'EDGAR_FORWARD_EARNINGS_COUNT',
    # 'EDGAR_PAST_NUMERIC_SENTENCE_COUNT',
    # 'EDGAR_PRESENT_NUMERIC_SENTENCE_COUNT',
    # 'EDGAR_FORWARD_NUMERIC_SENTENCE_COUNT',
    # 'EDGAR_PAST_POSTIIVE_SENTENCE_COUNT',
    # 'EDGAR_PAST_NEGATIVE_SENTENCE_COUNT',
    # 'EDGAR_PAST_OTHER_SENTENCE_COUNT',
    # 'EDGAR_FORWARD_LOOKING_SENTENCE_COUNT',
    # 'EDGAR_NON_EARNINGS_SENTENCE_COUNT',
    # 'EDGAR_TOTAL_SENTENCE_COUNT',
    # 'NEWS_ARTICLE_POSITIVE_WORDS',
    # 'NEWS_ARTICLE_NEGATIVE_WORDS',
    # 'NEWS_ARTICLE_OTHER_WORDS',
    # 'NEWS_ARTICLE_TOTAL_WORDS',
    # 'NEWS_ARTICLE_FOG',
    # 'EDGAR_POSITIVE_WORDS',
    # 'EDGAR_NEGATIVE_WORDS',
    # 'EDGAR_OTHER_WORDS',
    # 'EDGAR_TOTAL_WORDS',
    # 'EDGAR_FOG',
