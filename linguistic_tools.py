import os
import nltk, string
nltk.download('cmudict')
nltk.download('punkt')
from nltk.tokenize import sent_tokenize
from nltk.corpus import cmudict
from nltk import word_tokenize, pos_tag
from sklearn.feature_extraction.text import TfidfVectorizer

tense_tags = {
                   'PAST':['VBD', 'VBN',],
                   'PRESENT':['VBP', 'VBZ', 'VBG',],
                   'FUTURE':['VBC', 'VBF',],
                   'MODAL':['MD',],
                   }


list_directory_root = '/media/abc-123/EDGAR'
d = cmudict.dict()

positive_word_list = []


#data labels -- add all labels here ONLY


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

def get_words(sentence):
    return word_tokenize(sentence)

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

def _detect_sentence_tense(sentence, tense, modal = False):
    """
    Detects the tense of a sentence
    :param sentence: A sentence of string type
    :param tense: 'PAST', 'PRESENT', 'FUTURE' -- Uses the tense_tag_dictionary variable
    :param modal: Boolean. If true, adds modal search
    :return: True or False
    """
    text = get_words(sentence)
    tagged_words = pos_tag(text)
    if modal:
        if _detect_tense(tagged_words, tense_tags[tense]):
            if _detect_tense(tagged_words, tense_tags['MODAL']):
                return True
        return False
    else:
        if _detect_tense(tagged_words, tense_tags[tense]):
            return True
        return False

def determine_words(sentence_list, word_list):
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

def determine_sentences(sentence_list, word_list):
    """
    Determines if a sentence contains a word in the wordlist
    Requires nltk get_words function
    :param sentence_list: List of sentences
    :param word_list: List of words to identify
    :return: Number of sentences that contain any word in the wordlist
    """
    number_of_sentences = 0
    for sentence in sentence_list:
        for word in get_words(sentence):
            if word.upper().rstrip() in word_list:
                number_of_sentences += 1
                break
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
        word_list = get_words(sentence)
        number_of_words += len(word_list)
    return number_of_words

def parse_sentences(document):
    """
    Unsure if used
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
    :return: On Success: String that is used in the _detect_sentence_tense() function to determine which tense to search for.
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

def detect_sentence(sentences, type, modal = False, use_data_label_for_tense = True):
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
    if tense is None:
        return False
    for sentence in sentences:
        if _detect_sentence_tense(sentence, tense, modal=modal):
            sentence_count += 1
            sentence_list.append(sentence)
    return (sentence_count, sentence_list)

def calculate_fog(sentences):
    return _calculate_fog(sentences)

def count_positive_words(sentences):
    return determine_words(sentences, positive_word_list)

def count_negative_words(sentences):
    return determine_words(sentences, negative_word_list)

def count_earnings_sentences(sentences):
    return determine_sentences(sentences, earnings_word_list)

def count_positive_sentences(sentences):
    return determine_sentences(sentences, positive_word_list)

def count_negative_sentences(sentences):
    return determine_sentences(sentences, negative_word_list)

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



nltk.download('punkt') # if necessary...


stemmer = nltk.stem.porter.PorterStemmer()
remove_punctuation_map = dict((ord(char), None) for char in string.punctuation)

stemming_dictionary = {}

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
    jaccard = nltk.jaccard_distance(set(text1), set(text2))
    return jaccard

class branching(object):
    def __init__(self):
        self.future_sentences = False
        self.present_sentences = False
        self.past_sentences = False
        self.modal_sentences = False
        self.future_modal_sentences = False
        self.present_modal_sentences = False
        self.past_modal_sentences = False

    def brancher(self, label, news_article_text, edgar_article_text, news_article_sentences, edgar_article_sentences, **kwargs):
        """
        The number 2 is impossible. https://wiki.c2.com/?ZeroOneInfinityRule. Traffic control for the labels. Directs the label to the correct
        function so we can loop through an arbitrary number of labels.
        :param label: data label
        :param input: function input
        :return: relevant value
        """
        if '_JACCARD' in label:
            return jaccard_sim(news_article_text, edgar_article_text)
        elif '_COSINE' in label:
            return cosine_sim(news_article_text, edgar_article_text)
        #set the variables to the correct values given the label
        sentences = None
        text = None
        if 'EDGAR_' in label:
            sentences = edgar_article_sentences
            text = edgar_article_text
        else:
            sentences = news_article_sentences
            text = news_article_text
        #document level measurements
        if '_FOG' in label:
            return calculate_fog(sentences)
        elif '_TOTAL_WORDS' in label:
            return count_words_in_document(sentences)
        elif '_NEGATIVE_WORDS' in label:
            return count_negative_words(sentences)
        elif '_POSITIVE_WORDS' in label:
            return count_positive_words(sentences)
        #sentence level measurements
        elif '_SENTENCE_' in label:
            tense = _scan_tense_type(label)
            modal = _scan_modal(label)
            if modal:
                if tense == 'PAST':
                    if not self.past_modal_sentences:
                        self.past_modal_sentences = detect_sentence(sentences, label)
                elif tense == 'PRESENT':
                    if not self.present_modal_sentences:
                        self.present_modal_sentences = detect_sentence(sentences, label)
                elif tense == 'FUTURE':
                    if not self.future_modal_sentences:
                        self.future_modal_sentences = detect_sentence(sentences, label)
            else:
                if tense == 'PAST':
                    if not self.past_sentences:
                        self.past_sentences = detect_sentence(sentences, label)
                    if not self.present_sentences:
                        self.present_sentences = detect_sentence(sentences, label)
                    if not self.future_sentences:
                        self.future_sentences = detect_sentence(sentences, label)

            tense_return_values_tuple = detect_sentence(sentences, label)
            if tense_return_values_tuple:
                sentences = tense_return_values_tuple[1]
                number_of_sentences = tense_return_values_tuple[0]
                if '_EARNINGS_' in label:
                    return count_earnings_sentences(sentences)
                elif '_POSITIVE_' in label:
                    return count_positive_sentences(sentences)
                elif '_NEGATIVE_' in label:
                    return count_negative_sentences(sentences)
                else:
                    return number_of_sentences
        #default value
        else:
            return '-99'





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
