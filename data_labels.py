#edit all data labels for program here. If additional data labels are necessary -- add them here
import config
from itertools import combinations, product


#
# psdudo code:
# take N lists
# for each element in the list:
# match the value with each other value one by one. That is -- there should be one element per list at most

def _create_text_labels(prefix, suffix, valid_args):
    text_items = list(product(*valid_args))
    return_items = []
    for item in text_items:
        text_list =[prefix,"_".join(item),suffix]
        text = "_".join(i for i in text_list)
        return_items.append(text)
    pass
    return return_items


def _construct_labels(prefix, suffix, combination, *args):
    valid_args = []
    for i in combination:
        valid_args.append(args[0][i])
    return _create_text_labels(prefix, suffix, valid_args)
    #["{}_{}_{}".format(prefix, i, suffix)]


def construct_labels(prefix, suffix, *args):
    list_combinations = []
    labels_return = []
    for l in range(1, len(args)+1):
        for subset in combinations(list(range(0, len(args))), l):
            list_combinations.append((subset))
    for combination in list_combinations:
        labels_return += _construct_labels(prefix, suffix, combination, args)
    return labels_return

#add additional headers here
label_headers = [
    'ACCESSION NUMBER',
    'GVKEY',
    'FDS',
    'CUSIP',
    'TICKER',
    'ARTICLE_FILENAME',
    'EXHIBIT_FILENAME',
    'SIMSCORE_COSINE',
    'SIMSCORE_JACCARD',
    'EXHIBIT_NAME',
    'INTERNAL_ARTICLE_FILENAME',
    'INTERNAL_EXHIBIT_FILENAME',
    'ARTICLE_TIMESTAMP',
    'EXHIBIT_TIMESTAMP',
    'TIMESTAMP_DISTANCE',
    'ACCEPTANCE-DATETIME',
    'CENTRAL INDEX KEY',
    'CITY',
    'COMPANY CONFORMED NAME',
    'CONFORMED PERIOD OF REPORT',
    'CONFORMED SUBMISSION TYPE',
    'FILED AS OF DATE',
    'FILENAME',
    'FILING YEAR',
    'FISCAL YEAR END',
    'ITEM INFORMATION',
    'LINK',
    'PUBLIC DOCUMENT COUNT',
    'STANDARD INDUSTRIAL CLASSIFICATION',
    'STATE',
    'STATE OF INCORPORATION',
    'NEWS_ARTICLE_POSITIVE_WORDS',
    'NEWS_ARTICLE_NEGATIVE_WORDS',
    'NEWS_ARTICLE_OTHER_WORDS',
    'NEWS_ARTICLE_TOTAL_WORDS',
    'NEWS_ARTICLE_DOCUMENT_POS_WORDS',
    'NEWS_ARTICLE_DOCUMENT_NEG_WORDS',
    'NEWS_ARTICLE_FOG',
    'EDGAR_POSITIVE_WORDS',
    'EDGAR_NEGATIVE_WORDS',
    'EDGAR_OTHER_WORDS',
    'EDGAR_TOTAL_WORDS',
    'EDGAR_DOCUMENT_POS_WORDS',
    'EDGAR_DOCUMENT_NEG_WORDS',
    'EDGAR_FOG',
    'WSJ_ARTICLE',
    'REUTERS_ARTICLE',
    'DJI_ARTICLE',
    'ARTICLE_MENTIONS_EA',
    'ARTICLE_MENTIONS_QUARTER',
    'EDGAR_TOTAL_SENTENCE_COUNT',
    'NEWS_ARTICLE_TOTAL_SENTENCE_COUNT',]


article_filename_keys = [
    'YEAR',
    'NEWS_ARTICLE_FILENAME',
    'EDGAR_ARTICLE_KEY',
    'ARTICLE_TIMESTAMP',
    'EXHIBIT_TIMESTAMP',
    'TIMESTAMP_DISTANCE',
    'EDGAR_YEAR',]


news_article_keys = [
    'COMPANY_NAME_NEWS_FILENAME',
    'GVKEY',
    'FDS',
    'CUSIP',
    'TICKER',]


edgar_header_keys = [
    'ACCESSION NUMBER',
    'ACCEPTANCE-DATETIME',
    'CENTRAL INDEX KEY',
    'CITY',
    'COMPANY CONFORMED NAME',
    'CONFORMED PERIOD OF REPORT',
    'CONFORMED SUBMISSION TYPE',
    'FILED AS OF DATE',
    'FILENAME',
    'FILING YEAR',
    'FISCAL YEAR END',
    'ITEM INFORMATION',
    'LINK',
    'PUBLIC DOCUMENT COUNT',
    'STANDARD INDUSTRIAL CLASSIFICATION',
    'STATE',
    'STATE OF INCORPORATION', ]

label_headers = label_headers + construct_labels('NEWS_ARTICLE','SENTENCE_COUNT', config.tense_labels,
                                                 config.tone_labels, config.forward_labels, config.earnings_labels,
                                                 config.numeric_labels, config.quantitative_labels)

label_headers = label_headers + construct_labels('EDGAR', 'SENTENCE_COUNT', config.tense_labels,
                                                 config.tone_labels, config.forward_labels, config.earnings_labels,
                                                 config.numeric_labels, config.quantitative_labels)

a = 1