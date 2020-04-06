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

label_headers = [
        'ACCESSION_NUMBER',
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
        'CENTRAL_INDEX_KEY',
        'CITY',
        'COMPANY_CONFORMED_NAME',
        'CONFORMED_PERIOD_OF_REPORT',
        'CONFORMED_SUBMISSION_TYPE',
        'FILED_AS_OF_DATE',
        'FILENAME',
        'FILING_YEAR',
        'FISCAL_YEAR_END',
        'ITEM_INFORMATION',
        'LINK',
        'PUBLIC_DOCUMENT_COUNT',
        'STANDARD_INDUSTRIAL_CLASSIFICATION',
        'STATE',
        'STATE_OF_INCORPORATION',
        'EDGAR_TOTAL_SENTENCE_COUNT',
        'NEWS_ARTICLE_POSITIVE_WORDS',
        'NEWS_ARTICLE_NEGATIVE_WORDS',
        'NEWS_ARTICLE_OTHER_WORDS',
        'NEWS_ARTICLE_TOTAL_WORDS',
        'NEWS_ARTICLE_FOG',
        'EDGAR_POSITIVE_WORDS',
        'EDGAR_NEGATIVE_WORDS',
        'EDGAR_OTHER_WORDS',
        'EDGAR_TOTAL_WORDS',
        'EDGAR_FOG',
]

label_headers = label_headers + construct_labels('NEWS_ARTICLE','SENTENCE_COUNT', config.tense_labels,
                                                 config.tone_labels, config.forward_labels, config.earnings_labels,
                                                 config.numeric_labels, config.quantitative_labels)

a = 1