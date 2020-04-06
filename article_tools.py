#requires main function Link_To_Closest_News_Article.py
from collections import OrderedDict
from data_labels import label_headers

def return_blank_ordered_dictionary():
    blank_ordered_dictionary = OrderedDict()
    for item in label_headers:
        blank_ordered_dictionary[item] = '-99'
    return blank_ordered_dictionary

def public_doc_count_check(doc_count, value, exact_value = True):
    if exact_value:
        if int(doc_count) < value:
            return True
        if int(doc_count) > value:
            return True
    else:
        if int(doc_count) < value:
            return False

def remove_tables(soup_text, max_percent_numbes_in_tables):
    for table in soup_text.findAll('table'):
        count_number = 0
        text = table.get_text().strip()
        for character in text:
            if character.isnumeric():
                count_number += 1
        try:
            if count_number / len(text) > max_percent_numbes_in_tables:
                table.decompose()
        except ZeroDivisionError:
            pass
    return soup_text


def remove_short_lines(text, length):
    text = text.split('\n')
    post_text = []
    for line in text:
        if len(line) > length:
            post_text.append(line)
    return_text = ''.join(post_text)
    return return_text