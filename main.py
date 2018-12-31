import re
import csv
import os
import sys

class FdsDict(dict):
    '''
    custom class to extend standard python dictionary functionality
    creates an extend method to add identifying information for companies
    with multiple cusips
    '''
    def extend(self, key, item):
        if dict.get(key, 0) == 0:
            self[key] = [item]
        else:
            self[key].extend([item])

    def get(self, item):
        return dict.get(self, item, 0.0)



def identify_rtf_article(line):
    '''
    Identifies a RTF file from the FACTSET format
    FACTSET RTF documents BEGIN with a \par tag.
    The text of each document is contained on a single line
    Therefore -- the first four characters of the text will be \par
    All images are placed after the END of the text, so the textfile with images
    will be EMPTY -- we can delete these.
    :param line: Line of a file
    :return: Bool True if the first 4 characters are '\par' (RTF TEXT)
    '''
    if '\par' in line[0:3]:
        return True

def output(text, filename):
    with open(os.path.join)


class parse_rtf(object):
    def __init__(self, output_directory = None):
        #regex to identify all par tags
        self.re_par = re.compile(r"(\\par)[ \\]")
        #remove all tags except the pars converted to newlines
        self.re_tag = re.compile(r"(?!\\n)(\\.*?) ")
        #identify all dates in factset date format
        self.dates = re.compile(r"([0-9]{1,2}) (January|Feburary|March|April|May|June|July|August|September|October|November|December) (20[0-9]{2}) \\n")
        #identify the hyperlink fields and extract the text
        self.hyperlink_field = re.compile(r"{\\field.*?}{\\fldrslt{\\cf2 \\uc2(?: )*?(.*?)}+")
        self.output_directory = output_directory

    def parse(self, rtf_text):
        parsed_text = self._clean_url_field(self._create_newlines(rtf_text))
        date = self._find_date(parsed_text)



    def _create_newlines(self):

    def _find_date(self):

    def _clean_url_field(self):


class identify_filename(object):
    def __init__(self, filenames_location):
        with open(os.path.join(filenames_location),'r', errors = ignore) as f:
            csv_dict = csv.DictReader(f.read())
        self.filename_dict = FdsDict()
        for row in csv_dict:
            self.filename_dict[row['fds']].extend(
                {'tic':row['tic'],
                 'gvkey_chr':row['gvkey_chr'],
                 'coname':row['coname'],
                 'cusip':row['cusip']}
            )

    def identify_cusip(parsed_filename):


