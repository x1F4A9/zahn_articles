import re
import csv
import os
from collections import defaultdict


def output(text, filename, path):
    counter = 0
    if not os.path.isdir(path):
        os.mkdir(path)
    initial_filename = filename
    while os.path.isfile(os.path.join(path, filename+'.txt')):
        filename = '{}_{}'.format(initial_filename, str(counter))
        counter += 1
    with open(os.path.join(path , filename+'.txt',), 'w', errors='ignore') as f:
        f.write(text)


class TestSuite(object):
    def __init__(self):
        pass

    @staticmethod
    def scan_file(file_text):
        """
        Checks if the output file contains any rtf tag characters
        :param file_text: supposedly flat text file
        :return: Bool True/False
        """
        test = re.compile(r'\\|{|}')
        if type(file_text) is list:
            i = ' '.join(file_text)
        if type(file_text) is str:
            i = file_text
        if not test.search(i):
            return True


class FdsDict(dict):
    """
    custom class to extend standard python dictionary functionality
    creates an extend method to add identifying information for companies
    with multiple cusips
    """
    def extend(self, key, item):
        if self.get(key) == 0:
            self[key] = [item]
        else:
            self[key].extend([item])

    def get(self, item):
        return dict.get(self, item, 0)


class ParseRtf(object):
    def __init__(self, output_directory = None):
        """
        Main parsing class.
        Takes a raw rtf text snippet and cleans out the rtf formatting to create a flat text file
        :param output_directory: Specify the output directory when constructing the object.
                                 Path should be constructed with os.path.join
        """
        #identify all dates in factset date format
        self.dates = re.compile(r"([0-9]{1,2}) (January|February|March|April|May|June|July|August|September|October|November|December) (20[0-9]{2})")
        self.times = re.compile(r"([0-9]{2}):([0-9]{2})")
        self.output_directory = os.path.join(output_directory)
        self.files_output = defaultdict(int)
        self.cache = None

    def parse(self, rtf_text, filename, file = None):
        """
        Main entry point for class.
        Outputs file to location specified during the construction of the parse_rtf objet
        :param rtf_text: Text snippet
        :param filename: Base output filename of combined factset articles in rtf document
        :return: None -- Outputs to file
        """
        parsed_text = self._remove_tags(self._clean_url_field(self._create_newlines(rtf_text)))
        date = self._find_date(parsed_text)
        time = self._find_time(parsed_text)
        if date is None:
            #print('no date')
            return
        else:
            try:
                filename = filename+date+'_'+time
            except TypeError:
                print('halt')
            output(parsed_text, filename, self.output_directory)
            self.files_output[file] += 1

    def _parse_type_1_list(self, rtf_list, filename, file=None):
        """
        Parses a document without images
        :param rtf_list: List of RTF text
        :param filename: Base output filename of combined factset articles in rtf document
        :param file: OPTIONAL -- filename of rtf document for diagnostics
        :return: None -- Outputs to file
        """
        date = None
        time = None
        #find the first date only
        found_date = False
        found_time = False
        for line in rtf_list:
            parsed_text = self._remove_type_1_tags(self._clean_url_field(self._create_newlines(line)))
            #find the article date in the line
            date_t = self._find_date(parsed_text)
            time_t = self._find_time(parsed_text)
            if date_t and found_date == False:
                found_date = True
                date = date_t
            if time_t and found_time == False:
                found_time = True
                time = time_t
            #determine if we are at the end of the file, if not, store line in cache
            #we should use the original line
            if self._end_of_type_1_document(line):
                try:
                    filename_o = filename + date + '_' + time
                except TypeError:
                    #print('halt')
                    if time is None:
                        filename_o = filename + date + '_0000'
                self.cache += (parsed_text)
                try:
                    output(self.cache, filename_o, self.output_directory)
                except UnboundLocalError:
                    print('uhoh')
                self._clear_cache()
                self.files_output[file] += 1
                #update the datej flag
                found_date = False
            else:
                self._update_cache(parsed_text)
                continue

    def _parse_type_2_list(self, rtf_list, filename, file=None):
        """
        Parses a document with images
        :param rtf_list: List of RTF text
        :param filename: Base output filename of combined factset articles in rtf document
        :param file: OPTIONAL -- filename of rtf document for diagnostics
        :return: None -- Outputs to file
        """
        first_run = True
        date_l = None
        time_l = None
        for line in rtf_list:
            if self.identify_rtf_article(line):
                parsed_text = self._remove_tags(self._clean_url_field(self._create_newlines(line)))
                time = self._find_time(parsed_text)
                date = self._find_date(parsed_text)
                if date is None:
                    self._update_cache(parsed_text)
                    continue
                if first_run:
                    self.cache = parsed_text
                    first_run = False
                    date_l = date
                    time_l = time
                    continue
                try:
                    filename_o = filename+date_l+'_'+time_l
                except TypeError:
                    if time_l is None:
                        filename_o = filename + date_l + '_0000'
                    #print('halt')
                output(self.cache, filename_o, self.output_directory)
                date_l = date
                time_l = time
                self._clear_cache()
                self.cache = parsed_text
                self.files_output[file] += 1

    def parse_list(self, rtf_list, filename, file=None):
        """
        Main entry point for class.
        Parses a RTF file that is in a list format
        Outputs file to location specified during the construction of the parse_rtf object
        :param rtf_list: List of RTF text
        :param filename: Base output filename of combined factset articles in rtf document
        :param file: OPTIONAL -- filename of rtf document for diagnostics
        :return: None -- Outputs to file
        """
        #determine document type:
        if self._document_is_type_1(rtf_list):
            self._parse_type_1_list(rtf_list, filename, file)
        else:
            self._parse_type_2_list(rtf_list, filename, file)

    def _clear_cache(self):
        """
        Sets the class variable "cache" to None.
        :return: None
        """
        self.cache = None

    def _update_cache(self, line):
        """
        Updates class variable cache. Add to the cache the text contained in the "line" variable
        :param line: string
        :return: None -- Updates class variable cache
        """
        if self.cache is None:
            self.cache = line
        else:
            self.cache += (line)

    def _find_date(self, rtf_text):
        """
        Finds the first date with the prespecified FACTSET date format
        :param rtf_text: identified rtf text
        :return: STRING. Converts the date to YYYYMMDD format. That is, 1 January 2000 is converted to 20000101
        """
        date = self.dates.search(rtf_text)
        #check if the text snippet is an image container
        if date is None:
            return date
        month = self._convert_month(date.group(2))
        if len(date.group(1)) > 1:
            day = date.group(1)
        else:
            day = '0{}'.format(date.group(1))
        return '{}{}{}'.format(date.group(3), month, day)

    def _find_time(self, rtf_text):
        """
        Finds the timestamp for the news article
        :param rtf_text: identified rtf text
        :return: STRING -- Time in format HHMM
        """
        time = self.times.search(rtf_text)
        #find time
        if time is None:
            return time
        else:
            return '{}{}'.format(time.group(1), time.group(2))


    @staticmethod
    def _document_is_type_1(text):
        """
        Determines if a document contains images.
        Type 1 RTF documents do not contain images
        Type 2 RTF documents contain images
        Type 1 documents have a line that has the string "Document " at the beginning of some line
        :param text: A list of text
        :return: Bool. True if a document is type 1, False if document is type 2.
        """
        type_1 = re.compile('Document ')
        for line in text:
            if type_1.match(line):
                return True
        return False

    @staticmethod
    def _end_of_type_1_document(text):
        """
        Determines if a line is the end of a document
        All documents in FACTSET format have a line that begins with "Document "
        :param text: Single line from the document
        :return: Bool. True if end of document. False if not end of the document.
        """
        end_of_document = re.compile('Document ')
        if end_of_document.match(text):
            return True
        else:
            return False

    @staticmethod
    def _cache_not_empty(cache):
        """
        Determines if the cache is empty. Returns true if cache is empty
        :param cache: variable that contains cache
        :return: BOOL -- True if cache is empty
        """
        if cache is None:
            return False
        else:
            return True

    @staticmethod
    def _convert_month(month):
        """
        Converts text of month to a numeric format
        :param month: month text
        :return: STRING. Converts text to numeric format of month. That is, January is converted to 01.
        """
        month_dict = {'January': '01',
                      'February': '02',
                      'March': '03',
                      'April': '04',
                      'May': '05',
                      'June': '06',
                      'July': '07',
                      'August': '08',
                      'September': '09',
                      'October': '10',
                      'November': '11',
                      'December': '12',
                      }
        return month_dict[month]

    @staticmethod
    def _remove_type_1_tags(rtf_text):
        """
        Removes rtf tags from "type 1" rtf documents
        :param rtf_text: text of rtf document
        :return: STRING with rtf tags removed
        """
        brackets = re.compile(r"[{}]")
        headers = re.compile(r"\\f[2-6]")
        bold = re.compile(r"\\b[0-3]?")
        font = re.compile(r"\\fcharset0 .*?(?= |\\|;|\n);?")
        remainder = re.compile(r"\\.*?(?=\\| |;|\n);?")
        rtf_text = headers.sub('',rtf_text)
        rtf_text = bold.sub('',rtf_text)
        rtf_text = font.sub('',rtf_text)
        rtf_text = remainder.sub('',rtf_text)
        rtf_text = brackets.sub('', rtf_text)
        return rtf_text

    @staticmethod
    def _create_newlines(rtf_text):
        """
        Replaces \par tags with newline characters
        :param rtf_text: identified rtf text
        :return: STRING. \par tags REPLACED with newline characters
        """
        #regex to identify all par tags
        re_par = re.compile(r"(\\par)(?=[ \\])", re.DOTALL)
        return re_par.sub(r"\n", rtf_text)

    @staticmethod
    def _remove_tags(rtf_text):
        """
        Removes all RTF tags from text snippet
        :param rtf_text: identified rtf text
        :return: STRING with RTF tags REPLACED with empty strings
        """
        # remove all tags except the pars converted to newlines
        re_tag = re.compile(r"(\\.*?) ")
        re_tag_newline = re.compile(r"(\\.*?)(?=\n)")
        rtf_text = re_tag.sub(r"", rtf_text)
        #there are stragglers because of the newlines. We need two regular expressions
        return re_tag_newline.sub(r"", rtf_text)

    @staticmethod
    def _clean_url_field(rtf_text):
        """
        Extracts the text of a hyperlink field in a RTF document
        :param rtf_text: identified rtf document
        :return: STRING. Returns the TEXT that is displayed on the document for the hyperlink
        """
        #identify the hyperlink fields and extract the text
        hyperlink_field = re.compile(r"{\\field.*?}{\\fldrslt{\\cf2 \\uc2(?: )*?(.*?)}+", re.DOTALL)
        return hyperlink_field.sub(r"\1", rtf_text)

    @staticmethod
    def identify_rtf_article(line):
        """
        Identifies a RTF file from the FACTSET format
        FACTSET RTF documents BEGIN with a \par tag.
        The text of each document is contained on a single line
        Therefore -- the first four characters of the text will be \par
        All images are placed after the END of the text, so the textfile with images
        will be EMPTY -- we can delete these.
        :param line: Line of a file
        :return: Bool True if the first 4 characters are '\par' (RTF TEXT)
        """
        if r'\par' in line[0:4]:
            return True

    @staticmethod
    def remove_images(line):
        """
        Removes lines that contain only bytecode. Bytecode in rtf documents does not contain a space. If the line has
        a space, then return the line.
        :param line: STRING -- Line to inspect
        :return: STRING -- Returns the input value of the function if it is not bytecode.
        """
        if ' ' not in line:
            return False
        else:
            return line


class IdentifyFilename(object):
    def __init__(self, filenames_location):
        self.filename_dict = FdsDict()
        with open(os.path.join(filenames_location),'r', errors = 'ignore') as f:
            csv_dict = csv.DictReader(f)
            for row in csv_dict:
                self.filename_dict.extend(row['fds'],
                    {'tic':row['tic'],
                     'gvkey_chr':row['gvkey_chr'],
                     'conm':row['conm'],
                     'cusip':row['cusip'],
                     }
                )

    def construct_output_filename(self, filename):
        filename = filename[0:-4]
        parsed_filename = filename.split('_')
        if len(parsed_filename) == 1:
            t = self._construct_return_dictionary(parsed_filename[0], self.filename_dict)
        elif parsed_filename[1].isdigit():
            t = self._construct_return_dictionary(parsed_filename[0], self.filename_dict)
        else:
            t = self._construct_return_dictionary(parsed_filename[0], self.filename_dict, parsed_filename[1])
        return self._sanitize_filename(t)

    @staticmethod
    def _sanitize_filename(filename):
        return re.sub(r'[/\\]',r'',filename)

    @staticmethod
    def _construct_return_dictionary(key, dictionary, second_key=None):
        try:
            td = dictionary[key]
        except TypeError:
            print('halt')
        if second_key is None:
            td = td[0]
            return '{}_{}_{}_{}_{}_'.format(
                td['conm'],
                td['gvkey_chr'],
                key,
                td['cusip'],
                td['tic'],

            )
        else:
            for dict_list in td:
                try:
                    if dict_list['tic'] == second_key:
                        return '{}_{}_{}_{}_{}_'.format(
                            dict_list['conm'],
                            dict_list['gvkey_chr'],
                            key,
                            dict_list['cusip'],
                            dict_list['tic'],
                        )
                except TypeError:
                    print('ERROR')







