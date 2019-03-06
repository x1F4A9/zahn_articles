import csv
import os, sys

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
articles_root = os.path.join('/home/pikakilla/Zahn/Output')

articles = os.listdir(articles_root)

#construct the list:

#k = gvkey, v = datetime group, article name pair
gv_article_dict = {}
#k = cusip, v = datetime group, article name pair
cusip_article_dict = {}
#k = gvkey, v = datetime group, article name, cusip triple
gv_cusip_article_dict = {}


def update_dict(dictionary, key, article_name, cusip=None):
    article = article_name.split(sep='_')
    # populate
    year = article[5][0:4]
    if dictionary.get(year, None):
        if dictionary[year].get(key, None):
            dictionary[year][key].append((str(article[5] + article[6][0:4] + '00'), article_name, cusip))
        else:
            dictionary[year].update({key: [(str(article[5] + article[6][0:4] + '00'), article_name, cusip)]})
    else:
        dictionary[year] = {key: [(str(article[5] + article[6][0:4] + '00'), article_name, cusip)]}
    return dictionary

for article_fn in articles:
    article_split = article_fn.split(sep='_')
    gvkey = article_split[1]
    cusip = article_split[3]
    #populate
    gv_article_dict = update_dict(gv_article_dict, gvkey, article_fn)
    cusip_article_dict = update_dict(cusip_article_dict, cusip, article_fn)
    gv_cusip_article_dict = update_dict(gv_cusip_article_dict, gvkey, article_fn, cusip=cusip)


#create linking dictionary
gv_cik_link = {}
cusip_cik_link = {}
gv_cusip_link = {}
with open('./Library/linking_table.csv', 'r', errors='ignore') as f:
    csv_file = csv.DictReader(f)
    for line in csv_file:
        gv_cik_link[line['gvkey']] = line.get('cik', None)
        cusip_cik_link[line['cusip']] = line.get('cik', None)
        gv_cusip_link[line['gvkey']] = line.get('cusip', None)

print('done')








