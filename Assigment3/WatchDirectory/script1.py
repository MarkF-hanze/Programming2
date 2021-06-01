import numpy as np
from Bio import Entrez
import xml.etree.cElementTree as ET
import sys
from collections import defaultdict

Entrez.api_key = 'b1d6e58f788c2d50eef028a909f4971d9508'
Entrez.email = 'mark.leendert@gmail.com'

def get_citations(original_id):
    """
    Returns the pmids of the papers this paper cites
    """
    handle = Entrez.efetch(db='pubmed', id=original_id, retmode='xml')
    records = Entrez.read(handle)
    reference_id = []
    if len(records['PubmedArticle'][0]['PubmedData']['ReferenceList']) > 0:
        for citation in records['PubmedArticle'][0]['PubmedData']['ReferenceList'][0]['Reference']:
            if 'ArticleIdList' in citation:
                reference_id.append(str(citation['ArticleIdList'][0]))
    return reference_id



class Data:
    def __init__(self, id):
        self.data = get_citations(id)
        #self.data = [download_abstract(x) for x in data]
        self.current = -1

    def __iter__(self):
        return self

    def __next__(self):
        self.current += 1
        if self.current < len(self.data):
            return self.data[self.current]
        raise StopIteration


def download_abstract(download_id):
    handle = Entrez.efetch(db='pubmed', id=download_id, retmode="xml")
    records = Entrez.read(handle)
    abstract = records['PubmedArticle'][0]['MedlineCitation']['Article']['Abstract']['AbstractText'][0]
    return abstract


def mapper(in_key, in_value):
    abstract = download_abstract(in_key)
    return [(word, in_value) for word in abstract.split(' ')]


def reducer(out_key, intermediate_value):
    return out_key, sum(intermediate_value)

def shuffling(map_list):
    word_count = defaultdict(list)
    for key, value in map_list:
        word_count[key].append(value)
    return word_count

data = Data('32651208')
for id in data:
    result = shuffling(mapper(id, 1))
    for output in result:
        print(reducer(output, result[output]))
    sys.exit()

