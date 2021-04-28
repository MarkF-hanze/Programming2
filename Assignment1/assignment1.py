from Bio import Entrez
import multiprocessing as mp
import xml.etree.cElementTree as ET
import sys

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


def download_paper(download_id):
    handle = Entrez.efetch(db='pubmed', id=download_id, retmode="xml")
    tree = ET.parse(handle)
    tree.write(f'output/{download_id}.xml')


if __name__ == '__main__':
    user_id = str(sys.argv[1])
    citations = get_citations(user_id)
    citations = citations[:min(10, len(citations)-1)]
    cpu_count = mp.cpu_count()
    with mp.Pool(processes=cpu_count) as pool:
        pool.map(download_paper, citations)


