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

import getopt
if __name__ == '__main__':
    # parser = argparse.ArgumentParser()
    # parser.add_argument('-n')
    # parser.add_argument('-c', action='count')
    # parser.add_argument('-s', action='count')
    # parser.add_argument('-p')
    # parser.add_argument('-a')
    # #parser.add_argument('-h')
    # print(parser)
    argumentList = sys.argv[1:]
    # python assignment2.py -n 5 -s -p 2 -h "localhost" -a 12 "312412"
    # Options
    options = "n:csp:h:a:"
    opts, args = getopt.getopt(argumentList, options)
    modes = 0
    for o, a in opts:
        if o == "-n":
            number_of_children = a
        elif  o == "-p":
            portnumber = a
        elif o == "-h":
            hosts = a
        elif o == "-a":
            number_of_articles = a
        elif o == '-s':
            mode = 'server'
            modes += 1
        elif o == '-c':
            mode = 'client'
            modes += 1
    if modes ==0 or modes >= 2:
        print('Error')
        sys.exit()

    print(f'number of childeren = {number_of_children}')
    print(f'Portnumber = {portnumber}')
    print(f'Hosts = {hosts}')
    print(f'Number of articals = {number_of_articles}')
    print(f"Mode = {mode}")
    print(f"Article to download = {args[0]}")
    #args = parser.parse_args()

    # user_id = str(sys.argv[1])
    # citations = get_citations(user_id)
    # citations = citations[:min(10, len(citations)-1)]
    # cpu_count = mp.cpu_count()
    # with mp.Pool(processes=cpu_count) as pool:
    #     pool.map(download_paper, citations)


