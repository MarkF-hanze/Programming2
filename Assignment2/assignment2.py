from Bio import Entrez
import multiprocessing as mp
import xml.etree.cElementTree as ET
import sys

import time
import getopt
import Server
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
    argumentList = sys.argv[1:]
    # python3 assignment2.py -n 5 -s -p 2 -h "localhost" -a 12 "30797674"
    # Options
    options = "n:csp:h:a:"
    opts, args = getopt.getopt(argumentList, options)
    modes = 0
    for o, a in opts:
        if o == "-n":
            number_of_children = int(a)
        elif  o == "-p":
            Server.PORTNUM =int(a)
        elif o == "-h":
            hosts = str(a)
        elif o == "-a":
            number_of_articles = int(a)
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
    print(f'Portnumber = {Server.PORTNUM}')
    print(f'Hosts = {hosts}')
    print(f'Number of articals = {number_of_articles}')
    print(f"Mode = {mode}")
    print(f"Article to download = {args[0]}")
    data = get_citations(args[0])
    print(data)
    data = data[:max(number_of_articles, len(data))]
    if mode == 'server':
        server = mp.Process(target=Server.runserver, args=(download_paper, data))
        server.start()
        print(3)
        time.sleep(1)
#        server.join()
    if mode == 'client':
        client = mp.Process(target=Server.runclient, args=(number_of_children,))
        client.start()
        time.sleep(1)
        client.join()
