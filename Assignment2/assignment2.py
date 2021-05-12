#!/usr/bin/envs python3
from Bio import Entrez
import xml.etree.cElementTree as ET
import getopt

import multiprocessing as mp
from multiprocessing.managers import BaseManager, SyncManager
import sys, time, queue

POISONPILL = "MEMENTOMORI"
ERROR = "DOH"
IP = ''
AUTHKEY = b'whathasitgotinitspocketsesss?'

def make_server_manager(port, authkey):
    """ Create a manager for the server, listening on the given port.
        Return a manager object with get_job_q and get_result_q methods.
    """
    job_q = queue.Queue()
    result_q = queue.Queue()

    # This is based on the examples in the official docs of multiprocessing.
    # get_{job|result}_q return synchronized proxies for the actual Queue
    # objects.
    class QueueManager(BaseManager):
        pass

    QueueManager.register('get_job_q', callable=lambda: job_q)
    QueueManager.register('get_result_q', callable=lambda: result_q)

    manager = QueueManager(address=('', port), authkey=authkey)
    manager.start()
    print('Server started at port %s' % port)
    return manager


def runserver(fn, data, portnum):
    # Start a shared manager server and access its queues
    manager = make_server_manager(portnum, b'whathasitgotinitspocketsesss?')
    shared_job_q = manager.get_job_q()
    shared_result_q = manager.get_result_q()

    if not data:
        print("Gimme something to do here!")
        return

    print("Sending data!")
    for d in data:
        shared_job_q.put({'fn': fn, 'arg': d})

    time.sleep(2)

    results = []
    while True:
        try:
            result = shared_result_q.get_nowait()
            results.append(result)
            print("Got result!", result)
            if len(results) == len(data):
                print("Got all results!")
                break
        except queue.Empty:
            time.sleep(1)
            continue
    # Tell the client process no more data will be forthcoming
    print("Time to kill some peons!")
    shared_job_q.put(POISONPILL)
    # Sleep a bit before shutting down the server - to give clients time to
    # realize the job queue is empty and exit in an orderly way.
    time.sleep(5)
    print("Aaaaaand we're done for the server!")
    manager.shutdown()
    print(results)


def make_client_manager(ip, port, authkey):
    """ Create a manager for a client. This manager connects to a server on the
        given address and exposes the get_job_q and get_result_q methods for
        accessing the shared queues from the server.
        Return a manager object.
    """
    class ServerQueueManager(BaseManager):
        pass

    ServerQueueManager.register('get_job_q')
    ServerQueueManager.register('get_result_q')

    manager = ServerQueueManager(address=(ip, port), authkey=authkey)
    manager.connect()

    print('Client connected to %s:%s' % (ip, port))
    return manager


def runclient(num_processes, portnum):
    manager = make_client_manager(IP, portnum, AUTHKEY)
    job_q = manager.get_job_q()
    result_q = manager.get_result_q()
    run_workers(job_q, result_q, num_processes)


def run_workers(job_q, result_q, num_processes):
    processes = []
    for p in range(num_processes):
        temP = mp.Process(target=peon, args=(job_q, result_q))
        processes.append(temP)
        temP.start()
    print("Started %s workers!" % len(processes))
    for temP in processes:
        temP.join()


def peon(job_q, result_q):
    my_name = mp.current_process().name
    while True:
        try:
            job = job_q.get_nowait()
            if job == POISONPILL:
                job_q.put(POISONPILL)
                print("Aaaaaaargh", my_name)
                return
            else:
                try:
                    result = job['fn'](job['arg'])
                    print("Peon %s Workwork on %s!" % (my_name, job['arg']))
                    result_q.put({'job': job, 'result': result})
                except NameError:
                    print("Can't find yer fun Bob!")
                    result_q.put({'job': job, 'result': ERROR})

        except queue.Empty:
            print("sleepytime for", my_name)
            time.sleep(1)




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
    # python3 assignment2.py -n 1 -s -p 5381 -h "localhost" -a 20 "30797674"
    # python3 assignment2.py -n 1 -c -p 5381 -h "localhost" -a 20 "30797674"
    # Options
    options = "n:csp:h:a:"
    opts, args = getopt.getopt(argumentList, options)
    modes = 0
    for o, a in opts:
        if o == "-n":
            number_of_children = int(a)
        elif  o == "-p":
            PORTNUM = int(a)
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
    print(f'Portnumber = {PORTNUM}')
    print(f'Hosts = {hosts}')
    print(f'Number of articals = {number_of_articles}')
    print(f"Mode = {mode}")
    print(f"Article to download = {args[0]}")
    if mode == 'server':
        data = get_citations(args[0])
        data = data[:max(number_of_articles, len(data))]
        server = mp.Process(target=runserver, args=(download_paper, data, PORTNUM))
        server.start()
        time.sleep(1)
        server.join()
    if mode == 'client':
        client = mp.Process(target=runclient, args=(number_of_children, PORTNUM))
        client.start()
        time.sleep(1)
        client.join()
