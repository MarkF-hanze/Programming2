import multiprocessing as mp
from multiprocessing.managers import BaseManager, SyncManager
import sys, time, queue
import numpy as np

#TODO results saven hoe????
#TODO heartbeat queue toevoegen
#TODO core reserveren voor deze hearbeat

class QueueManager(BaseManager):
    pass

class ServerQueueManager(BaseManager):
    pass

class MessageManager(object):
    # TODO vragen hoe dit beter kan
    def __init__(self, mode):
        if mode == 's':
          get_job_q = queue.Queue()
          get_result_q = queue.Queue()
          QueueManager.register('get_job_q', callable=lambda: get_job_q)
          QueueManager.register('get_result_q', callable=lambda: get_result_q)
        elif mode == 'c':
          ServerQueueManager.register('get_job_q')
          ServerQueueManager.register('get_result_q')


class Results(object):
    def __init__(self):
        self.results = []

    def update_results(self, queue):
        self.results.append(queue.get_nowait())

    def get_results(self):
        return self.results


class PoisonPill(object):
    def __init__(self):
        self.poisonpill = "MEMENTOMORI"
    def get_pill(self):
        return self.poisonpill
        
class Authkey(object):
    def __init__(self):
        self.authkey = b"lasejrtli3qjrlk3241"
    def get_key(self):
        return self.authkey
        
def fn(x):
  time.sleep(1)
  return x**2
  
class Server(object):
    def __init__(self, ip, port):
        self.message_manager = MessageManager('s')
        key = Authkey()
        self.manager = QueueManager(address=(ip, port), authkey=key.get_key())
        self.manager.start()
        print('Server started at port %s' % port)
        self.poison_pill = PoisonPill()
        self.runserver()

    def runserver(self):
        # Start a shared manager server and access its queues
        # TODO dit kan niet 2x
        shared_job_q = self.manager.get_job_q()
        shared_result_q = self.manager.get_result_q()
        #TODO tijdelijk

        data = np.arange(0, 1000)
        for d in data:
            shared_job_q.put({'fn': fn, 'arg': d})
        results = Results()
        time.sleep(2)
        while True:
            try:
                results.update_results(shared_result_q)
                if len(results.get_results()) == len(data):
                    print(results.get_results())
                    print("Got all results!")
                    break
            except queue.Empty:
                time.sleep(1)
                continue
        # Tell the client process no more data will be forthcoming
        shared_job_q.put(self.poison_pill.get_pill())
        # Sleep a bit before shutting down the server - to give clients time to
        # realize the job queue is empty and exit in an orderly way.
        time.sleep(5)
        print("Aaaaaand we're done for the server!")
        self.manager.shutdown()

#TODO
# Hoe start je dit op een andere computer
class Worker(object):
    def __init__(self, ip, port, num_processes):
        self.message_manager = MessageManager('c')
        key = Authkey()
        self.manager = ServerQueueManager(address=(ip, port), authkey=key.get_key())
        self.manager.connect()
        print('Client connected to %s:%s' % (ip, port))
        self.poison_pill = PoisonPill()
        self.run_workers(num_processes)

    def run_workers(self, num_processes):
        job_q = self.manager.get_job_q()
        result_q = self.manager.get_result_q()
        processes = []
        # TODO hier -1 van maken en 1 core vrijhouden
        for p in range(num_processes):
            temP = mp.Process(target=self.peon, args=(job_q, result_q))
            processes.append(temP)
            temP.start()
        print("Started %s workers!" % len(processes))
        for temP in processes:
            temP.join()


    def peon(self, job_q, result_q):
        my_name = mp.current_process().name
        while True:
            try:
                job = job_q.get_nowait()
                if job == self.poison_pill.get_pill():
                    job_q.put(self.poison_pill.get_pill())
                    print("Aaaaaaargh", my_name)
                    return
                else:
                    try:
                        result = job['fn'](job['arg'])
                        print("Peon %s Workwork on %s!" % (my_name, job['arg']))
                        result_q.put({'job': job, 'result': result})
                    except NameError:
                        print("Can't find yer fun Bob!")
                        result_q.put({'job': job, 'result': 'F'})

            except queue.Empty:
                print("sleepytime for", my_name)
                time.sleep(1)

#test = Server('localhost', 3412)
test = Worker('localhost', 3412, 10)

