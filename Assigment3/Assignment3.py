import multiprocessing as mp
from multiprocessing.managers import BaseManager, SyncManager
import sys, time, queue
import numpy as np

class QueueManager(BaseManager):
    pass

class ServerQueueManager(BaseManager):
    pass

class MessageManager(object):
    def __init__(self):
        self._pools = ['get_job_q', 'get_result_q']
        self._make_message_pool()

    # TODO kijken of dit dynamisch kan
    def _make_message_pool(self):
        for pool in self._pools:
            q = queue.Queue()
            QueueManager.register(pool, callable=lambda: q)
            ServerQueueManager.register(pool)


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

class Server(object):
    def __init__(self, port, ip):
        self.manager = QueueManager(address=(ip, port))
        self.manager.start()
        print('Starting server')
        self.message_manager = MessageManager()
        self.runserver()
        self.poison_pill = PoisonPill()

    def runserver(self):
        # Start a shared manager server and access its queues
        # TODO dit kan niet 2x
        shared_job_q = self.manager.get_job_q()
        shared_result_q = self.manager.get_result_q()
        #TODO tijdelijk
        fn = lambda x: x**2
        data = np.arange(0, 1000)
        for d in data:
            shared_job_q.put({'fn': fn, 'arg': d})
        time.sleep(2)
        results = Results()
        while True:
            try:
                results.update_results(shared_result_q)
                if len(results.get_results()) == len(data):
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

# Hoe start je dit op een andere computer
class Worker(object):
    def __init__(self, ip, port, num_processes):
        self.manager = ServerQueueManager(address=(ip, port))
        self.manager.connect()
        print('Client connected to %s:%s' % (ip, port))
        self.poison_pill = PoisonPill()
        self.run_workers(num_processes)

    def run_workers(self, num_processes):
        job_q = self.manager.get_job_q()
        result_q = self.manager.get_result_q()
        processes = []
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

test = Server()


