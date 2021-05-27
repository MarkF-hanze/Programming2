import multiprocessing as mp
from multiprocessing.managers import BaseManager, SyncManager
import sys, time, queue
import numpy as np
import paramiko
import getopt
import ast
import time
import socket

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
  time.sleep(0.1)
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
        timer = TimeOutTimer(300)
        while True:
            try:
                results.update_results(shared_result_q)
                print(f'got results {results.get_results()[-1]}')
                timer.reset_time()
                if len(results.get_results()) == len(data):
                    print("Got all results!")
                    break
            except queue.Empty:
                if timer.over_time_out():
                    print('To long no result exiting')
                    break
                time.sleep(1)
                continue
        # Tell the client process no more data will be forthcoming
        shared_job_q.put(self.poison_pill.get_pill())
        # Sleep a bit before shutting down the server - to give clients time to
        # realize the job queue is empty and exit in an orderly way.
        time.sleep(5)
        print("Aaaaaand we're done for the server!")
        self.manager.shutdown()


class WorkerFactory(object):
    def __init__(self, server_ip, server_port):
        self.server_ip = server_ip
        self.server_port = server_port
        self.all_worker = []

    def setup_ssh(self, ips):
        for ip in ips:
            print(f'Setting up SSH {ip}')
            ssh = paramiko.SSHClient()
            self.all_worker.append(ssh)
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            ssh.connect(ip)
            print(f'setup_worker to connect to {self.server_ip} on {self.server_port}')
            ssh.exec_command(f'/homes/mlfrederiks/p1venv/bin/python /homes/mlfrederiks/PycharmProjects/Programming2/Assigment3/Assignment3.py  -w {ip} -h {[self.server_ip]} -p {self.server_port} -n {cores}')
            print('Done')

    def setup_worker(self, num_processes, worker_ip):
        worker = Worker(self.server_ip, self.server_port, num_processes, worker_ip)

# Hoe start je dit op een andere computer
class Worker(object):
    def __init__(self, ip, port, num_processes, worker_ip):
        self.worker_ip = worker_ip
        self.message_manager = MessageManager('c')
        key = Authkey()
        self.manager = ServerQueueManager(address=(ip, port), authkey=key.get_key())
        while True:
          try:
            self.manager.connect()
            break
          except ConnectionRefusedError:
            pass
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
        timer = TimeOutTimer(60)
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
                        timer.reset_time()
                        print("Peon %s Workwork on %s!" % (my_name, job['arg']))
                        result_q.put({'job': job, 'result': result, 'worker': self.worker_ip})
                    except NameError:
                        print("Can't find yer fun Bob!")
                        result_q.put({'job': job, 'result': 'F'})
            except queue.Empty:
                if timer.over_time_out():
                    return
                print("sleepytime for", my_name)
                time.sleep(1)


class TimeOutTimer(object):
    def __init__(self, max_time):
        self.max_time = max_time
        self.start_time = time.time()

    def over_time_out(self):
        return (time.time() - self.start_time) > self.max_time

    def reset_time(self):
        self.start_time = time.time()


class WatchDirectoy(object):
    def __init__(self, directory):
        self.directory = directory




def string_to_list(a_string_list):
    a_string_list = a_string_list.replace('[', '["')
    a_string_list = a_string_list.replace(']', '"]')
    a_string_list = a_string_list.replace(',', '","')
    return ast.literal_eval(a_string_list)

if __name__ == '__main__':
    options = "mw:h:p:n:"
    argumentList = sys.argv[1:]
    opts, args = getopt.getopt(argumentList, options)
    mode = None
    for o, a in opts:
        if o == '-m':
            mode = 'server'
        elif o == '-w':
            mode = 'worker'
            ip_worker = a
        elif o == '-h':
            all_ips = string_to_list(a)
            server_ip = all_ips[0]
            worker_ips = all_ips[:]
            worker_ips.remove(server_ip)
        elif o == '-p':
            server_port = int(a)
        elif o == '-n':
            cores = int(a)

    worker_factory = WorkerFactory(server_ip, server_port)
    if mode == 'server':
        worker_factory.setup_ssh(worker_ips)
        server = Server(server_ip, server_port)
    elif mode == 'worker':
        worker_factory.setup_worker(cores, ip_worker)
    else:
        # Start SSH of server
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(server_ip)
        all_ips = str(all_ips).replace(' ', '')
        stdin, stdout, stderr = ssh.exec_command(f'/homes/mlfrederiks/p1venv/bin/python /homes/mlfrederiks/PycharmProjects/Programming2/Assigment3/Assignment3.py -m -h {all_ips} -p {server_port} -n {cores}')
        # Waarom werkt het alleen, met dit sluit die anders het script?
        for line in iter(stdout.readline, ""):
            print(line, end="")
        for line in iter(stderr.readline, ""):
            print(line, end="")
#TODo timeout to server and worker
