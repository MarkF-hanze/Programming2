import multiprocessing as mp
from multiprocessing.managers import BaseManager, SyncManager
import sys, time, queue
import numpy as np
import paramiko
import getopt
import ast
import time
import socket
import os
import importlib
from WatchDirectory import script1
from collections import defaultdict


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
            get_active_task_q = queue.Queue()
            QueueManager.register('get_job_q', callable=lambda: get_job_q)
            QueueManager.register('get_result_q', callable=lambda: get_result_q)
            QueueManager.register('get_active_task_q', callable=lambda: get_active_task_q)
        elif mode == 'c':
            ServerQueueManager.register('get_job_q')
            ServerQueueManager.register('get_result_q')
            ServerQueueManager.register('get_active_task_q')


class Results(object):
    def __init__(self):
        self.results = []

    def update_results(self, result):
        self.results.append(result)

    def get_results(self):
        return self.results

    def remove_results(self, result):
        self.results.remove(result)


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


# def shuffling(self, map_list):
#      word_count = defaultdict(list)
#      for key, value in map_list:
#          word_count[key].append(value)
#      return word_count


class Server(object):
    def __init__(self, ip, port, worker_ips):
        self.working_jobs = KeepWorkingJobs(worker_ips)
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
        shared_active_task_q = self.manager.get_active_task_q()
        # TODO tijdelijk
        # TODO task facotry maken met dit erin
        ID_to_func = {}
        unique_num = 0
        for d in script1.Data('32651208'):
            unique_num += 1
            ID_to_func[unique_num] = (script1.mapper, d)
            shared_job_q.put({'fn': script1.mapper, 'arg': d, 'ID': unique_num, 'type': 'mapper'})
        to_get_results = unique_num
        results = Results()
        time.sleep(2)
        timer = TimeOutTimer(30)
        result_count = 0
        to_get_results_2 = None
        map_count = 0
        intermediate__results = defaultdict(list)
        while True:
            # A job got taken!
            while not shared_active_task_q.empty():
                taken_job = shared_active_task_q.get_nowait()
                self.working_jobs.add_job(taken_job['worker'], taken_job['job']['ID'])
                #print(self.working_jobs.get_active_jobs())
                # print(f"Gave job {taken_job['job']['ID']} to worker {taken_job['worker']}")
                # print(f'gave {gave_count}')
            # A job got returned!
            while not shared_result_q.empty():
                curent_results = shared_result_q.get_nowait()
                self.working_jobs.remove_job(curent_results['worker'], curent_results['job']['ID'])
                #print(self.working_jobs.get_active_jobs())
                # Shuffling 
                if curent_results['job']['type'] == 'mapper':
                    # TODO dit niet hardcoden
                    if curent_results['result'] != 'Error404':
                        for result in curent_results['result']:
                            intermediate__results[result[0]].append(result[1])
                    map_count += 1
                else:
                    results.update_results(curent_results)
                    result_count += 1
                # print(f"Got result {curent_results['job']['ID']} from worker {curent_results['worker']}")
                # print(f'got {back_count}')
                timer.reset_time()
            # Start the reducers
            if map_count == to_get_results:
                to_get_results_2 = 0
                map_count = 0
                print('GO!')
                for d in intermediate__results:
                    unique_num += 1
                    ID_to_func[unique_num] = (script1.reducer, d)
                    shared_job_q.put({'fn': script1.reducer, 'arg': (d, intermediate__results[d]), 'ID': unique_num,
                                      'type': 'reducer'})
                    to_get_results_2 += 1
            if result_count == to_get_results_2:
                print([x['result'] for x in results.get_results()])
                print("Got all results!")
                break
            if timer.over_time_out():
                for line in iter(worker_factory.stdout.readline, ""):
                    print(line, end="")
                for line in iter(worker_factory.stderr.readline, ""):
                    print(line, end="")
                print('To long no result exiting')
                break
            time.sleep(2)
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
            stdin, self.stdout, self.stderr = ssh.exec_command(
                f'/homes/mlfrederiks/p1venv/bin/python /homes/mlfrederiks/PycharmProjects/Programming2/Assigment3/Assignment3.py  -w {ip} -h {[self.server_ip]} -p {self.server_port} -n {cores}')
            print('Done')

    def setup_worker(self, num_processes, worker_ip):
        worker = Worker(self.server_ip, self.server_port, num_processes, worker_ip)


class Worker(object):
    def __init__(self, ip, port, num_processes, worker_ip):
        self.worker_ip = worker_ip
        self.message_manager = MessageManager('c')
        key = Authkey()
        self.manager = ServerQueueManager(address=(ip, port), authkey=key.get_key())
        _timer = time.time()
        while True:
            try:
                self.manager.connect()
                break
            except ConnectionRefusedError:
                if (time.time() - _timer) > 60:
                    break
        print('Client connected to %s:%s' % (ip, port))
        self.poison_pill = PoisonPill()
        self.run_workers(num_processes)

    def run_workers(self, num_processes):
        processes = []
        for p in range(num_processes):
            temP = mp.Process(target=self.peon)
            processes.append(temP)
            temP.start()
        print("Started %s workers!" % len(processes))
        for temP in processes:
            temP.join()

    def peon(self):
        job_q = self.manager.get_job_q()
        result_q = self.manager.get_result_q()
        active_task_q = self.manager.get_active_task_q()
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
                        print(f'started_working {job}')
                        active_task_q.put({'job': job, 'worker': self.worker_ip})
                        # TODO dit met meerdere arguments (dus 1 worker werkt met functies tegelijk)
                        print(f'Ok now really')
                        result = job['fn'](job['arg'][0], job['arg'][1])

                        print(f'Ended job {job}')
                        timer.reset_time()
                        result_q.put({'job': job, 'result': result, 'worker': self.worker_ip})
                    except Exception  as e:
                        result_q.put({'job': job, 'result': 'Error404', 'worker': self.worker_ip})
            except queue.Empty:
                if timer.over_time_out():
                    return
                print("sleepytime for", my_name)
                time.sleep(1)


class KeepWorkingJobs(object):
    def __init__(self, workers):
        self.active_workers_jobs = {}
        for worker in workers:
            self.active_workers_jobs[worker] = []

    def add_job(self, worker, job):
        self.active_workers_jobs[worker].append(int(job))

    def remove_job(self, worker, job):
        self.active_workers_jobs[worker].remove(int(job))

    def get_active_jobs(self):
        return self.active_workers_jobs


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
        self.current = -1
        self.directory = directory
        self.all_files = []
        self.add_files()
        self.new_files = self.all_files[:]

    def add_files(self):
        for filename in os.listdir(self.directory):
            if filename.endswith(".py"):
                self.all_files.append(filename)

    def check_new_file(self):
        for filename in os.listdir(self.directory):
            if filename.endswith(".py"):
                if filename not in self.all_files:
                    self.new_files.append(filename)
                    self.all_files.append(filename)
                    print(f'new file found! {filename}')

    def file_not_new(self, file):
        file = file.split('/')[-1]
        self.new_files.remove(file)

    def get_new_files(self):
        return [f"{self.directory}/{x}" for x in self.new_files]


def string_to_list(a_string_list):
    a_string_list = a_string_list.replace('[', '["')
    a_string_list = a_string_list.replace(']', '"]')
    a_string_list = a_string_list.replace(',', '","')
    return ast.literal_eval(a_string_list)


if __name__ == '__main__':
    path = '/homes/mlfrederiks/PycharmProjects/Programming2/Assigment3/WatchDirectory'
    watcher = WatchDirectoy(path)
    # TODO dit laten checken doorlopen ofzo
    watcher.check_new_file()
    new_files = watcher.get_new_files()
    for new_file in new_files:
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
                all_ips = str(all_ips).replace(' ', '')
            elif o == '-p':
                server_port = int(a)
            elif o == '-n':
                cores = int(a)

        worker_factory = WorkerFactory(server_ip, server_port)
        if mode == 'server':
            worker_factory.setup_ssh(worker_ips)
            server = Server(server_ip, server_port, worker_ips)
        elif mode == 'worker':
            worker_factory.setup_worker(cores, ip_worker)
        else:
            # Start SSH of server
            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            ssh.connect(server_ip)
            stdin, stdout, stderr = ssh.exec_command(f'/homes/mlfrederiks/p1venv/bin/python '
                                                     f'/homes/mlfrederiks/PycharmProjects/Programming2/Assigment3/Assignment3.py '
                                                     f'-m -h {all_ips} -p {server_port} -n {cores}')
            # Waarom werkt het alleen, met dit sluit die anders het script?
            for line in iter(stdout.readline, ""):
                print(line, end="")
            for line in iter(stderr.readline, ""):
                print(line, end="")
