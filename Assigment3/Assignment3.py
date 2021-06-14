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
import importlib.util
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
            get_heartbeat_q = queue.Queue()
            QueueManager.register('get_job_q', callable=lambda: get_job_q)
            QueueManager.register('get_result_q', callable=lambda: get_result_q)
            QueueManager.register('get_active_task_q', callable=lambda: get_active_task_q)
            QueueManager.register('get_heartbeat_q', callable=lambda: get_heartbeat_q)
        elif mode == 'c':
            ServerQueueManager.register('get_job_q')
            ServerQueueManager.register('get_result_q')
            ServerQueueManager.register('get_active_task_q')
            ServerQueueManager.register('get_heartbeat_q')


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
    def __init__(self, ip, port, worker_ips, script):
        self.script = script
        self.working_jobs = KeepWorkingJobs(worker_ips)
        self.message_manager = MessageManager('s')
        key = Authkey()
        self.manager = QueueManager(address=(ip, port), authkey=key.get_key())
        self.manager.start()
        print('Server started at port %s' % port)
        self.poison_pill = PoisonPill()
        # Start a shared manager server and access its queues
        self.shared_job_q = self.manager.get_job_q()
        self.shared_result_q = self.manager.get_result_q()
        self.shared_active_task_q = self.manager.get_active_task_q()
        self.shared_heartbeat_q = self.manager.get_heartbeat_q()
        # The reducer results
        self.intermediate__results = defaultdict(list)
        self.map_count = 0
        # End_results
        self.results = Results()
        self.result_count = 0
        # Time_out_timer
        self.timer = TimeOutTimer(30)
        # Start heartbeat
        self.shared_heartbeat_q.put(self.working_jobs)
        self.runserver()

    def _check_jobs(self):
        # A job got taken!
        while not self.shared_active_task_q.empty():
            taken_job = self.shared_active_task_q.get_nowait()
            self.working_jobs.add_job(taken_job['worker'], taken_job['job']['ID'])
            # print(self.working_jobs.get_active_jobs())
            # print(f"Gave job {taken_job['job']['ID']} to worker {taken_job['worker']}")
            # print(f'gave {gave_count}')
        # A job got returned!
        while not self.shared_result_q.empty():
            curent_results = self.shared_result_q.get_nowait()
            self.working_jobs.remove_job(curent_results['worker'], curent_results['job']['ID'])
            print(self.working_jobs.get_active_jobs())
            # Shuffling
            if curent_results['job']['type'] == 'mapper':
                # TODO dit niet hardcoden
                if curent_results['result'] != 'Error404':
                    for result in curent_results['result']:
                        self.intermediate__results[result[0]].append(result[1])
                self.map_count += 1
            else:
                self.results.update_results(curent_results)
                self.result_count += 1
            # print(f"Got result {curent_results['job']['ID']} from worker {curent_results['worker']}")
            # print(f'got {back_count}')
            self.timer.reset_time()

    def runserver(self):
        # Put the functions in a queue
        ID_to_func = {}
        unique_num = 0
        for d in self.script.Data('32651208'):
            unique_num += 1
            ID_to_func[unique_num] = (self.script.mapper, d)
            self.shared_job_q.put({'arg': d, 'ID': unique_num, 'type': 'mapper'})
        to_get_results = unique_num
        time.sleep(2)
        to_get_results_2 = None
        while True:
            # CHeck how the jobs are going
            self._check_jobs()
            # Start the reducers
            if self.map_count == to_get_results:
                to_get_results_2 = 0
                self.map_count = 0
                # Put the intermediate keys in the queue
                for d in self.intermediate__results:
                    unique_num += 1
                    ID_to_func[unique_num] = (self.script.reducer, d)
                    self.shared_job_q.put({'arg': (d, self.intermediate__results[d]), 'ID': unique_num,
                                      'type': 'reducer'})
                    to_get_results_2 += 1
            if self.result_count == to_get_results_2:
                # print([x['result'] for x in results.get_results()])
                print("Got all results!")
                break
            if self.timer.over_time_out():
                print('To long no result exiting')
                break
            # Check if workers are dead
            print(self.working_jobs.get_status())
            time.sleep(2)
            try:
                self.working_jobs = self.shared_heartbeat_q.get_nowait()
                print(self.working_jobs.get_status())
                print('Niet Rip')
            except queue.Empty:
                print('Rip')
                pass
        # Tell the client process no more data will be forthcoming
        self.shared_job_q.put(self.poison_pill.get_pill())
        self.shared_heartbeat_q.put(self.poison_pill.get_pill())
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

    def setup_ssh(self, ips, path):
        for ip in ips:
            print(f'Setting up SSH {ip}')
            ssh = paramiko.SSHClient()
            self.all_worker.append(ssh)
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            ssh.connect(ip)
            print(f'setup_worker to connect to {self.server_ip} on {self.server_port}')
            stdin, self.stdout, self.stderr = ssh.exec_command(
                f'/homes/mlfrederiks/p1venv/bin/python /homes/mlfrederiks/PycharmProjects/Programming2/Assigment3/Assignment3.py '
                f'-w {ip} -h {[self.server_ip]} -p {self.server_port} -n {cores} -d {path[0]}')
            print('Done')

    def setup_worker(self, num_processes, worker_ip, watcher):
        worker = Worker(self.server_ip, self.server_port, num_processes, worker_ip, watcher)


class Worker(object):
    def __init__(self, ip, port, num_processes, worker_ip, watcher):
        self.script = watcher.get_import_file()
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
        self.heartbeat_q = self.manager.get_heartbeat_q()
        print('Client connected to %s:%s' % (ip, port))
        self.poison_pill = PoisonPill()
        self.run_workers(num_processes)

    def run_workers(self, num_processes):
        processes = []
        firstrun = True
        for p in range(num_processes):
            if firstrun:
                temP = mp.Process(target=self.return_heartbeat)
                firstrun = False
            else:
                temP = mp.Process(target=self.peon)
            processes.append(temP)
            temP.start()
        print("Started %s workers!" % len(processes))
        for temP in processes:
            temP.join()

    def return_heartbeat(self):
        while True:
            try:
                roster = self.heartbeat_q.get_nowait()
                if roster == self.poison_pill.get_pill():
                    self.heartbeat_q.put(self.poison_pill.get_pill())
                    return
                roster.set_status(self.worker_ip)
                self.heartbeat_q.put(roster)
            except queue.Empty:
                pass
            time.sleep(np.random.randint(10, 20))

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
                        if job['type'] == 'mapper':
                            func_work = self.script.mapper
                        elif job['type'] == 'reducer':
                            func_work = self.script.reducer
                        print(f'started_working {job}')
                        active_task_q.put({'job': job, 'worker': self.worker_ip})
                        # TODO dit met meerdere arguments (dus 1 worker werkt met functies tegelijk)
                        print(f'Ok now really')
                        result = func_work(job['arg'][0], job['arg'][1])

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
        self.status = {}
        self.active_workers_jobs = {}
        self.workers = workers
        for worker in workers:
            self.status[worker] = time.time()
            self.active_workers_jobs[worker] = []

    def add_job(self, worker, job):
        self.active_workers_jobs[worker].append(int(job))

    def remove_job(self, worker, job):
        self.active_workers_jobs[worker].remove(int(job))

    def get_active_jobs(self):
        return self.active_workers_jobs

    def get_status(self):
        return self.status

    def set_status(self, worker):
        self.status[worker] = time.time()


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
        self.new_files = self.all_files[:]

    def add_files(self):
        for filename in os.listdir(self.directory):
            if filename.endswith(".py"):
                self.all_files.append(filename)
                self.new_files.append(filename)

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

    def get_import_file(self, with_file=None):
        if with_file is not None:
            spec = importlib.util.spec_from_file_location("clientScript", f'{self.directory}/{with_file}')
        else:
            print(self.directory)
            spec = importlib.util.spec_from_file_location("clientScript", self.directory)
        script = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(script)
        return script


def string_to_list(a_string_list):
    a_string_list = a_string_list.replace('[', '["')
    a_string_list = a_string_list.replace(']', '"]')
    a_string_list = a_string_list.replace(',', '","')
    return ast.literal_eval(a_string_list)


if __name__ == '__main__':
    # path = '/homes/mlfrederiks/PycharmProjects/Programming2/Assigment3/WatchDirectory'
    # TODO dit laten checken doorlopen ofzo
    options = "mw:h:p:n:d:"
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
        elif o == '-d':
            directory = a
    worker_factory = WorkerFactory(server_ip, server_port)
    if mode == 'server':
        watcher = WatchDirectoy(directory)
        watcher.add_files()
        watcher.check_new_file()
        new_files = watcher.get_new_files()
        for file in new_files:
            print(file)
            worker_factory.setup_ssh(worker_ips, new_files)
            server = Server(server_ip, server_port, worker_ips, watcher.get_import_file(file.split('/')[-1]))
    elif mode == 'worker':
        watcher = WatchDirectoy(directory)
        worker_factory.setup_worker(cores, ip_worker, watcher)
    else:
        # Start SSH of server
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(server_ip)
        stdin, stdout, stderr = ssh.exec_command(f'/homes/mlfrederiks/p1venv/bin/python '
                                                 f'/homes/mlfrederiks/PycharmProjects/Programming2/Assigment3/Assignment3.py '
                                                 f'-m -h {all_ips} -p {server_port} -n {cores} -d {directory}')
        # Waarom werkt het alleen, met dit sluit die anders het script?
        for line in iter(stdout.readline, ""):
            print(line, end="")
        for line in iter(stderr.readline, ""):
            print(line, end="")
