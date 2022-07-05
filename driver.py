from concurrent import futures
import grpc
import mrtask_pb2
import mrtask_pb2_grpc
import os
from glob import glob
import re
from pathlib import Path
from threading import Lock, Event
import argparse

'''
In the worst cases, input file sizes can vary a lot.
In map-reduce, we are trying to divide our entire task in smaller tasks
of almost equal size to give it to multiple workers. If we directly
distribute files among workers, it can lead to non-uniform load
distribution. Therefore, we need to divide our total work into chunks
of almost equal sizes. 
We don't exactly write these chunks in 
memory because that would not be memory efficient. Instead, chunks are
just some information about what files to read,
where to start reading a file and how much to read. This metadata is what
we pass over the network.
'''
def get_chunk_size(args, filelist):
    total_size = 0
    for fpath in filelist:
        total_size += os.stat(fpath).st_size
    chunk_size = int(total_size//args.N)
    return chunk_size

'''
Because our task is to calculate frequencies
of words, we also need to respect word boundaries, while
dividing files in smaller chunks.
'''
def get_chunks_info(args, chunk_size, inp_files):
    index = 0
    info = []
    start = 0

    for task_id in range(args.N):
        task_info = []
        size = chunk_size
        while(size > 0 and index < len(inp_files)):
            file_size = os.stat(inp_files[index]).st_size
            if start + size >= file_size:
                '''
                This chunk includes all text 
                till the end of the current file
                '''
                task_info.append([index,start,-1])
                size = size - (file_size - start)
                index += 1
                start = 0
            else:
                '''
                This chunk includes some part of the current
                file
                '''
                with open(inp_files[index],'r') as fin:
                    fin.seek(start+size-1)
                    if fin.read(1).isalpha():
                        '''
                        The last character in this chunk is an
                        alphabet. In order to respect word boundaries,
                        we need to stretch this chunk so as to include
                        the complete word at the end of the chunk.
                        '''
                        ch = fin.read(1)
                        while ch.isalpha():
                            size += 1
                            ch = fin.read(1)
                        if ch is '':
                            size = -1      
                task_info.append([index,start,size])
                if size is -1:
                    start = 0
                    index += 1
                else:
                    start += size
                size = 0
        info.append(task_info)
    return info

class MRTask(mrtask_pb2_grpc.MRTaskServicer):

    def __init__(self, stop_event, args, info):
        super(MRTask, self).__init__()
        self.tasks_done = 0
        self.task_id = 0
        self.all_map_tasks_allocated = False
        self.all_reduce_tasks_allocated = False
        self.map_tasks_completed = Event()
        self.stop_event = stop_event
        self.waiting_threads = 0

        '''
        A lock is required if the driving is serving multiple
        workers through multiple threads. A context switch
        should not happen in between of contructing map and 
        reduce tasks because self.task_id is being updated.
        '''

        self.lock = Lock()
        self.args = args
        self.info = info

    def MapTask(self):

        self.lock.acquire()
        packed_info = []
        for files_information in self.info[self.task_id]:
            packed_info.append(
                mrtask_pb2.MetaData(
                    fileindex = files_information[0],
                    startseek = files_information[1],
                    chunksize = files_information[2]
                )
            )
        task = mrtask_pb2.Task(
            map_task = True,
            task_id = self.task_id,
            fileinfo = packed_info,
            task_param = self.args.M
        )
        self.task_id += 1
        if(self.task_id >= self.args.N):
            self.all_map_tasks_allocated = True
            self.task_id = 0
        self.lock.release()

        return task
    
    def ReduceTask(self):
        self.lock.acquire()
        task = mrtask_pb2.Task(
            map_task = False,
            task_id = self.task_id,
            fileinfo = [],
            task_param = self.args.N
        )
        self.task_id += 1
        if(self.task_id >= self.args.M):
            self.all_reduce_tasks_allocated = True
        self.lock.release()

        return task

    def DoNothingTask(self):
        return mrtask_pb2.Task(
            map_task = False,
            task_id = -1,
            fileinfo = [],
            task_param = -1
        )

    def wait_for_all_map_tasks(self):
        self.lock.acquire()
        self.waiting_threads += 1
        self.lock.release()
        self.map_tasks_completed.wait()
        self.lock.acquire()
        self.waiting_threads -= 1
        self.lock.release()

    def SendTask(self, request, context):
        
        if(not self.all_map_tasks_allocated):
            return self.MapTask()
        else:
            if(not self.map_tasks_completed.is_set()):
                '''
                All map tasks have not been completed.
                So worker threads should wait. To implement this,
                Event() from the threading module has been used.
                Not all the threads should wait, because then,
                the driver won't be able to communicate with
                the worker who would be sending out the message
                that it is done with its task.
                '''
                if(self.waiting_threads < self.args.MAX_WORKERS - 1):
                    self.wait_for_all_map_tasks()
                else:
                    return self.DoNothingTask()
        
        if(not self.all_reduce_tasks_allocated):
            return self.ReduceTask()
        else:
            '''
            All tasks have been completed.
            Call the event to stop the server.
            '''
            self.stop_event.set()
        
    def TaskDone(self, request, context):
        self.tasks_done += 1

        if(self.tasks_done == self.args.N):
            self.map_tasks_completed.set()

        return mrtask_pb2.Okay()

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("N", help="Please provide the number N of map tasks", type=int)
    parser.add_argument("M", help="Please provide the number M of map tasks", type=int)
    args = parser.parse_args()
    inp_files = sorted(glob('inputs/*.txt'))
    chunk_size = get_chunk_size(args, inp_files)
    info = get_chunks_info(args, chunk_size, inp_files)
    args.MAX_WORKERS = 10
    stop_event = Event()
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=args.MAX_WORKERS))
    mrtask_pb2_grpc.add_MRTaskServicer_to_server(MRTask(stop_event, args, info), server)
    server.add_insecure_port('[::]:50051')
    server.start()
    stop_event.wait()
    server.stop(None)

if __name__ == "__main__":
    main()