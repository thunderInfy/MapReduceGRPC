import grpc
import mrtask_pb2
import mrtask_pb2_grpc
from glob import glob
from pathlib import Path
import re

def tokenize_text(text):
    text = text.lower()
    text = re.sub(r'[^A-Za-z]', ';', text).split(';')
    return text

def mapper(inp_files, inp_task):
    M = inp_task.task_param

    # make directories if they don't exist
    Path("files/intermediate").mkdir(parents=True, exist_ok=True)
    
    text = ''
    for i in inp_task.fileinfo:
        with open(inp_files[i.fileindex],'r') as fin:
            fin.seek(i.startseek)
            if i.chunksize!=-1:
                text += fin.read(i.chunksize)
            else:
                text += fin.read()
    
    text = tokenize_text(text)
    
    fpouts = [
        open(
            "files/intermediate/mr-%d-%d.txt"%(inp_task.task_id, bucket_id),"w"
            ) for bucket_id in range(M)
        ]

    '''
    a is mapped to bucket 0
    b is mapped to bucket 1 (if it exists, i.e. M > 1) and so on
    '''

    for word in text:
        if len(word):
            bucket = (ord(word[0]) - ord('a'))%M
            fpouts[bucket].write(word+'\n')
    
    for bucket_id in range(M):
        fpouts[bucket_id].close()

def reducer(task_id, N):
    # make directories if they don't exist
    Path("files/out").mkdir(parents=True, exist_ok=True)
    
    frequency = {}
    for map_task_id in range(N):
        with open("files/intermediate/mr-%d-%d.txt"%(map_task_id, task_id),"r") as fin:
            lines = fin.read().splitlines()
            for word in lines:
                if word in frequency:
                    frequency[word] += 1
                else:
                    frequency[word] = 1
    with open("files/out/out-%d.txt"%(task_id),"w") as fout:
        for key, value in frequency.items():
            fout.write('%s %d\n'%(key, value))

def main():
    inp_files = sorted(glob('inputs/*.txt'))
    with grpc.insecure_channel('localhost:50051') as channel:
        stub = mrtask_pb2_grpc.MRTaskStub(channel)
        
        # wait for the driver to start
        task = stub.SendTask(mrtask_pb2.IDemandATask(), wait_for_ready = True)
        while True:
            try:
                if(task.map_task):
                    mapper(inp_files, task)
                    stub.TaskDone(mrtask_pb2.IAmDone())
                else:
                    if(task.task_id!=-1):
                        reducer(task.task_id, task.task_param)
                        stub.TaskDone(mrtask_pb2.IAmDone())
                task = stub.SendTask(mrtask_pb2.IDemandATask())
            except:
                break

if __name__ == "__main__":
    main()