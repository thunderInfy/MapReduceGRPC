import os
from glob import glob
from matplotlib import pyplot as plt

def get_sizes(filelist):
    return [os.stat(i).st_size//(1024) for i in filelist]

def get_different_folder_sizes(M):
    inp_file_sizes = get_sizes(glob('inputs/*.txt'))
    intermediate_file_sizes = []
    mapper_file_sizes = []
    for i in range(M):
        intermediate_file_sizes.append(get_sizes(glob('files/intermediate/*%d.txt'%(i))))
    for i in range(len(intermediate_file_sizes[0])):
        total = 0
        for j in range(M):
            total += intermediate_file_sizes[j][i]
        mapper_file_sizes.append(total)
    return inp_file_sizes, intermediate_file_sizes, mapper_file_sizes

inp_file_sizes, intermediate_file_sizes, mapper_file_sizes = get_different_folder_sizes(5)

fig = plt.figure(figsize=(10,10))
fig.patch.set_facecolor('white')
plt.plot(inp_file_sizes)
plt.grid()
plt.xlabel('Input Files')
plt.ylabel('File size in KB')
plt.title('Non-uniform load')
plt.savefig('Non-uniform-load.png')

fig = plt.figure(figsize=(10,10))
fig.patch.set_facecolor('white')
plt.plot(mapper_file_sizes, label='Sum of all buckets for different map-task-ids is almost constant')
for i in range(M):
    plt.plot(intermediate_file_sizes[i], label='Bucket %d'%(i))
plt.grid()
plt.legend(loc='center')
plt.xlabel('Intermediate Files')
plt.ylabel('File size in KB')
plt.title('Varying bucket sizes because of differences in letter frequency')
plt.savefig('bucket-sizes.png')

