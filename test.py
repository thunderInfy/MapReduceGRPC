from glob import glob
import re

def tokenize_text(text):
    text = text.lower()
    text = re.sub(r'[^A-Za-z]', ';', text).split(';')
    return text

# Slow method, doesn't use map reduce, used for testing
def get_counts_directly():
    inp_files = glob('inputs/*.txt')
    counts = {}
    for file in inp_files:
        with open(file,'r') as fin:
            text = fin.read()
            text = tokenize_text(text)
        for word in text:
            if len(word):
                if word in counts:
                    counts[word] += 1
                else:
                    counts[word] = 1
    
    return counts

# combines all the output files to give a single dictionary
def get_mr_counts_in_one_dict():
    mr_out_files = glob('files/out/*.txt')

    MR_counts = {}
    for file in mr_out_files:
        with open(file, 'r') as fin:
            text = fin.read().split('\n')
        out_counts = {i.split(' ')[0]:int(i.split(' ')[1]) for i in text if len(i)!=0}
        MR_counts.update(out_counts)
    return MR_counts

counts = get_counts_directly()
MR_counts = get_mr_counts_in_one_dict()

if MR_counts == counts:
    print('Counts obtained through map-reduce match with counts calculated directly.')
else:
    print("Counts obtained through map-reduce don't match with counts calculated directly.")