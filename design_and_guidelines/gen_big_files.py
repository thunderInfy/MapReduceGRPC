from glob import glob
import random
import argparse

def generate_big_files(inp_files, how_many, how_big):
    for i in range(how_many):
        with open(random.choice(inp_files),'rb') as fin:
            text = fin.read()
            with open('inputs/big_file%d.txt'%(i),'wb') as fout:
                fout.write(text*how_big)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("Num", help="How many big files you wanna create?", type=int)
    parser.add_argument("Fac", help="Provide a multiplying factor to indicate how big", type=int)
    args = parser.parse_args()
    inp_files = glob('inputs/*.txt')
    generate_big_files(inp_files, args.Num, args.Fac)

if __name__ == "__main__":
    main()

