import json
from mpi4py import MPI

filename = "data/smallTwitter.json"
i = 0
file0 = open(filename,'r')

i = 0
for line in file0:
    i = i + 1
    if(line.endswith(',\n') and i <=5):
        line = line[:-2]
        data = json.loads(line)
        print(f'>>>{i}')
        for key,value in data['doc']['entities'].items():
            print(f'{key}: {value}')
