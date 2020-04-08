import json
from mpi4py import MPI
import time
import argparse
import util
    
# program starts
start_time = time.time()

# create MPI communicator
comm = MPI.COMM_WORLD
comm_rank = comm.Get_rank()
comm_size = comm.Get_size()

# parse arguments
parser = argparse.ArgumentParser()
parser.add_argument("-f", "--filename", required=True, help='filename to process')

filename = parser.parse_args().filename

# master node opens file and scatter data
chunks = None
print(f'{comm_rank} of size {comm_size}')
print(comm_rank,'ready to read file')
file0 = open(filename,'r')
comm.Barrier()

if comm_rank == 0:
    print("RANK{comm_rank} reading data...")
    dataset = []
    
    for line in file0:
        # read json files
        if(line.endswith(',\n')):
            line = line[:-2]
            data = json.loads(line)
            dataset.append(data)
    file0.close()
    print(f'dataset size: {len(dataset)}')
    chunks = [[]] * comm_size
    for key, value in enumerate(dataset):
        chunks[key%comm_size].append(value)

localdata = comm.scatter(chunks,root=0)
hashtags = {}
language = {}
for data in localdata:
    # extract hashtag and language code
    hashtag_list = data['doc']['entities']['hashtags']
    language_code = data['doc']['metadata']['iso_language_code']
    if len(hashtag_list) != 0:
        for textdict in hashtag_list:
            hashtag = textdict['text'].lower()
            if hashtag not in hashtags:
                hashtags[hashtag] = 1
            else:
                hashtags[hashtag] += 1
    if language_code not in language:
        language[language_code] = 1
    else:
        language[language_code] += 1

all_hashtags = comm.gather(hashtags,root=0)
all_language = comm.gather(language,root=0)

print(f"allhash: {len(all_hashtags)} , alllang: {len(all_language)}")
if comm_rank == 0:        
    final_hashtags = util.merge_dict(all_hashtags)
    final_language = util.merge_dict(all_language)
    
    print("prepared to sort data")
    sorted_hashtags = sorted(final_hashtags, key=final_hashtags.get, reverse=True)[0:10]
    sorted_language = sorted(final_language, key=final_language.get, reverse=True)[0:10]
    
    # print out results
    print("Top hashtags:")
    for i in range(10):
        print(f'{i+1:2d}. #{sorted_hashtags[i]}, {final_hashtags[sorted_hashtags[i]]}')
    print("-" * 30)
    print("Top languages:")
    for i in range(10):
        print(f'{i+1:2d}. {util.parse_language_code(sorted_language[i])}, {final_language[sorted_language[i]]}')

    end_time = time.time()
    print(f'\nExecution time is {end_time-start_time:.5f} seconds')




