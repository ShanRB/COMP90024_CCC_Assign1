# encoding=utf8
import json
from mpi4py import MPI
import time
import argparse

def parse_language_code(code):
    if code == 'en':
        return 'English(en)'
    elif code == 'ar':
        return 'Arabic(ar)'
    elif code == 'bn':
        return 'Bengali(bn)'
    elif code == 'cs':
        return 'Czech(cs)'
    elif code == 'da':
        return 'Danish(da)'
    elif code == 'de':
        return 'German(de)'
    elif code == 'el':
        return 'Greek(el)'
    elif code == 'es':
        return 'Spanish(es)'
    elif code == 'fa':
        return 'Persian(fa)'
    elif code == 'fi':
        return 'Finnish(fi)'
    elif code == 'fil':
        return 'Filipino(fil)'
    elif code == 'fr':
        return 'French(fr)'
    elif code == 'he':
        return 'Hebrew(he)'
    elif code == 'hi':
        return 'Hindi(hi)'
    elif code == 'hu':
        return 'Hungarian(hu)'
    elif code == 'id':
        return 'Indonesian(id)'
    elif code == 'it':
        return 'Italian(it)'
    elif code == 'ja':
        return 'Japanese(ja)'
    elif code == 'ko':
        return 'Korean(ko)'
    elif code == 'msa':
        return 'Malay(msa)'
    elif code == 'nl':
        return 'Dutch(nl)'
    elif code == 'no':
        return 'Norwegian(no)'
    elif code == 'pl':
        return 'Polish(pl)'
    elif code == 'pt':
        return 'Portuguese(pt)'
    elif code == 'ro':
        return 'Romanian(ro)'
    elif code == 'ru':
        return 'Russian(ru)'
    elif code == 'sv':
        return 'Swedish(sv)'
    elif code == 'th':
        return 'Thai(th)'
    elif code == 'tr':
        return 'Turkish(tr)'
    elif code == 'uk':
        return 'Ukrainian(uk)'
    elif code == 'ur':
        return 'Urdu(ur)'
    elif code == 'vi':
        return 'Vietnamese(vi)'
    elif code == 'zh-cn':
        return 'Chinese(Simplified)(zh-cn)'
    elif code == 'zh-tw':
        return 'Chinese(Traditional)(zh-tw)'
    else:
        return 'Others('+code+')'

def merge_dict(dict_list):
    result = dict_list[0]
    for i in range(1,len(dict_list)):
        for key,value in dict_list[i].items():
            if key not in result:
                result[key] = value
            else:
                result[key] += value
    return result
    
# program starts
start_time = time.time()

# parse arguments
parser = argparse.ArgumentParser()
parser.add_argument("-f", "--filename", required=True, help='filename to process')

filename = parser.parse_args().filename
print(filename)
#print("ready to process file: {filename}")

# create MPI communicator
comm = MPI.COMM_WORLD
comm_rank = comm.Get_rank()
comm_size = comm.Get_size()


line_num = 0
file0 = open(filename,'r',encoding='utf-8')
# lines = file0.read()


hashtags = {}
language = {}
count = 0

if comm_rank == 0:
    hashtags_list = []
    for line in file0:
        count = count + 1
        if line.endswith(',\n'):
            line = line[:-2]
            data = json.loads(line)
            #extract hashtag and language code
            hashtag_list = data['doc']['entities']['hashtags']
            # language_code = data['doc']['metadata']['iso_language_code']
            hashtags_list.append(hashtag_list)
    file0.close()

    partitions = [[] for i in range(comm_size)]
    for key, value in enumerate(hashtags_list):
        partitions[key%comm_size].append(value)

   
else:
    partitions = None

local_hashtags_list = comm.scatter(partitions, root = 0) 
print( comm_rank,len(local_hashtags_list))

local_hashtags_dict = {}

for local_hashtags in local_hashtags_list:
    if len(local_hashtags_list) != 0:
        for textdict in local_hashtags_list:
            hashtag = textdict['text'].lower()
            if hashtag not in local_hashtags_dict:
                local_hashtags_dict[hashtag] = 1
            else:
                local_hashtags_dict[hashtag] += 1

hashtags_dict = comm.gather(local_hashtags_dict, root=0)

if comm_rank == 0:
    print(hashtags_dict)




