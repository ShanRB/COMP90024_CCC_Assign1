"""
    filename :      rankTwitter.py
    author:         Rongbing Shan (945388)
    email:          alex.shan@student.unimelb.edu.au
    description:    This is the implementation for assignment1 of Cluster and Cloud Computing
                    at the Universityof Melbourne for 2020 Semester 1.
                    The program will process a Json input file of twitters and return the
                    top 10 count of hashtags and languages used in the twitter file
"""

import json
from mpi4py import MPI
import time
import argparse

def langParse(code):
    """
    function to parse language code into full language names
    """
    # create dictionary for mapping language codes
    langParser = {'en':'English','am':'Amharic','ar':'Arabic','bg':'Bulgarian','bn':'Bengali','bo':'Tibetan',\
            'ckb':'Sorani Kurdish','cs':'Czech','cy':'Welsh','da':'Danish','de':'German','el':'Greek',\
            'es':'Spanish','et':'Estonian','fa':'Persian','fi':'Finnish','fil':'Filipino','fr':'French',\
            'gu':'Gujarati','he':'Hebrew','hi':'Hindi','ht':'Haitian','hu':'Hungarian','hy':'Armenian',\
            'id':'Indonesian','in':'Indonesian','is':'IceLandic','it':'Italian','iw':'Hebrew','ja':'Japanese',\
            'ka':'Georgian','km':'Khmer','kn':'Kannada','ko':'Korean','lo':'Lao','lt':'Lithuanian',\
            'lv':'Latvian','ml':'Malayalam','mr':'Marathi','msa':'Malay','my':'Burmese','ne':'Nepali',\
            'nl':'Dutch','no':'Norwegian','or':'Oriya','pa':'Panjabi','pl':'Polish','ps':'Pashto',\
            'pt':'Portuguese','ro':'Romanian','ru':'Russian','sd':'Sindhi','si':'Sinhala','sk':'Slovak',\
            'sl':'Slovenian','sr':'Serbian','sv':'Swedish','ta':'Tamil','te':'Telugu','th':'Thai',\
            'tl':'Tagalog','tr':'Turkish','ug':'Uyghur','uk':'Ukrainian','ur':'Urdu','vi':'Vietnamese',\
            'zh-cn':'Chinese(Simplified)','zh-tw':'Chinese(Traditional)','und':'Undefined'}
    # parse code 
    if code in langParser:
        return langParser[code] + '(' + code + ')'
    else:
        # for code not defined here, return as 'Others'
        return 'Undefined(' + code + ')'

def merge_dict(dict_list):
    """
    function to merge all dictionaries in the input dict_list
    into a single dictionary, addings values together for same key
    """
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
# create MPI communicator
comm = MPI.COMM_WORLD
comm_rank = comm.Get_rank()
comm_size = comm.Get_size()
# parse arguments
parser = argparse.ArgumentParser()
parser.add_argument("-f", "--filename", required=True, help='filename to process')
# get file name from arguments
filename = parser.parse_args().filename

# ready to process file to extract hashtags and languages
line_num = 0
file0 = open(filename,'r')  # open target json file
hashtags = {}   # dictionary to hold hashtags and their no. of appearances
language = {}   # dictionary to hold language and their no. of appearances
for line in file0:
    line_num = line_num + 1
    # divide the file to be processed by different node
    # the data is split to each node by remainders of line number divided by node numbers
    if(comm_rank == line_num % comm_size):
            # read json files
        if(line.endswith(',\n')):
            line = line[:-2]
            data = json.loads(line)
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
file0.close()
comm.Barrier()  # wait for all nodes to align

# gather results from all nodes to master node
all_hashtags = comm.gather(hashtags, root=0)
all_language = comm.gather(language, root=0)
# master node process all results and find top 10 hashtags and languages
if comm_rank == 0:
    # merge all dicitionaries from each node to single one
    final_hashtags = merge_dict(all_hashtags)
    final_language = merge_dict(all_language)
    # sorted the reusult by appearances of hashtags and languages
    sorted_hashtags = sorted(final_hashtags, key=final_hashtags.get, reverse=True)[0:10]
    sorted_language = sorted(final_language, key=final_language.get, reverse=True)[0:10]
    
    # print out results of top 10 hashtags and languages
    print("Top hashtags:")
    for i in range(10):
        print(f'{i+1:2d}. #{sorted_hashtags[i]}, {final_hashtags[sorted_hashtags[i]]}')
    print("-" * 30)
    print("Top languages:")
    for i in range(10):
        print(f'{i+1:2d}. {langParse(sorted_language[i])}, {final_language[sorted_language[i]]}')
    
    # calculate execution time
    end_time = time.time()
    print(f'\nExecution time is {end_time-start_time:.5f} seconds')
