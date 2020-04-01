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
#print("ready to process file: {filename}")

# create MPI communicator
comm = MPI.COMM_WORLD
comm_rank = comm.Get_rank()
comm_size = comm.Get_size()


line_num = 0
file0 = open(filename,'r')

hashtags = {}
language = {}
for line in file0:
    line_num = line_num + 1
    # divide the file to be processed by different node
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

#print('Rank ', comm_rank)
#print(f'hashtags({len(hashtags)}): {hashtags}]n\nlanguage({len(language)}): {language}')

comm.Barrier()

if comm_rank != 0:
    comm.send(hashtags, dest=0, tag=1)
    comm.send(language, dest=0, tag=2)

comm.Barrier()
if comm_rank == 0:
    all_hashtags = [hashtags]
    all_language = [language]
    for i in range(1,comm_size):
        all_hashtags += [comm.recv(source=i, tag=1)]
        all_language += [comm.recv(source=i, tag=2)]
    final_hashtags = merge_dict(all_hashtags)
    final_language = merge_dict(all_language)
    
    sorted_hashtags = sorted(final_hashtags, key=final_hashtags.get, reverse=True)[0:10]
    sorted_language = sorted(final_language, key=final_language.get, reverse=True)[0:10]
    
    # print out results
    print("Top hashtags:")
    for i in range(10):
        print(f'{i+1:2d}. #{sorted_hashtags[i]}, {final_hashtags[sorted_hashtags[i]]}')
    print("-" * 30)
    print("Top languages:")
    for i in range(10):
        print(f'{i+1:2d}. {parse_language_code(sorted_language[i])}, {final_language[sorted_language[i]]}')

    end_time = time.time()
    print(f'\nExecution time is {end_time-start_time:.5f} seconds')
