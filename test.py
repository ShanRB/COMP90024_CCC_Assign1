import json
filename = "data/tinyTwitter.json"

file0 = open(filename)

count = 0
for line in file0:
    count = count + 1
    if line.endswith(',\n'):
        line = line[:-2]
        data = json.loads(line)
        if count <=1:
            print(data['doc']['metadata'])

print(f'file {filename}, size is: {count}')
