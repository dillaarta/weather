import json
import time
import csv
import glob

path = "/opt/weather/yelp_dataset/*.json"
name_file = list(glob.glob(path))
output_file = [name.rstrip('json')+'csv' for name in name_file]

for input,output in zip(name_file, output_file):
    count = 0
    with open(input, 'r') as json_file:
        with open(output, 'w') as f:
            for i in json_file:
                data = json.loads(i)
                writer = csv.writer(f)
                if count == 0:
                    header = data.keys()
                    writer.writerow(header)
                    count += 1
                writer.writerow(data.values())
            time.sleep(5)
