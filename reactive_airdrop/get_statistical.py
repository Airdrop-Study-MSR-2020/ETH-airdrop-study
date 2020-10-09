import sys
import csv

with open(sys.argv[1]) as csv_file:
    csv_reader = csv.reader(csv_file, delimiter=',')
    file1 = open(sys.argv[2], "w+")
    overall = {}
    already = set()
    less_than = {}
    for row in csv_reader:
        #token_addr+token_sender+tx_hash
        key = row[3]+"_"+row[4]+"_"+row[7]
        if key not in already:
            key2 = row[3]+"_"+row[4]
            if key2 in overall:
                value = overall[key2]
                overall[key2] = value+1
                if float(row[2]) <= 0.001:
                    less_than[key2] = less_than[key2] + 1
            else:
                overall[key2] = 1
                if float(row[2]) <= 0.001:
                    less_than[key2] = 1
                else:
                    less_than[key2] = 0
        already.add(key)
    for key, value in overall.items():
        if value < 10:
            continue
        token_addr = key.split("_")[0]
        token_from = key.split("_")[1]
        file1.write("{0} {1} {2} {3}\n".format(token_addr, token_from, value, less_than[key], float(less_than[key])/value))
