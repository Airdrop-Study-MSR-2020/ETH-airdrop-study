import sys
import csv


average_amount = {}
with open(sys.argv[1]) as csv_file:
    csv_reader = csv.reader(csv_file, delimiter=' ')
    previous_token = ''
    previous_addr = ''
    begin = 0
    end = 0
    token_amount = {}
    number = 0
    total = 0
    for row in csv_reader:
        if previous_token == '':
            previous_token = row[0]
            previous_addr = row[1]
            begin = int(row[5])
            end = int(row[6])
            number += 1
            total += int(row[2])
            token_amount[row[1]] = int(row[2])
            continue
        if row[0] == previous_token and int(row[5]) == begin and int(row[6]) == end:
            token_amount[row[1]] = int(row[2])
            number += 1
            total += int(row[2])
        else:
            key = previous_token+str(begin)+str(end)
            average = float(total)/(2*number)
            average_amount[key] = average
            previous_token = row[0]
            previous_addr = row[1]
            begin = int(row[5])
            end = int(row[6])
            number = 1
            total = int(row[2])
            token_amount = {}
            token_amount[row[1]] = int(row[2])
    key = previous_token+str(begin)+str(end)
    average = float(total)/(2*number)
    average_amount[key] = average

with open(sys.argv[1]) as csv_file:
    csv_reader = csv.reader(csv_file, delimiter=' ')
    file1 = open("final_version_airdrop2.csv", "a+")
    for row in csv_reader:
        key = row[0]+str(row[5])+str(row[6])
        average = average_amount[key]
        if int(row[2]) >= average:
            file1.write("{0} {1} {2} {3} {4} {5} {6}\n".format(row[0], row[1], row[2], row[3], row[4], row[5], row[6]))
