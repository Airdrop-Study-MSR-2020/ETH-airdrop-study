import csv
import sys

begin_time = {}
end_time = {}
with open(sys.argv[1]) as csv_file:
    csv_reader = csv.reader(csv_file, delimiter=' ')
    previous_token = ''
    previous_crowdsale = ''
    begin = 0
    end = 0
    begin_time = {}
    end_time = {}
    for row in csv_reader:
        if previous_token == '':
            previous_token = row[0]
            previous_crowdsale = row[1]
            begin = int(row[2])
            end = int(row[3])
            continue
        if row[0] == previous_token and row[1] == previous_crowdsale:
            if int(row[3]) > end:
                end = int(row[3])
            if int(row[2]) < begin:
                begin = int(row[2])
        else:
            key = previous_token+previous_crowdsale
            begin_time[key] = begin
            end_time[key] = end
            previous_token = row[0]
            previous_crowdsale = row[1]
            begin = int(row[2])
            end = int(row[3])
    key=previous_token+previous_crowdsale
    begin_time[key]=begin
    end_time[key] = end

with open(sys.argv[2]) as csv_file1:
    csv_reader1 = csv.reader(csv_file1, delimiter=' ')
    file1 = open("step1_airdropper.csv", "w+")
    file2 = open("find_the_ico_senders.csv", "w+")
    for row in csv_reader1:
        key = row[0]+row[1]
        if key in begin_time:
            coin_begin = begin_time[key]
            coin_end = end_time[key]
            if int(row[6]) < coin_begin or int(row[5]) > coin_end:
                file1.write("{0} {1} {2} {3} {4} {5} {6}\n".format(row[0], row[1], row[2], row[3], row[4], row[5], row[6]))
            else:
                file2.write("{0} {1} {2} {3} {4} {5}\n".format(row[0], row[1], row[5], row[6], coin_begin, coin_end))
        else:
            file1.write("{0} {1} {2} {3} {4} {5} {6}\n".format(row[0], row[1], row[2], row[3], row[4], row[5], row[6]))
