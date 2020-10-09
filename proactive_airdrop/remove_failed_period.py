import sys
import csv

success_period = []
with open(sys.argv[1]) as csv_file:
    csv_reader = csv.reader(csv_file, delimiter=' ')
    previous_key = ''
    previous_total = 0
    total = 0
    for row in csv_reader:
        if previous_key == '':
            previous_key = row[0] + "_" + row[5] + "_" + row[6]
            previous_total = int(row[3])
            total = int(row[2])
            if float(row[4]) >= 0.5:
                success_period.append(previous_key)
            continue
        key = row[0] + "_" + row[5] + "_" + row[6]
        if key == previous_key:
            total += int(row[2])
        else:
            if float(total)/previous_total >= 0.8:
                if previous_key not in success_period:
                    success_period.append(previous_key)
            previous_key = key
            total = int(row[2])
            if float(row[4]) >= 0.5:
                success_period.append(previous_key)
            previous_total = int(row[3])

    if float(total)/previous_total >= 0.8:
        success_period.append(previous_key)
    print len(success_period)

with open(sys.argv[1]) as csv_file:
    csv_reader = csv.reader(csv_file, delimiter=' ')
    file1 = open("final_proactive_airdropper.csv", "a+")
    for row in csv_reader:
        key = row[0] + "_" + row[5] + "_" + row[6]
        if key in success_period:
            file1.write("{0} {1} {2} {3} {4} {5} {6}\n".format(row[0], row[1], row[2], row[3], row[4], row[5], row[6]))
