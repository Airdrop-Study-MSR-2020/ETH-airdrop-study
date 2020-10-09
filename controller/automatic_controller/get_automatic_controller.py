import sys
import csv
import collections

with open(sys.argv[1]) as csv_file:
    csv_reader = csv.reader(csv_file, delimiter=' ')
    T = int(sys.argv[2])
    N = int(sys.argv[3])
    previous_key = ''
    number_count = collections.defaultdict(set)
    number = {}
    file1 = open("program_controller.csv", "a+")
    for row in csv_reader:
        key = row[0]+"_"+row[1]
        if key == previous_key:
            number_count[int(row[3])].add(row[2])
            if key in number:
                number[key] = number[key] + 1
            else:
                number[key] = 1
        else:
            sorted_result = sorted(number_count.items())
            for result in sorted_result:
                total_sum = 0
                for i in range(result[0]-T, result[0]+1):
                    total_sum += len(number_count[i])
                if total_sum >= N:
                    file1.write("{0} {1} {2}\n".format(previous_key.split("_")[0], previous_key.split("_")[1], number[previous_key]))
                    break
            number_count = collections.defaultdict(set)
            number = {}
            previous_key = key
            number_count[int(row[3])].add(row[2])
            number[key] = 1
