import sys
import csv
import requests

template = "https://api.bloxy.info/token/token_info?token={0}&key=ACCU3un69T0MT&format=table"

with open("reactive_airdrop.csv") as csv_file:
    csv_reader = csv.reader(csv_file, delimiter=" ")
    file1 = open("reactive_airdrop_info.csv", "w+")
    for row in csv_reader:
        url = template.format(row[0])
        print row[0]
        response = requests.get(url)
        data = response.json()
        for item in data:
            try:
                file1.write("{0},{1},{2},{3},{4}\n".format(item[0], item[1], item[2], item[3], item[4]))
            except:
                print row[0] + " error !"
