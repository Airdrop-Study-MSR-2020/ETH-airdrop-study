import sys
import csv
from neo4j.v1 import GraphDatabase

driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "123456"))

def get_airdropper_involved_token_number(tx, airdropper, from_time, end_time):
    number = tx.run("match (n:NODE{address:$airdropper})-[t:TOKEN_TRANSFER]->(m:NODE) "
                    "where t.time >= $from_time and t.time <= $end_time "
                    "return count(distinct(m.address))",airdropper=airdropper, from_time=from_time, end_time=end_time).single()[0]
    if number > 1:
        return False
    else:
        return True

#step 1 airdropper result
with open(sys.argv[1]) as csv_file:
    file1 = open("new_step2_airdropper.csv", "w+")
    file2 = open("failed_airdropper.csv", "w+")
    csv_reader = csv.reader(csv_file, delimiter=' ')
    for row in csv_reader:
        print row[0], row[1]
        if row[1] == "0x0000000000000000000000000000000000000000":
            file1.write("{0} {1} {2} {3} {4} {5} {6}\n".format(row[0], row[1], row[2], row[3], row[4], row[5], row[6]))
            continue
        with driver.session() as session:
            result = session.read_transaction(get_airdropper_involved_token_number, row[1], int(row[5]), int(row[6]))
        if result:
            file1.write("{0} {1} {2} {3} {4} {5} {6}\n".format(row[0], row[1], row[2], row[3], row[4], row[5], row[6]))
        else:
            file2.write("{0} {1} {2} {3} {4} {5} {6}\n".format(row[0], row[1], row[2], row[3], row[4], row[5], row[6]))
