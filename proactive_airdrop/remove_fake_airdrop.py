import sys
import csv
from neo4j.v1 import GraphDatabase

driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "123456"))

T = float(sys.argv[3])
def get_number_of_unique_sender(tx, token_address, airdropper, from_time, end_time):
    total = 0;
    unique = set()
    for row in tx.run("match (n:NODE{address:$airdropper})-[t:TOKEN_TRANSFER]->(m:NODE{address:$token_address}) "
                      "where t.time >= $from_time and t.time <= $end_time "
                      "return t.to as to", airdropper=airdropper, token_address=token_address, from_time=from_time, end_time=end_time):
        total += 1
        unique.add(row["to"])
    if len(unique) > 50 and float(len(unique))/total >= T:
        return True
    else:
        return False

# results from the remove_outlier
with open(sys.argv[1]) as csv_file:
    csv_reader = csv.reader(csv_file, delimiter=' ')
    file1 = open(sys.argv[2], "a+")
    for row in csv_reader:
        print row[0], row[1]
        with driver.session() as session:
            result = session.read_transaction(get_number_of_unique_sender, row[0], row[1], int(row[5]), int(row[6]))
            if result:
                file1.write("{0} {1} {2} {3} {4} {5} {6}\n".format(row[0], row[1], row[2], row[3], row[4], row[5], row[6]))
