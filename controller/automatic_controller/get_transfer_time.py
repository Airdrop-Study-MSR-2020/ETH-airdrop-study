import sys
import csv
from neo4j.v1 import GraphDatabase

driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "123456"))

def get_transfer_time(tx, address, begin_time, controlled_account):
    file1 = open(sys.argv[2], "a+")
    for row in tx.run("match (n:NODE)-[t:TOKEN_TRANSFER]->(m:NODE{address:$address}) "
                      "where n.address in $controlled_account and t.time > $begin_time "
                      "return n.address as account, t.time as time, t.to as to", address=address, controlled_account = controlled_account, begin_time = begin_time):
        file1.write("{0} {1} {2} {3}\n".format(address, row["to"], row["account"], row["time"]))

with open(sys.argv[1]) as csv_file:
    csv_reader = csv.reader(csv_file, delimiter=' ')
    previous_key = ""
    controlled_accounts = {}
    for row in csv_reader:
        key = row[0]+"_"+row[1]+"_"+row[2]
        if key in controlled_accounts:
            controlled_accounts[key].append(row[4]) 
        else:
            controlled_accounts[key] = []
            controlled_accounts[key].append(row[4])

for key, value in controlled_accounts.items():
    address = key.split("_")[0]
    from_time = int(key.split("_")[1])
    with driver.session() as session:
        session.read_transaction(get_transfer_time, address, from_time, value)
