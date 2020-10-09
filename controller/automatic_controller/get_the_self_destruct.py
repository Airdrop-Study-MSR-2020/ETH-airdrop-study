import sys
import csv
from neo4j.v1 import GraphDatabase

driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "123456"))
true_number = 0
def get_selfdestruct(tx, token_address, controller, controlled_account):
    file1 = open(sys.argv[2], "a+")
    result = tx.run("match (p:NODE)-[t1:SELFDESTRUCT]->(q:NODE) "
                    "where p.address in $controlled_account "
                    "return count(distinct(p.address))", controlled_account = controlled_account).single()[0]
    if result > 0:
        file1.write("{0} {1} {2}\n".format(token_address, controller, len(controlled_account)))



with open(sys.argv[1]) as csv_file:
    csv_reader = csv.reader(csv_file, delimiter=' ')
    controlled_accounts = {}
    for row in csv_reader:
        key = row[0]+"_"+row[1]+"_"+row[2] + "_" + row[3]
        if key in controlled_accounts:
            controlled_accounts[key].append(row[4]) 
        else:
            controlled_accounts[key] = []
            controlled_accounts[key].append(row[4])

for key, value in controlled_accounts.items():
    #print key, len(value)
    address = key.split("_")[0]
    controller = key.split("_")[3]
    with driver.session() as session:
        session.read_transaction(get_selfdestruct, address, controller, value)
