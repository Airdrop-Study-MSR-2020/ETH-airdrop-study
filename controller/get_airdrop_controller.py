import sys
import csv
import collections
from neo4j.v1 import GraphDatabase

driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "123456"))

exchange_list = [] 
sender_list = []
def get_exchange(tx):
    global exchange_list
    for row in tx.run("match (n:NODE) where exists (n.EX) return n.address as addr"):
        exchange_list.append(row["addr"])

def get_airdrop_receiver(tx, token_addr, airdropper_list, from_time, end_time):
    global exchange_list
    global sender_list
    sender_list = []
    sender_set = set()
    for row in tx.run("match (n:NODE)-[t:TOKEN_TRANSFER]->(m:NODE{address:$token_addr}) "
            "where n.address in $airdropper_list and t.time >= $from_time and t.time <= $end_time "
            "with t.to as from "
            "match (p:NODE{address:from})-[t1:TOKEN_TRANSFER]->(q:NODE{address:$token_addr}) "
            "where not t1.to in $exchange_list and not t1.to in $airdropper_list "
            "return p.address as sender, count(distinct(t1.to)) as recv_num", exchange_list=exchange_list, token_addr=token_addr, airdropper_list=airdropper_list, from_time=from_time, end_time=end_time):
        if row["recv_num"]==1:
            sender_list.append(row["sender"])
            sender_set.add(row["sender"])

def get_controller(tx, token_addr, airdropper_list, from_time, end_time):
    global sender_list
    file1 = open(sys.argv[2], "a+")
    file2 = open(sys.argv[3], "a+")
    controller_map = {}
    for row in tx.run("match (n:NODE)-[t:TOKEN_TRANSFER]->(m:NODE{address:$token_addr}) "
            "where n.address in $airdropper_list and t.time >= $from_time and t.time <=$end_time and t.to in $sender_list "
            "with t.to as potential_controlled_account, t.time as time "
            "match (p:NODE{address:potential_controlled_account})-[t1:TOKEN_TRANSFER]->(q:NODE{address:$token_addr}) "
            "return p.address as controlled_account, t1.to as controller", sender_list = sender_list, token_addr=token_addr, airdropper_list=airdropper_list, from_time=from_time, end_time=end_time):
        if row["controller"] in controller_map:
            value = controller_map[row["controller"]]
        else:
            value = set()
        value.add(row["controlled_account"])
        controller_map[row["controller"]] = value
    print len(controller_map)
    for key, value in controller_map.items():
        if len(value) > 1:
            file1.write("{0} {1} {2} {3} {4}\n".format(token_addr, from_time, end_time, key, len(value)))
            for v in value:
                file2.write("{0} {1} {2} {3} {4}\n".format(token_addr, from_time, end_time, key, v))

with driver.session() as session:
    session.read_transaction(get_exchange)
with open(sys.argv[1]) as csv_file:
    csv_reader = csv.reader(csv_file, delimiter=' ')
    previous_key = ""
    airdropper_list = []
    for row in csv_reader:
        print row[0], row[1]
        key = row[0]+"_"+row[2]+"_"+row[3]
        if key == previous_key:
            airdropper_list.append(row[1])
        else:
            if previous_key != "":
                token_addr = previous_key.split("_")[0]
                from_time = int(previous_key.split("_")[1])
                end_time = int(previous_key.split("_")[2])
                with driver.session() as session:
                    session.read_transaction(get_airdrop_receiver, token_addr, airdropper_list, from_time, end_time)
                    session.read_transaction(get_controller, token_addr, airdropper_list, from_time, end_time)
            previous_key = key
            airdropper_list = []
            airdropper_list.append(row[1])
    token_addr = previous_key.split("_")[0]
    from_time = int(previous_key.split("_")[1])
    end_time = int(previous_key.split("_")[2])
    with driver.session() as session:
        session.read_transaction(get_airdrop_receiver, token_addr, airdropper_list, from_time, end_time)
        session.read_transaction(get_controller, token_addr, airdropper_list, from_time, end_time)
