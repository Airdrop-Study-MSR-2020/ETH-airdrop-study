import sys
import time
import csv
import bisect
import burst_detection as bd
import numpy as np
from neo4j.v1 import GraphDatabase

driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "123456"))
time_list = []
first_timestamp = 0

def init_time_list():
    global time_list
    time_list = []
    init_timestamp = 1438214400
    end_timestamp = 1546214400
    for i in range(0, 10000):
        current_timestamp = init_timestamp + i * 86400
        if current_timestamp > end_timestamp:
            break
        else:
            time_list.append(current_timestamp)

def get_total_events(tx, token_address):
    global time_list
    global first_timestamp
    events = []
    i = 0
    timestamp = 0
    day_event = 0
    first_timestamp = 0
    for row in tx.run("match (n:NODE)-[t:TOKEN_TRANSFER]->(m:NODE{address:$token_address}) "
                      "where t.time <= 1546214400 "
                      "return t.time as time order by t.time", token_address=token_address):
        if i == 0:
            index = bisect.bisect_left(time_list, row["time"])
            timestamp = time_list[index]
            first_timestamp = timestamp
            i += 1
        if row["time"] <= timestamp:
            day_event += 1
        else:
            events.append(day_event)
            day_event = 0
            timestamp += 86400
            while (row["time"] > timestamp):
                events.append(0)
                timestamp += 86400
            day_event += 1
    events.append(day_event)
    maximum_day_event = max(events) + 10
    total_events = []
    for i in range(len(events)):
        total_events.append(maximum_day_event)
    file1 = open("burst_probability.csv", "a+")
    
    r = np.array(events, dtype=float)
    d = np.array(total_events, dtype=float)
    n = len(r)
    q,d,r,p = bd.burst_detection(r,d,n,s=1.75, gamma=1, smooth_win=3)
    file1.write("{0} {1} {2}\n".format(token_address, p[0], p[1]))
    bursts = bd.enumerate_bursts(q, 'burstLabel')
    return bursts

def get_potential_airdropper(tx, token_address, begin, end):
    sender_frequency = {}
    total = 0
    for row in tx.run("match (n:NODE)-[t:TOKEN_TRANSFER]->(m:NODE{address:$token_address}) "
                      "where t.time > $begin and t.time <= $end and not exists (n.EX) and not exists (n.others) "
                      "return n.address as addr "
                      "order by t.time", token_address=token_address, begin=int(begin), end=int(end)):
        sender_frequency[row["addr"]] = sender_frequency.get(row["addr"], 0) + 1
        total += 1
    sorted_result = sorted(sender_frequency.items(), key=lambda x:x[1], reverse=True)
    file1 = open("potential_airdropper.csv", "a+")
    i = 0
    for result in sorted_result:
        if i == 0 and float(result[1])/total >= 0.8 and result[1] > 50:
            file1.write("{0} {1} {2} {3} {4} {5} {6}\n".format(token_address, result[0], result[1], total, float(result[1])/total, begin, end))
            break
        i += 1
        if float(result[1])/total >= 0.1 or result[1] > 50:
            file1.write("{0} {1} {2} {3} {4} {5} {6}\n".format(token_address, result[0], result[1], total, float(result[1])/total, begin, end))

#the input file is the token address
with open(sys.argv[1]) as csv_file:
    init_time_list()
    csv_reader = csv.reader(csv_file,delimiter=' ')
    file1 = open("potential_periods.csv", "a+")
    for row in csv_reader:
        with driver.session() as session:
            print row[0], row[1]
            bursts = session.read_transaction(get_total_events, row[0])
            for index, r in bursts.iterrows():
                file1.write("{0} {1} {2}\n".format(row[0], first_timestamp+(r['begin'] -1)*86400, first_timestamp+r['end']*86400))
                session.read_transaction(get_potential_airdropper, row[0], first_timestamp+(r['begin']-1)*86400, first_timestamp+r['end']*86400)
