import os
import string

first_loop = list(string.ascii_lowercase)[:7]
second_loop = list(string.ascii_lowercase)
for i in first_loop:
    for j in second_loop:
        filepath = "file:///transfer"+str(i)+str(j)
        file1 = open("load_token_transfer.cql", "w+")
        file1.write("USING PERIODIC COMMIT 100000\n")
        file1.write('LOAD CSV FROM "{0}" AS line\n'.format(filepath))
        file1.write("MERGE (n:NODE{address:line[1]})\n")
        file1.write("MERGE (m:NODE{address:line[0]})\n")
        file1.write("CREATE (n)-[t:TOKEN_TRANSFER{to:line[2], value:toFloat(line[3]), time:toInt(line[5])}]->(m);")
        os.system("cat ./load_token_transfer.cql | /usr/bin/cypher-shell -u neo4j -p 123456")
