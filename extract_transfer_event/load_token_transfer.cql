USING PERIODIC COMMIT 100000
LOAD CSV FROM "file:///transfergz" AS line
MERGE (n:NODE{address:line[1]})
MERGE (m:NODE{address:line[0]})
CREATE (n)-[t:TOKEN_TRANSFER{to:line[2], value:toFloat(line[3]), time:toInt(line[5])}]->(m);