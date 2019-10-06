from neo4j import GraphDatabase
import textdistance
from concurrent.futures import ThreadPoolExecutor, wait, ALL_COMPLETED, as_completed

uri             = "bolt://localhost:7687"

userName        = "neo4j"

password        = "123"

graphDB_Driver  = GraphDatabase.driver(uri, auth=(userName, password))

def get_group_by_staring_with(tx, string):
    return tx.run(
    "MATCH (n:Entity) " + 
    "WHERE n.name STARTS WITH $string " + 
    "WITH CASE WHEN n IS NOT NULL THEN collect(n) ELSE [] END AS list " + 
    "RETURN list", 
    string=string
    )

def get_group_by_staring_with_for_parallel(string):
    with graphDB_Driver.session() as session:
        result = session.read_transaction(get_group_by_staring_with, string).data()
        session.close()
        if len(result) > 0:
            result = result[0].get("list")
        else:
            result = []
    return result

with graphDB_Driver.session() as session:
    alphabet = list("abcdefghijklmnopqrstuvwxyz0123456789 ".upper())

    starts = []
    for l1 in alphabet:
        for l2 in alphabet:
            for l3 in alphabet:
                string = "".join([l1, l2, l3])
                starts.append(string)

    dupGroups = []

    print("Getting similarity groups")
    pool = ThreadPoolExecutor(3)
    futures = []
    for string in starts:
        futures.append(pool.submit(get_group_by_staring_with_for_parallel, string))
    for future in as_completed(futures):
        res = future.result()
        if len(res) > 1:
            dupGroups.append(res)
    print("Total groups", len(dupGroups))

    session.close()
graphDB_Driver.close()
