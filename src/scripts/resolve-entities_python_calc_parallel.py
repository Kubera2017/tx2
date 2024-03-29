#
# Build index first from Neo4j browser:
# CREATE INDEX ON :Entity(name)
# And wait until index ONLINE 100%, to check this run from Neo4j browser:
# CALL db.indexes()
#

from neo4j import GraphDatabase
import textdistance
from concurrent.futures import ThreadPoolExecutor, wait, ALL_COMPLETED, as_completed

uri             = "bolt://localhost:7687"

userName        = "neo4j"

password        = "123"

graphDB_Driver  = GraphDatabase.driver(uri, auth=(userName, password))

threads         = 3

def get_group_by_staring_with(tx, string):
    return tx.run(
    "MATCH (n:Entity) " + 
    "WHERE n.name STARTS WITH $string " + 
    "WITH CASE WHEN n IS NOT NULL THEN collect(id(n)) ELSE [] END AS list " + 
    "RETURN list", 
    string=string
    )

def get_group_by_staring_with_for_parallel(string):
    with graphDB_Driver.session() as session:
        result = session.read_transaction(get_group_by_staring_with, string).data()
        session.close()
        if len(result) > 0:
            result = {'starts': string, 'ids': result[0].get("list")}
        else:
            result = {'starts': string, 'ids': []}
    return result

def mark_and_get_dup_pairs(tx, list, temp_label):
    return tx.run(
    "MATCH (n) WHERE id(n) IN $list " + 
    "WITH collect(n) AS list " + 
    'CALL apoc.create.addLabels(list, [$temp_label]) YIELD node ' + 
    "WITH collect(node) AS list " +
    "UNWIND list AS n1 " +
    "UNWIND list AS n2 " +
    "WITH n1, n2 " +
    "WHERE id(n1) > id(n2) " +
    "RETURN {id: id(n1), name: n1.name } AS A, {id: id(n2), name: n2.name } AS B", 
    list=list, temp_label=temp_label
    )

def calculate_score(A, B):
    bankA = A.get("name")
    bankB = B.get("name")
    score = textdistance.sorensen_dice.similarity(bankA, bankB)
    return score

def mark_similar_nodes(tx, id1, id2):
    return tx.run(
    "MATCH (n1) WHERE id(n1) = $id1 " + 
    "MATCH (n2) WHERE id(n2) = $id2 " + 
    "MERGE (n1)-[:SIMILAR_TO]-(n2) ", 
    id1=id1, id2=id2
    )

def _calc_and_mark(pair):
    A = pair.get("A")
    B = pair.get("B")
    if calculate_score(A, B) > 0.85:
        with graphDB_Driver.session() as session:
            session.write_transaction(mark_similar_nodes, A.get("id"), B.get("id"))
            session.close()
        return True
    else:
        return False

def detect_similarity_communities(tx, i, temp_label):
    return tx.run(
    "CALL algo.louvain.stream($temp_label, 'SIMILAR_TO', {}) " + 
    "YIELD nodeId, community " + 
    "WITH algo.getNodeById(nodeId) AS member, community " + 
    "WITH community, collect(member) AS members " +
    "WHERE size(members) > 1 " +
    "UNWIND members AS member " +
    'MERGE (c:EntityCommunity {id: toString($i) + "_" + toString(community)}) ' +
    "MERGE (c)-[:COMMUNITY_MEMBER]->(member) ", 
    temp_label=temp_label, i=i
    )

def clean_up(tx, list, temp_label):
    return tx.run(
    "MATCH (n) WHERE id(n) IN $list " + 
    "WITH collect(n) AS list " + 
    "CALL apoc.create.removeLabels(list, [$temp_label]) YIELD node " + 
    "UNWIND list AS n " +
    "MATCH (n)-[r:SIMILAR_TO]->() " +
    "DELETE r", 
    list=list, temp_label=temp_label
    )

with graphDB_Driver.session() as session:
    alphabet = list("abcdefghijklmnopqrstuvwxyz0123456789 ".upper())

    starts = []
    for l1 in alphabet:
        for l2 in alphabet:
            for l3 in alphabet:
                for l4 in alphabet:
                    string = "".join([l1, l2, l3, l4])
                    starts.append(string)

    dupGroups = []

    print("Getting similarity groups")
    pool = ThreadPoolExecutor(3)
    futures = []
    for string in starts:
        futures.append(pool.submit(get_group_by_staring_with_for_parallel, string))
    for future in as_completed(futures):
        res = future.result()
        if len(res.get('ids')) > 1:
            dupGroups.append(res)
    print("Total groups", len(dupGroups))

    for i, group in enumerate(dupGroups):
        print("Starts with", group.get('starts'), "Members", len(group.get('ids')))
        temp_label = "POTENTIAL_DUP_ENT_GROUP_" + str(i)
        dupPairs = session.write_transaction(mark_and_get_dup_pairs, group.get('ids'), temp_label).data()

        print("Resolving similarities. Pairs:", len(dupPairs))
        pool = ThreadPoolExecutor(threads)
        futures = []
        for pair in dupPairs:
            futures.append(pool.submit(_calc_and_mark, pair))
        similar_nodes_founded = False
        for future in as_completed(futures):
            res = future.result()
            if res == True:
                similar_nodes_founded = True
        if similar_nodes_founded == True:
            print("Detect communuties")
            session.write_transaction(detect_similarity_communities, i, temp_label)
    
        session.write_transaction(clean_up, group.get('ids'), temp_label)

    session.close()
graphDB_Driver.close()
