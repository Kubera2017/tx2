from neo4j import GraphDatabase

uri             = "bolt://localhost:7687"

userName        = "neo4j"

password        = "123"

graphDB_Driver  = GraphDatabase.driver(uri, auth=(userName, password))

def get_dup_groups(tx):
    return tx.run(
    "MATCH (n:BankAccount) " + 
    "WHERE NOT n.account IS NULL " + 
    "WITH n.account AS account, collect(id(n)) AS dups " + 
    "WITH account, dups, size(dups) AS s " +
    "WHERE s > 1 "
    "RETURN dups")

def mark_and_compute_pairs_similarity(tx, list, temp_label):
    return tx.run(
    "MATCH (n) WHERE id(n) IN $list " + 
    "WITH collect(n) AS list " + 
    'CALL apoc.create.addLabels(list, [$temp_label]) YIELD node ' + 
    "WITH collect(node) AS list " +
    "UNWIND list AS n1 " +
    "UNWIND list AS n2 " +
    "WITH n1, n2 " +
    "WHERE id(n1) > id(n2) " +
    "WITH n1, n2, " +
    "apoc.text.sorensenDiceSimilarity(n1.bank, n2.bank) as sorensenScore " +
    "WHERE sorensenScore > 0.7 " +
    "MERGE (n1)-[:SIMILAR_TO]-(n2) ", 
    list=list, temp_label=temp_label
    )

def detect_similarity_communities(tx, i, temp_label):
    return tx.run(
    "CALL algo.louvain.stream($temp_label, 'SIMILAR_TO', {}) " + 
    "YIELD nodeId, community " + 
    "WITH algo.getNodeById(nodeId) AS member, community " + 
    'MERGE (c:BankAccountCommunity {id: toString($i) + "_" + toString(community)}) ' +
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
    dups = session.read_transaction(get_dup_groups).data()
    total = len(dups)

    print("Total groups:", total)

    for i, dup in enumerate(dups):
        group = dup.get("dups")
        print("Group", i, "Members:", len(group))
        temp_label = "POTENTIAL_DUP_ACC_GROUP_" + str(i)
        session.write_transaction(mark_and_compute_pairs_similarity, group, temp_label)
        session.write_transaction(detect_similarity_communities, i, temp_label)
        session.write_transaction(clean_up, group, temp_label)

graphDB_Driver.close()
