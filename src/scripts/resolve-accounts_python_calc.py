from neo4j import GraphDatabase
import textdistance

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
    "RETURN {id: id(n1), account: n1.account, bank: n1.bank } AS A, {id: id(n2), account: n2.account, bank: n2.bank } AS B", 
    list=list, temp_label=temp_label
    )

def calculate_score(A, B):
    bankA = A.get("bank")
    bankB = B.get("bank")
    score = textdistance.sorensen_dice.similarity(bankA, bankB)
    return score

def mark_similar_nodes(tx, id1, id2):
    return tx.run(
    "MATCH (n1) WHERE id(n1) = $id1 " + 
    "MATCH (n2) WHERE id(n2) = $id2 " + 
    "MERGE (n1)-[:SIMILAR_TO]-(n2) ", 
    id1=id1, id2=id2
    )

def detect_similarity_communities(tx, i, temp_label):
    return tx.run(
    "CALL algo.louvain.stream($temp_label, 'SIMILAR_TO', {}) " + 
    "YIELD nodeId, community " + 
    "WITH algo.getNodeById(nodeId) AS member, community " + 
    "WITH community, collect(member) AS members " +
    "WHERE size(members) > 1 " +
    "UNWIND members AS member " +
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
        print("Group", i, "/", len(dups), "Members:", len(group))
        temp_label = "POTENTIAL_DUP_ACC_GROUP_" + str(i)
        dupPairs = session.write_transaction(mark_and_get_dup_pairs, group, temp_label).data()
        print("Group", i, "/", len(dups), "Resolving similarities. Pairs:", len(dupPairs))
        similar_nodes_founded = False
        for j, pair in enumerate(dupPairs):
            print("Group", i, "/", len(dups), "Processing", j + 1, "/", len(dupPairs) )
            A = pair.get("A")
            B = pair.get("B")
            if calculate_score(A, B) > 0.85:
                session.write_transaction(mark_similar_nodes, A.get("id"), B.get("id"))
                similar_nodes_founded = True
        if similar_nodes_founded == True:
            print("Group", i, "/", len(dups), "Detect communuties")
            session.write_transaction(detect_similarity_communities, i, temp_label)

        session.write_transaction(clean_up, group, temp_label)
        print("Group", i, "/", len(dups), "Done")

    session.close()
graphDB_Driver.close()
