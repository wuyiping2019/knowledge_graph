from neo4j import GraphDatabase
import logging
from neo4j.exceptions import ServiceUnavailable


class Neo4jClient:

    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def create_relation(self, entity1Name, entity1Type, entity2Name, entity2Type, relation):
        with self.driver.session() as session:
            # Write transactions allow the driver to handle retries and transient errors
            result = session.write_transaction(self._create_and_return_relation, entity1Name, entity1Type, entity2Name, entity2Type, relation)
            for row in result:
                print("Created {relation} between: {p1}, {p2}".format(p1=row['p1'], p2=row['p2'], relation=relation))

    @staticmethod
    def _create_and_return_relation(tx, entity1Name, entity1Type, entity2Name, entity2Type, relation):
        query = (
                "CREATE (p1:" + entity1Type + "{name: $entity1Name })"
                "CREATE (p2:" + entity2Type + "{name: $entity2Name })"
                "CREATE (p1)-[:" + relation + "]->(p2)"
                "RETURN p1, p2"
        )
        result = tx.run(query, entity1Name=entity1Name, entity2Name=entity2Name)
        try:
            return [{"p1": row["p1"]["name"], "p2": row["p2"]["name"]} for row in result]
        except ServiceUnavailable as exception:
            logging.error("{query} raised an error: \n {exception}".format(
                query=query, exception=exception))
            raise

    def find_entity(self, entityName, entityType):
        with self.driver.session() as session:
            result = session.read_transaction(self._find_and_return_entity, entityName, entityType)
            for row in result:
                print("Found {entityName}: {row}".format(row=row, entityName=entityName))

    @staticmethod
    def _find_and_return_entity(tx, entityName, entityType):
        query = (
                "MATCH (p:" + entityType + ") "
                "WHERE p.name = $entityName "
                "RETURN p.name AS name"
        )
        result = tx.run(query, entityName=entityName)
        return [row["name"] for row in result]


if __name__ == "__main__":
    # Aura queries use an encrypted connection using the "neo4j+s" URI scheme
    uri = "neo4j+s://2e4ff1c6.databases.neo4j.io:7687"
    user = "neo4j"
    password = "9y7RqIvuUin9Eka0U6qS1fq-mFS4S2ldUbrCWOvnpnE"
    app = Neo4jClient(uri, user, password)
    app.create_relation("工银安盛人寿保险有限公司", "InsuranceCompany", "工银安盛人寿团体一年定期寿险(B款)", "InsuranceProduct", "售卖")
    app.find_entity("工银安盛人寿保险有限公司", "InsuranceCompany")
    app.close()
