from neo4j.v1 import GraphDatabase
import os

driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "neo4jpass"))

## movies.dat must be in neo4j/import folder!
## Did this manually
def import_movies(tx):
    tx.run("LOAD CSV FROM 'file:///movies.dat' AS movie "
           "FIELDTERMINATOR  \'|\'"
           "MERGE (:Movie { movieID: toInteger(movie[0]), movieTitle: movie[1]})")

def import_ratings(tx):
    tx.run("LOAD CSV FROM 'file:///netIDs.data' AS ratings "
           "FIELDTERMINATOR \'\\t\' "
           "MATCH (m:Movie) "
           "WHERE m.movieID = toInteger(ratings[1]) "
           "MERGE (user:User {userID: toInteger(ratings[0])}) "
           "MERGE (user)-[r:RATED {rating: toFloat(ratings[2]), timestamp: toInteger(ratings[3])}]->(m)")

def query1(tx):
    tx.run("MATCH (u:User)-[r:RATED]->(m:Movie) "
           "WITH u, count(*) as moviesRated "
           "RETURN u.userID, moviesRated "
           "ORDER BY u.userID")

def query2(tx):
    tx.run("MATCH () -[r:RATED]->(m:Movie) "
           "WITH m, avg(r.rating) AS avgRating "
           "RETURN m.movieID, avgRating "
           "ORDER BY avgRating DESC "
           "LIMIT 10")

def query3(tx):
    for record in tx.run("MATCH () -[r:RATED]->(m:Movie) "
                         "WHERE r.rating < 3 "
                         "WITH DISTINCT m.movieTitle AS title "
                         "MATCH ()-[r:RATED]->(m:Movie) "
                         "WHERE m.movieTitle = title "
                         "RETURN m.movieTitle as Title, avg(r.rating) as AverageRating "
                         "ORDER BY AverageRating DESC LIMIT 10"):
        print(record["m.movieTitle"])


with driver.session() as session:
    session.write_transaction(import_movies)
    session.write_transaction(import_ratings)
    #session.write_transaction(query3)