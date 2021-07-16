# (stream:Kinesis)-[:WRITES_TO]->(graph:Neo4j)

Spark pipeline to write data streams from Kinesis to Neo4j.

## Connector 4.0.3 Patch
This is a temporary workaround to install the Neo4j Connector for Apache Spark 4.0.3, since it's not released yet. 

```bash
mvn install:install-file \
   -Dfile="jars/neo4j-connector-apache-spark_2.12-4.0.3_for_spark_3.jar" \
   -DgroupId=org.neo4j \
   -DartifactId=neo4j-connector-apache-spark_2.12 \
   -Dversion=4.0.3_for_spark_3 \
   -Dpackaging=jar \
   -DgeneratePom=true
```
