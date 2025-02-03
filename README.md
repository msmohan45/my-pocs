This is POC for Spark Streaming Ingestion Pipeline
Plan - 
1. Spin up Docker containers for Spark cluster - Master and Data Nodes
2. Spark Consumer code to consume Kafka Messages
  2.1. Parse the data
  2.2. Apply Transformations
  2.3. Write to Postgres in batches
3. Evaluate the performance
