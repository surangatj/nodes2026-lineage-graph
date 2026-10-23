# Databricks notebook source
# MAGIC %md
# MAGIC # Unity Catalog lineage → knowledge graph
# MAGIC **NODES 2026 · Exploring Databricks System Tables with Neo4j**
# MAGIC
# MAGIC 1. Read lineage from `system.access.table_lineage` / `column_lineage`
# MAGIC 2. Enrich with `information_schema` (columns, tags)
# MAGIC 3. Shape it into nodes + relationships (a graph is just two DataFrames)
# MAGIC 4. MERGE into Neo4j with the Python driver (works from serverless)
# MAGIC
# MAGIC Neo4j credentials live in a Databricks secret scope `neo4j` (keys: `uri`, `user`, `password`).

# COMMAND ----------

# MAGIC %pip install neo4j==5.28.1 --quiet

# COMMAND ----------

dbutils.widgets.text("catalog", "main")
dbutils.widgets.text("lookback_days", "30")
dbutils.widgets.dropdown("mask_user_emails", "true", ["true", "false"])  # keep emails off a live stream
dbutils.widgets.dropdown("rebuild", "true", ["true", "false"])           # MERGE never deletes: start clean
CATALOG = dbutils.widgets.get("catalog")
LOOKBACK = int(dbutils.widgets.get("lookback_days"))
MASK_EMAILS = dbutils.widgets.get("mask_user_emails") == "true"
REBUILD = dbutils.widgets.get("rebuild") == "true"
USER_EXPR = "regexp_replace(created_by, '@.*$', '')" if MASK_EMAILS else "created_by"

# COMMAND ----------

# MAGIC %md ## 1 · Table lineage: every row is an edge

# COMMAND ----------

table_edges = spark.sql(f"""
  SELECT source_table_full_name AS src,
         target_table_full_name AS dst,
         source_type, target_type,
         entity_type, entity_id, {USER_EXPR} AS created_by,
         entity_metadata.job_info.job_id AS job_id,
         entity_metadata.notebook_id     AS notebook_id,
         max(event_time) AS last_seen,
         count(*)        AS events
  FROM system.access.table_lineage
  WHERE event_date >= current_date() - INTERVAL {LOOKBACK} DAYS
    AND (source_table_catalog = '{CATALOG}' OR target_table_catalog = '{CATALOG}')
  GROUP BY ALL
""")
display(table_edges)

# COMMAND ----------

# MAGIC %md ## 2 · Column lineage: the fine-grained edges

# COMMAND ----------

column_edges = spark.sql(f"""
  SELECT concat(source_table_full_name, '.', source_column_name) AS src,
         concat(target_table_full_name, '.', target_column_name) AS dst,
         max(event_time) AS last_seen
  FROM system.access.column_lineage
  WHERE event_date >= current_date() - INTERVAL {LOOKBACK} DAYS
    AND target_table_catalog = '{CATALOG}'
    AND source_table_full_name IS NOT NULL AND target_table_full_name IS NOT NULL  -- path-only reads have no table name
    AND source_column_name IS NOT NULL AND target_column_name IS NOT NULL
  GROUP BY ALL
""")
display(column_edges)

# COMMAND ----------

# MAGIC %md ## 3 · Metadata: tables, columns, tags

# COMMAND ----------

tables = spark.sql(f"""
  SELECT concat_ws('.', table_catalog, table_schema, table_name) AS full_name,
         table_catalog AS catalog, table_schema AS schema, table_name AS name,
         table_type, table_owner AS owner, comment
  FROM {CATALOG}.information_schema.tables
  WHERE table_schema <> 'information_schema'
""")

columns = spark.sql(f"""
  SELECT concat_ws('.', table_catalog, table_schema, table_name, column_name) AS full_name,
         concat_ws('.', table_catalog, table_schema, table_name) AS table_full_name,
         column_name AS name, data_type
  FROM {CATALOG}.information_schema.columns
  WHERE table_schema <> 'information_schema'
""")

column_tags = spark.sql(f"""
  SELECT concat_ws('.', catalog_name, schema_name, table_name, column_name) AS full_name,
         tag_name, tag_value
  FROM {CATALOG}.information_schema.column_tags
""")

table_tags = spark.sql(f"""
  SELECT concat_ws('.', catalog_name, schema_name, table_name) AS full_name,
         tag_name, tag_value
  FROM {CATALOG}.information_schema.table_tags
""")

# COMMAND ----------

# MAGIC %md ## 4 · Load into Neo4j

# COMMAND ----------

from neo4j import GraphDatabase

uri = dbutils.secrets.get("neo4j", "uri").strip()
if "://" not in uri:  # accept a bare Aura host or instance id
    uri = "neo4j+s://" + (uri if "." in uri else f"{uri}.databases.neo4j.io")

driver = GraphDatabase.driver(
    uri,
    auth=(dbutils.secrets.get("neo4j", "user"), dbutils.secrets.get("neo4j", "password")),
)
driver.verify_connectivity()

def write(cypher, df, batch=1000):
    rows = df if isinstance(df, list) else [r.asDict() for r in df.collect()]
    with driver.session() as s:
        for i in range(0, len(rows), batch):
            s.run(cypher, rows=rows[i:i + batch]).consume()
    print(f"{len(rows):>6} rows  ←  {cypher.split(chr(10))[1].strip()[:70]}")

with driver.session() as s:
    if REBUILD:
        s.run("MATCH (n) DETACH DELETE n").consume()  # metadata graphs are small; batch this if yours isn't
    for stmt in [
        "CREATE CONSTRAINT table_fqn IF NOT EXISTS FOR (t:Table) REQUIRE t.full_name IS UNIQUE",
        "CREATE CONSTRAINT column_fqn IF NOT EXISTS FOR (c:Column) REQUIRE c.full_name IS UNIQUE",
        "CREATE CONSTRAINT process_id IF NOT EXISTS FOR (p:Process) REQUIRE p.key IS UNIQUE",
        "CREATE CONSTRAINT user_name IF NOT EXISTS FOR (u:User) REQUIRE u.name IS UNIQUE",
    ]:
        s.run(stmt)

# COMMAND ----------

write("""UNWIND $rows AS r
MERGE (c:Catalog {name: r.catalog})
MERGE (s:Schema {full_name: r.catalog + '.' + r.schema}) SET s.name = r.schema
MERGE (c)-[:CONTAINS]->(s)
MERGE (t:Table {full_name: r.full_name})
SET t.name = r.name, t.table_type = r.table_type, t.owner = r.owner, t.comment = r.comment,
    t.layer = CASE WHEN r.schema ENDS WITH 'bronze' THEN 'bronze'
                   WHEN r.schema ENDS WITH 'silver' THEN 'silver'
                   WHEN r.schema ENDS WITH 'gold'   THEN 'gold' END
MERGE (s)-[:CONTAINS]->(t)""", tables)

write("""UNWIND $rows AS r
MATCH (t:Table {full_name: r.table_full_name})
MERGE (c:Column {full_name: r.full_name}) SET c.name = r.name, c.data_type = r.data_type
MERGE (t)-[:HAS_COLUMN]->(c)""", columns)

write("""UNWIND $rows AS r
MATCH (c:Column {full_name: r.full_name})
MERGE (g:Tag {key: r.tag_name, value: r.tag_value})
MERGE (c)-[:TAGGED]->(g)
FOREACH (_ IN CASE WHEN r.tag_name = 'pii' THEN [1] ELSE [] END | SET c:PII)""", column_tags)

write("""UNWIND $rows AS r
MATCH (t:Table {full_name: r.full_name})
MERGE (g:Tag {key: r.tag_name, value: r.tag_value})
MERGE (t)-[:TAGGED]->(g)""", table_tags)

# COMMAND ----------

# Upstream sources outside the catalog (e.g. samples.bakehouse) still become Table nodes.
from pyspark.sql import functions as F

feeds = (table_edges.where("src IS NOT NULL AND dst IS NOT NULL")
         .groupBy("src", "dst")
         .agg(F.max("last_seen").alias("last_seen"), F.sum("events").alias("events")))

write("""UNWIND $rows AS r
MERGE (a:Table {full_name: r.src})
MERGE (b:Table {full_name: r.dst})
MERGE (a)-[f:FEEDS]->(b)
SET f.last_seen = r.last_seen, f.events = r.events""", feeds)

# COMMAND ----------

# MAGIC %md ### Who did it: jobs, notebook tasks, people
# MAGIC `entity_metadata` carries `job_info.job_id` and `notebook_id`; the Jobs and Workspace APIs turn those ids into names.

# COMMAND ----------

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
jobs, notebooks = {}, {}
for (job_id,) in table_edges.where("job_id IS NOT NULL").select("job_id").distinct().collect():
    try:
        job = w.jobs.get(int(job_id))
    except Exception as e:  # deleted job or no permission: keep the id only
        print(f"job {job_id}: {e}")
        continue
    jobs[job_id] = job.settings.name
    for t in job.settings.tasks or []:
        if t.notebook_task:
            try:
                nb_id = str(w.workspace.get_status(t.notebook_task.notebook_path).object_id)
                notebooks[nb_id] = {"path": t.notebook_task.notebook_path, "task_key": t.task_key}
            except Exception as e:
                print(f"{t.notebook_task.notebook_path}: {e}")

processes = []
for r in table_edges.where("entity_id IS NOT NULL AND src IS NOT NULL AND dst IS NOT NULL").collect():
    nb = notebooks.get(r.notebook_id or "", {})
    processes.append({**r.asDict(), "job_name": jobs.get(r.job_id), "nb_path": nb.get("path"), "task_key": nb.get("task_key")})

# A notebook task is the Process that READS/WRITES; its Job groups the tasks; the User ran the Job.
write("""UNWIND $rows AS r
MATCH (a:Table {full_name: r.src}), (b:Table {full_name: r.dst})
MERGE (p:Process {key: CASE WHEN r.notebook_id IS NOT NULL THEN 'NOTEBOOK:' + r.notebook_id
                            ELSE r.entity_type + ':' + r.entity_id END})
SET p.type = CASE WHEN r.notebook_id IS NOT NULL THEN 'NOTEBOOK' ELSE r.entity_type END,
    p.name = coalesce(r.task_key, r.nb_path, r.entity_type + ' ' + r.entity_id),
    p.path = r.nb_path
FOREACH (_ IN CASE WHEN r.notebook_id IS NOT NULL THEN [1] ELSE [] END | SET p:Notebook)
MERGE (p)-[:READS]->(a)
MERGE (p)-[:WRITES]->(b)
FOREACH (_ IN CASE WHEN r.job_id IS NOT NULL THEN [1] ELSE [] END |
  MERGE (j:Process:Job {key: 'JOB:' + r.job_id})
  SET j.type = 'JOB', j.job_id = r.job_id, j.name = coalesce(r.job_name, 'job ' + r.job_id)
  MERGE (j)-[:HAS_TASK]->(p))
WITH r, p WHERE r.created_by IS NOT NULL
MERGE (u:User {name: r.created_by})
WITH r, p, u
OPTIONAL MATCH (j:Job {key: 'JOB:' + r.job_id})
FOREACH (_ IN CASE WHEN j IS NULL THEN [1] ELSE [] END | MERGE (u)-[:RAN]->(p))
FOREACH (_ IN CASE WHEN j IS NOT NULL THEN [1] ELSE [] END | MERGE (u)-[:RAN]->(j))""", processes)

write("""UNWIND $rows AS r
MERGE (a:Column {full_name: r.src})
MERGE (b:Column {full_name: r.dst})
MERGE (a)-[d:DERIVES]->(b) SET d.last_seen = r.last_seen""", column_edges)

# COMMAND ----------

# MAGIC %md ## 5 · Ask the graph a question: where does PII flow?

# COMMAND ----------

with driver.session() as s:
    res = s.run("""
      MATCH path = (src:Column:PII)-[:DERIVES*1..5]->(dst:Column)
      RETURN src.full_name AS pii_source, dst.full_name AS reaches, length(path) AS hops
      ORDER BY hops DESC""").data()
    counts = s.run("""
      CALL () { MATCH (n) RETURN count(n) AS nodes }
      CALL () { MATCH ()-[r]->() RETURN count(r) AS rels }
      RETURN nodes, rels""").single().data()
display(spark.createDataFrame(res) if res else spark.createDataFrame([], "pii_source string, reaches string, hops int"))
driver.close()

import json
dbutils.notebook.exit(json.dumps({**counts, "pii_paths": len(res)}))
