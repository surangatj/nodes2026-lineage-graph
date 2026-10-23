# Unity Catalog lineage as a Neo4j graph

Code for my NODES 2026 talk, [Exploring Databricks System Tables with Neo4j](https://neo4j.com/nodes/agenda/exploring-databricks-system-tables-with-neo4j/) (Data Intelligence track, 12 Nov).

Unity Catalog already records table and column lineage in `system.access`. This repo takes those rows and turns them into a graph you can query with Cypher: impact analysis, root cause, where PII ends up, and which job writes what.

It's one Lakeflow Job with four serverless tasks:

```
bronze -> silver -> gold -> publish_lineage_graph
```

The first three just build a small medallion on top of `samples.bakehouse`, so there's real lineage to read. The last task does the actual work:

1. reads `system.access.table_lineage` and `system.access.column_lineage`
2. adds columns and tags from `information_schema`
3. looks up job and task names for each lineage row
4. MERGEs it all into Neo4j with the Python driver

## Graph model

```
(:Catalog)-[:CONTAINS]->(:Schema)-[:CONTAINS]->(:Table {layer})-[:HAS_COLUMN]->(:Column[:PII])
(:Table)-[:FEEDS]->(:Table)                  // table lineage
(:Column)-[:DERIVES]->(:Column)              // column lineage
(:Job)-[:HAS_TASK]->(:Notebook)              // from entity_metadata + Jobs API
(:Process)-[:READS|WRITES]->(:Table)         // a notebook task, or a pipeline / query / dashboard
(:User)-[:RAN]->(:Job|:Process)
(:Table|Column)-[:TAGGED]->(:Tag {key, value})
```

## Before you start

You need three things:

- **Access to the lineage tables.** By default only users who are both account admin and metastore admin can read system tables. Everyone else needs `USE CATALOG` on `system` plus `USE SCHEMA` and `SELECT` on `system.access`. Ask for this early, it took me longer than anything else.
- **A Neo4j instance.** AuraDB Free is plenty.
- **A secret scope called `neo4j`** with `uri`, `user` and `password`. On newer Aura instances the username is the 8-character instance ID, not `neo4j`.

```bash
databricks secrets create-scope neo4j --profile <profile>
databricks secrets put-secret neo4j uri --profile <profile>        # neo4j+s://xxxxxxxx.databases.neo4j.io
databricks secrets put-secret neo4j user --profile <profile>
databricks secrets put-secret neo4j password --profile <profile>
```

## Deploy and run

```bash
databricks bundle deploy -t dev --profile <profile>
databricks bundle run bakehouse_medallion_to_graph -t dev --profile <profile>
```

The catalog defaults to `main`. Use `--var catalog=<name>` for a different one, or put it in `.databricks/bundle/dev/variable-overrides.json` so it stays out of git:

```json
{"catalog": "<name>"}
```

The system tables lag a few minutes behind, so lineage from a fresh run usually shows up in the graph on the next publish.

## Publish task options

| Widget | Default | What it does |
|---|---|---|
| `lookback_days` | `30` | How far back to read lineage |
| `mask_user_emails` | `true` | Keeps only the part of `created_by` before the `@`, so emails don't end up in Neo4j or on screen |
| `rebuild` | `true` | Clears the graph before loading. `MERGE` never deletes, so without this, removed tags and tables hang around |

## Why not the Neo4j Spark Connector?

It needs dedicated access mode and doesn't run on serverless. Lineage metadata is small, so the plain Python driver does the job.

## Demo queries

[`demo_queries.cypher`](demo_queries.cypher) has the queries from the talk: impact analysis, root cause, PII flow, untagged PII in gold, the most critical tables, and who writes into gold.
