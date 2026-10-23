// NODES 2026 demo queries. Run them in Neo4j Browser or Aura Query.

// 1. The whole estate at a glance
MATCH p = (:Table)-[:FEEDS]->(:Table) RETURN p;

// 2. Impact analysis: what breaks if the supplier feed changes?
MATCH p = (:Table {full_name: 'samples.bakehouse.sales_suppliers'})-[:FEEDS*1..6]->(t:Table)
RETURN p;

// 3. Root cause: everything upstream of a gold table
MATCH p = (src:Table)-[:FEEDS*1..6]->(clv:Table)
WHERE clv.full_name ENDS WITH '.nodes_gold.customer_lifetime_value'
RETURN p;

// 4. Governance: where does PII end up (column level)?
MATCH p = (c:Column:PII)-[:DERIVES*0..5]->(d:Column)<-[:HAS_COLUMN]-(t:Table)
RETURN p;

// 5. Gold tables exposing PII without a PII tag on the column (governance gap)
MATCH (c:Column:PII)-[:DERIVES*1..5]->(d:Column)<-[:HAS_COLUMN]-(t:Table {layer: 'gold'})
WHERE NOT d:PII
RETURN DISTINCT t.full_name AS table, d.name AS untagged_column, c.full_name AS origin;

// 6. Who / what writes into gold? (person → Lakeflow Job → notebook task → table)
MATCH p = (:User)-[:RAN]->(:Job)-[:HAS_TASK]->(:Notebook)-[:WRITES]->(:Table {layer: 'gold'}) RETURN p;

// 7. Most critical tables (most downstream dependents)
MATCH (t:Table)-[:FEEDS*1..6]->(d:Table)
RETURN t.full_name AS table, count(DISTINCT d) AS downstream ORDER BY downstream DESC LIMIT 10;

// 8. Impact + ownership: if the supplier feed changes, which tables break AND which job task do I call?
MATCH (s:Table)-[:FEEDS*1..5]->(t:Table)<-[:WRITES]-(nb:Notebook)<-[:HAS_TASK]-(j:Job)
WHERE s.full_name ENDS WITH '.nodes_bronze.suppliers'
RETURN DISTINCT t.name AS table, nb.name AS task, j.name AS job;
