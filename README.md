# Lineage Impact
## Youtube video https://www.youtube.com/watch?v=1himkwnKP3Y

A Python tool for generating large-scale data lineage graphs with customizable parameters.

## Overview

The LineageGraphGenerator creates realistic data lineage graphs that model relationships between various data assets across different teams and data sources. These graphs can be used for:

- Testing graph visualization and analysis tools
- Benchmarking graph database performance
- Simulating data lineage scenarios
- Developing and testing impact analysis algorithms

## Features

- Generate large graphs (100K+ nodes)
- Multiple data source types (warehouse, database, BI, etc.)
- Realistic asset types with proper restrictions
- Team-based ownership
- Field-level lineage connections
- Various output formats (JSON, GEXF, GraphML)

## Data Source Restrictions

Each data source is restricted to specific asset types it can contain:

- **Data Warehouses (Snowflake, BigQuery, Redshift)**: schema, table, view, column
- **Databases (PostgreSQL, MySQL, ArangoDB)**: schema, table, view, column
- **Transformation (dbt)**: model, source, column
- **Orchestration (Airflow)**: workflow, job
- **BI Tools (Tableau, Looker, Power BI)**: dashboard, report, metric, dimension, measure
- **Streaming (Kafka)**: topic, schema
- **Storage (S3)**: bucket

## Usage

```python
from lineage_generator import LineageGraphGenerator

# Create a generator
generator = LineageGraphGenerator(
    min_nodes=100000,
    edge_multiplier=5,
    num_teams=10,
    output_dir="./output"
)

# Generate the graph
graph = generator.generate_graph()

# Get statistics
stats = generator.get_graph_stats()
print(stats)

# Save in different formats
generator.save_graph(format="json")
generator.save_graph(format="gexf")
```

You can also use the provided script:

```bash
python generate_lineage_graph.py
```

## Graph Structure

The generated graph includes:

- **Nodes**: Represent various data assets (tables, views, models, reports, etc.)
- **Edges**: Represent relationships between assets (source_to_target, parent_child, etc.)
- **Properties**: Each node and edge has properties like id, name, type, team, etc.

## Customization

You can customize the graph generation by modifying:

- Number of nodes and edges
- Number of teams
- Valid relationships between asset types
- Data source restrictions

## Output Files

Output files are saved in the specified output directory with timestamp-based filenames:

- `lineage_graph_YYYYMMDD_HHMMSS.json`
- `lineage_graph_YYYYMMDD_HHMMSS.gexf`
- `lineage_graph_YYYYMMDD_HHMMSS.graphml`
