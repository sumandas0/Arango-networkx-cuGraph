import networkx as nx
import random
import string
import json
import os
from datetime import datetime
import uuid
from collections import defaultdict

class LineageGraphGenerator:
    def __init__(self, 
                 min_nodes=100000, 
                 edge_multiplier=5, 
                 num_teams=10, 
                 output_dir="output",
                 orphaned_node_percent=0.1,
                 disconnected_subgraphs=3):
        self.min_nodes = min_nodes
        self.edge_multiplier = edge_multiplier
        self.num_teams = num_teams
        self.output_dir = output_dir
        self.orphaned_node_percent = orphaned_node_percent  # Percentage of nodes to be orphaned
        self.disconnected_subgraphs = disconnected_subgraphs  # Number of disconnected subgraphs to create
        self.G = nx.DiGraph()
        self.teams = []
        self.data_sources = []
        self.asset_types = []
        self.relationship_types = []
        self._setup_metadata()
        self._setup_valid_relationships()
        self._setup_data_source_restrictions()
        
        # Ensure output directory exists
        os.makedirs(self.output_dir, exist_ok=True)
    
    def _setup_metadata(self):
        """Define teams, data sources, asset types, and relationship types"""
        self.teams = [
            {"id": f"team_{i}", "name": f"Team {i}", "department": random.choice(["Engineering", "Data Science", "Analytics", "Finance", "Marketing", "Sales"])}
            for i in range(1, self.num_teams + 1)
        ]
        
        self.data_sources = [
            {"id": "snowflake", "name": "Snowflake", "type": "warehouse"},
            {"id": "bigquery", "name": "BigQuery", "type": "warehouse"},
            {"id": "redshift", "name": "Redshift", "type": "warehouse"},
            {"id": "postgres", "name": "PostgreSQL", "type": "database"},
            {"id": "mysql", "name": "MySQL", "type": "database"},
            {"id": "arangodb", "name": "ArangoDB", "type": "database"},
            {"id": "dbt", "name": "dbt", "type": "transformation"},
            {"id": "airflow", "name": "Airflow", "type": "orchestration"},
            {"id": "tableau", "name": "Tableau", "type": "bi"},
            {"id": "looker", "name": "Looker", "type": "bi"},
            {"id": "powerbi", "name": "Power BI", "type": "bi"},
            {"id": "kafka", "name": "Kafka", "type": "streaming"},
            {"id": "s3", "name": "S3", "type": "storage"}
        ]
        
        self.asset_types = [
            {"id": "table", "name": "Table"},
            {"id": "view", "name": "View"},
            {"id": "model", "name": "dbt Model"},
            {"id": "source", "name": "dbt Source"},
            {"id": "dashboard", "name": "Dashboard"},
            {"id": "report", "name": "Report"},
            {"id": "metric", "name": "Metric"},
            {"id": "column", "name": "Column"},
            {"id": "dimension", "name": "Dimension"},
            {"id": "measure", "name": "Measure"},
            {"id": "workflow", "name": "Workflow"},
            {"id": "job", "name": "Job"},
            {"id": "topic", "name": "Kafka Topic"},
            {"id": "bucket", "name": "S3 Bucket"}
        ]
        
        self.relationship_types = [
            {"id": "source_to_target", "name": "Source to Target"},
            {"id": "parent_child", "name": "Parent-Child"},
            {"id": "depends_on", "name": "Depends On"},
            {"id": "produces", "name": "Produces"},
            {"id": "consumes", "name": "Consumes"},
            {"id": "joins", "name": "Joins"},
            {"id": "includes", "name": "Includes"},
            {"id": "references", "name": "References"},
            {"id": "runs", "name": "Runs"},
            {"id": "populates", "name": "Populates"},
            {"id": "aggregates", "name": "Aggregates"},
            {"id": "filters", "name": "Filters"},
            {"id": "transforms", "name": "Transforms"},
            {"id": "enriches", "name": "Enriches"}
        ]
    
    def _setup_data_source_restrictions(self):
        """Define which asset types are valid for each data source"""
        self.data_source_asset_types = {
            # Data warehouses
            "snowflake": ["schema", "table", "view", "column"],
            "bigquery": ["schema", "table", "view", "column"],
            "redshift": ["schema", "table", "view", "column"],
            
            # Databases
            "postgres": ["schema", "table", "view", "column"],
            "mysql": ["schema", "table", "view", "column"],
            "arangodb": ["schema", "table", "view", "column"],
            
            # Transformation
            "dbt": ["model", "source", "column"],
            
            # Orchestration
            "airflow": ["workflow", "job"],
            
            # BI Tools
            "tableau": ["dashboard", "report", "metric", "dimension", "measure"],
            "looker": ["dashboard", "report", "metric", "dimension", "measure"],
            "powerbi": ["dashboard", "report", "metric", "dimension", "measure"],
            
            # Streaming
            "kafka": ["topic", "schema"],
            
            # Storage
            "s3": ["bucket"]
        }
        
        # Reverse mapping: asset type to valid data sources
        self.asset_type_data_sources = defaultdict(list)
        for ds, asset_types in self.data_source_asset_types.items():
            for asset_type in asset_types:
                self.asset_type_data_sources[asset_type].append(ds)
    
    def is_valid_asset_for_data_source(self, asset_type, data_source):
        """Check if the asset type is valid for the given data source"""
        return asset_type in self.data_source_asset_types.get(data_source, [])
    
    def get_valid_asset_types_for_data_source(self, data_source):
        """Get valid asset types for the given data source"""
        return self.data_source_asset_types.get(data_source, [])
    
    def get_valid_data_sources_for_asset_type(self, asset_type):
        """Get valid data sources for the given asset type"""
        return self.asset_type_data_sources.get(asset_type, [])
    
    def _setup_valid_relationships(self):
        """Define valid relationships between different asset types"""
        self._valid_relationships = {
            # Format: (source_type, target_type): [allowed_relationship_types]
            
            # Parent-child relationships
            ("table", "column"): ["parent_child"],
            ("view", "column"): ["parent_child"],
            ("model", "column"): ["parent_child"],
            ("dashboard", "report"): ["parent_child"],
            ("report", "metric"): ["parent_child"],
            ("workflow", "job"): ["parent_child"],
            ("source", "column"): ["parent_child"],
            
            # Data flow relationships
            ("table", "table"): ["source_to_target", "depends_on", "references", "joins"],
            ("table", "view"): ["source_to_target", "references"],
            ("table", "model"): ["source_to_target", "references"],
            ("view", "view"): ["source_to_target", "depends_on", "references"],
            ("view", "model"): ["source_to_target", "references"],
            ("model", "model"): ["source_to_target", "depends_on", "references", "joins"],
            ("model", "table"): ["produces", "populates"],
            ("table", "report"): ["source_to_target"],
            ("view", "report"): ["source_to_target"],
            ("model", "report"): ["source_to_target"],
            
            # Column-level lineage
            ("column", "column"): ["source_to_target", "references", "transforms", "aggregates", "filters", "enriches"],
            
            # Job relationships
            ("job", "table"): ["produces", "consumes", "transforms", "populates"],
            ("job", "view"): ["produces", "transforms"],
            ("job", "model"): ["produces", "transforms", "runs"],
            ("job", "job"): ["depends_on", "runs"],
            ("job", "topic"): ["produces", "consumes"],
            ("job", "bucket"): ["produces", "consumes"],
            
            # Streaming & storage
            ("topic", "table"): ["populates", "source_to_target"],
            ("topic", "model"): ["source_to_target"],
            ("topic", "job"): ["consumes"],
            ("bucket", "table"): ["populates", "source_to_target"],
            ("bucket", "job"): ["consumes"],
            ("table", "topic"): ["produces"],
            ("table", "bucket"): ["produces"],
            
            # BI relationships
            ("table", "dashboard"): ["source_to_target"],
            ("view", "dashboard"): ["source_to_target"],
            ("model", "dashboard"): ["source_to_target"],
            ("column", "metric"): ["source_to_target", "references", "aggregates"],
            
            # Other common relationships
            ("source", "model"): ["source_to_target"],
            ("table", "source"): ["source_to_target"]
        }
    
    def is_valid_relationship(self, source_type, target_type, relationship_type):
        """Check if the relationship between source and target types is valid"""
        # Check direct relationship
        if (source_type, target_type) in self._valid_relationships:
            return relationship_type in self._valid_relationships[(source_type, target_type)]
        
        # Check for any wildcard relationships
        if (source_type, "*") in self._valid_relationships:
            return relationship_type in self._valid_relationships[(source_type, "*")]
        
        if ("*", target_type) in self._valid_relationships:
            return relationship_type in self._valid_relationships[("*", target_type)]
        
        # Default: relationship not defined, so not valid
        return False
    
    def get_valid_relationships(self, source_type, target_type):
        """Get valid relationship types between source and target asset types"""
        if (source_type, target_type) in self._valid_relationships:
            return self._valid_relationships[(source_type, target_type)]
        return []
    
    def add_edge_with_validation(self, source_id, target_id, relationship=None):
        """Add an edge with validation of relationship types"""
        # Get node types
        if source_id not in self.G.nodes() or target_id not in self.G.nodes():
            return False
            
        source_type = self.G.nodes[source_id].get("type", "unknown")
        target_type = self.G.nodes[target_id].get("type", "unknown")
        
        # If relationship type not specified, pick a valid one
        if not relationship:
            valid_rels = self.get_valid_relationships(source_type, target_type)
            if not valid_rels:
                return False
            relationship = random.choice(valid_rels)
        
        # Validate relationship
        if self.is_valid_relationship(source_type, target_type, relationship):
            self.G.add_edge(source_id, target_id, relationship=relationship)
            return True
        
        return False
    
    def _generate_random_name(self, prefix, length=8):
        """Generate a random name with the given prefix"""
        suffix = ''.join(random.choices(string.ascii_lowercase + string.digits, k=length))
        return f"{prefix}_{suffix}"
    
    def _generate_random_field_name(self):
        """Generate a random field name"""
        prefixes = ["id", "name", "value", "count", "date", "timestamp", "amount", "price", "quantity", "status"]
        suffixes = ["", "_id", "_name", "_value", "_count", "_date", "_ts", "_amt", "_price", "_qty", "_status"]
        
        prefix = random.choice(prefixes)
        suffix = random.choice(suffixes)
        
        if suffix == "":
            return prefix
        else:
            return f"{prefix}{suffix}"
    
    def _get_random_data_type(self):
        """Get a random data type for a field"""
        data_types = [
            "INTEGER", "BIGINT", "FLOAT", "DOUBLE", "DECIMAL(10,2)", 
            "VARCHAR(255)", "STRING", "TEXT", "CHAR(10)", 
            "TIMESTAMP", "DATE", "DATETIME", "BOOLEAN"
        ]
        return random.choice(data_types)
    
    def generate_schema(self):
        """Generate schema structure for each team and data source"""
        schemas = {}
        
        for team in self.teams:
            team_id = team["id"]
            team_schemas = {}
            
            # Each team uses 3-5 data sources
            team_data_sources = random.sample(self.data_sources, random.randint(3, min(5, len(self.data_sources))))
            
            for data_source in team_data_sources:
                ds_id = data_source["id"]
                
                # Skip non-database data sources for schema generation
                if data_source["type"] not in ["warehouse", "database"]:
                    continue
                
                # Generate 2-5 databases per team per data source
                db_count = random.randint(2, 5)
                databases = []
                
                for i in range(db_count):
                    db_name = self._generate_random_name(f"{team_id}_db", 6)
                    
                    # Generate 3-10 schemas per database
                    schema_count = random.randint(3, 10)
                    db_schemas = []
                    
                    for j in range(schema_count):
                        schema_name = self._generate_random_name(f"{db_name}_schema", 6)
                        db_schemas.append(schema_name)
                    
                    databases.append({"name": db_name, "schemas": db_schemas})
                
                team_schemas[ds_id] = databases
            
            schemas[team_id] = team_schemas
        
        return schemas
    
    def _assign_scores(self):
        """
        Assign usage scores to nodes with the following distribution:
        - 20% have heavy usage (scores between 70-100)
        - ~10% have moderate usage (scores between 40-69)
        - ~20% have light usage (scores between 1-39)
        - ~50% have no usage (score = -1)
        """
        nodes = list(self.G.nodes())
        # Shuffle to ensure randomness
        random.shuffle(nodes)
        
        total_nodes = len(nodes)
        heavy_usage_count = int(total_nodes * 0.2)  # 20% heavy usage
        moderate_usage_count = int(total_nodes * 0.1)  # ~10% moderate usage
        light_usage_count = int(total_nodes * 0.2)  # ~20% light usage
        # The rest (~50%) will have no usage
        
        # Assign scores
        for i, node in enumerate(nodes):
            if i < heavy_usage_count:
                # Heavy usage (70-100)
                score = random.randint(70, 100)
            elif i < (heavy_usage_count + moderate_usage_count):
                # Moderate usage (40-69)
                score = random.randint(40, 69)
            elif i < (heavy_usage_count + moderate_usage_count + light_usage_count):
                # Light usage (1-39)
                score = random.randint(1, 39)
            else:
                # No usage (-1)
                score = -1
                
            # Add the score to the node attributes
            self.G.nodes[node]['score'] = score
    
    def generate_graph(self):
        """Generate the full lineage graph"""
        print(f"Generating lineage graph with minimum {self.min_nodes} nodes and approximately {self.min_nodes * self.edge_multiplier} edges...")
        
        # Generate schemas for database-type data sources
        schemas = self.generate_schema()
        
        # Track node counts per team for balanced distribution
        team_node_counts = {team["id"]: 0 for team in self.teams}
        nodes_per_team = self.min_nodes // len(self.teams)
        
        # Generate nodes for each team
        for team in self.teams:
            team_id = team["id"]
            print(f"Generating nodes for {team['name']}...")
            
            # Each team uses 3-5 data sources
            team_data_sources = random.sample(self.data_sources, random.randint(3, min(5, len(self.data_sources))))
            
            for data_source in team_data_sources:
                ds_id = data_source["id"]
                ds_type = data_source["type"]
                
                if ds_type in ["warehouse", "database"]:
                    self._generate_database_assets(team, data_source, schemas[team_id].get(ds_id, []), team_node_counts, nodes_per_team)
                elif ds_type == "transformation":
                    self._generate_dbt_assets(team, data_source, team_node_counts, nodes_per_team)
                elif ds_type == "bi":
                    self._generate_bi_assets(team, data_source, team_node_counts, nodes_per_team)
                elif ds_type == "orchestration":
                    self._generate_orchestration_assets(team, data_source, team_node_counts, nodes_per_team)
                elif ds_type == "streaming":
                    self._generate_streaming_assets(team, data_source, team_node_counts, nodes_per_team)
                elif ds_type == "storage":
                    self._generate_storage_assets(team, data_source, team_node_counts, nodes_per_team)
        
        # Generate cross-team lineage edges
        self._generate_cross_team_lineage()
        
        # Ensure we have enough nodes
        total_nodes = len(self.G.nodes())
        if total_nodes < self.min_nodes:
            print(f"Only generated {total_nodes} nodes. Adding {self.min_nodes - total_nodes} more nodes...")
            self._add_additional_nodes(self.min_nodes - total_nodes)
        
        # Ensure we have enough edges
        target_edges = self.min_nodes * self.edge_multiplier
        total_edges = len(self.G.edges())
        if total_edges < target_edges:
            print(f"Only generated {total_edges} edges. Adding {target_edges - total_edges} more edges...")
            self._add_additional_edges(target_edges - total_edges)
        
        # Create orphaned nodes and disconnected subgraphs
        self._create_orphaned_nodes()
        self._create_disconnected_subgraphs()
        
        # Assign popularity scores
        self._assign_scores()
        
        # Print final counts
        final_nodes = len(self.G.nodes())
        final_edges = len(self.G.edges())
        print(f"Final graph has {final_nodes} nodes and {final_edges} edges")
        
        # Print connectivity stats
        connectivity_stats = self._analyze_connectivity()
        print(f"Graph connectivity: {connectivity_stats['connected_components']} connected components, {connectivity_stats['orphaned_nodes']} orphaned nodes")
        
        return self.G
    
    def _generate_database_assets(self, team, data_source, db_schemas, team_node_counts, nodes_per_team):
        """Generate database assets (tables, views, columns)"""
        team_id = team["id"]
        ds_id = data_source["id"]
        
        if not db_schemas or team_node_counts[team_id] >= nodes_per_team:
            return
        
        # Ensure we're only creating valid asset types for this data source
        valid_asset_types = self.get_valid_asset_types_for_data_source(ds_id)
        
        # Check if we can create tables and views for this data source
        can_create_tables = "table" in valid_asset_types
        can_create_views = "view" in valid_asset_types
        can_create_columns = "column" in valid_asset_types
        can_create_schemas = "schema" in valid_asset_types
        
        if not (can_create_tables or can_create_views):
            return
        
        for database in db_schemas:
            db_name = database["name"]
            
            for schema_name in database["schemas"]:
                # Add schema node if allowed
                if can_create_schemas:
                    schema_id = f"{ds_id}.{db_name}.{schema_name}"
                    if schema_id not in self.G:
                        self.G.add_node(schema_id,
                                       id=schema_id,
                                       name=schema_name,
                                       full_name=f"{db_name}.{schema_name}",
                                       type="schema",
                                       data_source=ds_id,
                                       database=db_name,
                                       team=team_id,
                                       created_at=datetime.now().isoformat())
                        team_node_counts[team_id] += 1
                
                # Generate tables if allowed
                if can_create_tables:
                    # Generate 5-20 tables per schema
                    table_count = random.randint(5, 20)
                    
                    for i in range(table_count):
                        if team_node_counts[team_id] >= nodes_per_team:
                            return
                        
                        table_name = self._generate_random_name("tbl")
                        table_id = f"{ds_id}.{db_name}.{schema_name}.{table_name}"
                        
                        # Add table node
                        self.G.add_node(table_id, 
                                        id=table_id,
                                        name=table_name,
                                        full_name=f"{db_name}.{schema_name}.{table_name}",
                                        type="table",
                                        data_source=ds_id,
                                        database=db_name,
                                        schema=schema_name,
                                        team=team_id,
                                        created_at=datetime.now().isoformat())
                        team_node_counts[team_id] += 1
                        
                        # Connect table to schema if schema nodes exist
                        if can_create_schemas:
                            schema_id = f"{ds_id}.{db_name}.{schema_name}"
                            if schema_id in self.G:
                                self.G.add_edge(schema_id, table_id, relationship="parent_child")
                        
                        # Generate columns if allowed
                        if can_create_columns:
                            # Generate 5-50 columns per table
                            column_count = random.randint(5, 50)
                            
                            for j in range(column_count):
                                if team_node_counts[team_id] >= nodes_per_team:
                                    return
                                
                                column_name = self._generate_random_field_name()
                                column_id = f"{table_id}.{column_name}"
                                
                                # Add column node
                                self.G.add_node(column_id,
                                                id=column_id,
                                                name=column_name,
                                                full_name=f"{db_name}.{schema_name}.{table_name}.{column_name}",
                                                type="column",
                                                data_type=self._get_random_data_type(),
                                                data_source=ds_id,
                                                database=db_name,
                                                schema=schema_name,
                                                table=table_name,
                                                team=team_id,
                                                created_at=datetime.now().isoformat())
                                team_node_counts[team_id] += 1
                                
                                # Connect column to table
                                self.add_edge_with_validation(table_id, column_id, "parent_child")
                
                # Generate views if allowed
                if can_create_views and can_create_tables:
                    # Generate some views that depend on the tables
                    view_count = random.randint(2, 10)
                    tables_in_schema = [n for n in self.G.nodes() 
                                      if self.G.nodes[n].get("type") == "table" and 
                                         self.G.nodes[n].get("schema") == schema_name and
                                         self.G.nodes[n].get("database") == db_name]
                    
                    if tables_in_schema:
                        for i in range(view_count):
                            if team_node_counts[team_id] >= nodes_per_team:
                                return
                            
                            view_name = self._generate_random_name("view")
                            view_id = f"{ds_id}.{db_name}.{schema_name}.{view_name}"
                            
                            # Add view node
                            self.G.add_node(view_id,
                                            id=view_id,
                                            name=view_name,
                                            full_name=f"{db_name}.{schema_name}.{view_name}",
                                            type="view",
                                            data_source=ds_id,
                                            database=db_name,
                                            schema=schema_name,
                                            team=team_id,
                                            created_at=datetime.now().isoformat())
                            team_node_counts[team_id] += 1
                            
                            # Connect view to schema if schema nodes exist
                            if can_create_schemas:
                                schema_id = f"{ds_id}.{db_name}.{schema_name}"
                                if schema_id in self.G:
                                    self.G.add_edge(schema_id, view_id, relationship="parent_child")
                            
                            # Connect view to source tables (1-5 source tables per view)
                            source_count = min(random.randint(1, 5), len(tables_in_schema))
                            source_tables = random.sample(tables_in_schema, source_count)
                            
                            for table_id in source_tables:
                                self.add_edge_with_validation(table_id, view_id, "source_to_target")
                                
                                # Also create field-level lineage for some columns
                                if can_create_columns:
                                    table_columns = [n for n in self.G.neighbors(table_id)]
                                    
                                    # Generate columns for the view
                                    view_column_count = random.randint(3, 15)
                                    for j in range(view_column_count):
                                        if team_node_counts[team_id] >= nodes_per_team:
                                            return
                                        
                                        view_col_name = self._generate_random_field_name()
                                        view_col_id = f"{view_id}.{view_col_name}"
                                        
                                        # Add view column node
                                        self.G.add_node(view_col_id,
                                                        id=view_col_id,
                                                        name=view_col_name,
                                                        full_name=f"{db_name}.{schema_name}.{view_name}.{view_col_name}",
                                                        type="column",
                                                        data_type=self._get_random_data_type(),
                                                        data_source=ds_id,
                                                        database=db_name,
                                                        schema=schema_name,
                                                        view=view_name,
                                                        team=team_id,
                                                        created_at=datetime.now().isoformat())
                                        team_node_counts[team_id] += 1
                                        
                                        # Connect view column to view
                                        self.add_edge_with_validation(view_id, view_col_id, "parent_child")
                                        
                                        # Connect to source columns
                                        if table_columns:
                                            source_columns = random.sample(table_columns, min(3, len(table_columns)))
                                            for source_col_id in source_columns:
                                                valid_rels = self.get_valid_relationships("column", "column")
                                                if valid_rels:
                                                    rel_type = random.choice(valid_rels)
                                                    self.add_edge_with_validation(source_col_id, view_col_id, rel_type)

    def _generate_dbt_assets(self, team, data_source, team_node_counts, nodes_per_team):
        """Generate dbt assets (sources, models)"""
        team_id = team["id"]
        ds_id = data_source["id"]
        
        if team_node_counts[team_id] >= nodes_per_team:
            return
        
        # Check if we can create models and sources for this data source
        valid_asset_types = self.get_valid_asset_types_for_data_source(ds_id)
        
        can_create_models = "model" in valid_asset_types
        can_create_sources = "source" in valid_asset_types
        can_create_columns = "column" in valid_asset_types
        
        if not (can_create_models or can_create_sources):
            return
            
        # Generate 2-5 dbt projects per team
        project_count = random.randint(2, 5)
        
        for p in range(project_count):
            project_name = f"{team_id}_dbt_project_{p+1}"
            
            # Find potential source database tables
            database_tables = [n for n in self.G.nodes() 
                              if self.G.nodes[n].get("type") == "table" and 
                                 self.G.nodes[n].get("team") == team_id]
            
            sources = []
            # Create sources for some database tables if allowed
            if can_create_sources and database_tables:
                source_tables = random.sample(database_tables, min(random.randint(5, 20), len(database_tables)))
                
                for table_id in source_tables:
                    if team_node_counts[team_id] >= nodes_per_team:
                        return
                    
                    table_info = self.G.nodes[table_id]
                    source_name = f"src_{table_info['name']}"
                    source_id = f"{ds_id}.{project_name}.{source_name}"
                    
                    # Add source node
                    self.G.add_node(source_id,
                                    id=source_id,
                                    name=source_name,
                                    full_name=f"{project_name}.{source_name}",
                                    type="source",
                                    data_source=ds_id,
                                    project=project_name,
                                    team=team_id,
                                    created_at=datetime.now().isoformat())
                    team_node_counts[team_id] += 1
                    
                    # Connect source to database table
                    self.G.add_edge(table_id, source_id, relationship="source_to_target")
                    sources.append(source_id)
                    
                    # Create field-level lineage if allowed
                    if can_create_columns:
                        table_columns = [n for n in self.G.neighbors(table_id)]
                        
                        for col_id in table_columns:
                            if team_node_counts[team_id] >= nodes_per_team:
                                return
                            
                            col_info = self.G.nodes[col_id]
                            source_col_name = col_info['name']
                            source_col_id = f"{source_id}.{source_col_name}"
                            
                            # Add source column node
                            self.G.add_node(source_col_id,
                                            id=source_col_id,
                                            name=source_col_name,
                                            full_name=f"{project_name}.{source_name}.{source_col_name}",
                                            type="column",
                                            data_type=col_info.get("data_type", "UNKNOWN"),
                                            data_source=ds_id,
                                            project=project_name,
                                            source=source_name,
                                            team=team_id,
                                            created_at=datetime.now().isoformat())
                            team_node_counts[team_id] += 1
                            
                            # Connect source column to source
                            self.G.add_edge(source_id, source_col_id, relationship="parent_child")
                            
                            # Connect source column to database column
                            self.G.add_edge(col_id, source_col_id, relationship="source_to_target")
            
            # Create models if allowed
            if can_create_models:
                # Generate different types of models (staging, intermediate, marts)
                model_types = ["stg", "int", "mart"]
                
                # Create staging models based on sources
                stg_models = []
                if can_create_sources:
                    for source_id in sources:
                        if team_node_counts[team_id] >= nodes_per_team:
                            return
                        
                        source_info = self.G.nodes[source_id]
                        model_name = f"stg_{source_info['name'].replace('src_', '')}"
                        model_id = f"{ds_id}.{project_name}.{model_name}"
                        
                        # Add model node
                        self.G.add_node(model_id,
                                        id=model_id,
                                        name=model_name,
                                        full_name=f"{project_name}.{model_name}",
                                        type="model",
                                        model_type="staging",
                                        data_source=ds_id,
                                        project=project_name,
                                        team=team_id,
                                        created_at=datetime.now().isoformat())
                        team_node_counts[team_id] += 1
                        
                        # Connect model to source
                        self.G.add_edge(source_id, model_id, relationship="source_to_target")
                        stg_models.append(model_id)
                        
                        # Create field-level lineage if allowed
                        if can_create_columns:
                            source_columns = [n for n in self.G.neighbors(source_id) if self.G.nodes[n].get("type") == "column"]
                            
                            for source_col_id in source_columns:
                                if team_node_counts[team_id] >= nodes_per_team:
                                    return
                                
                                source_col_info = self.G.nodes[source_col_id]
                                model_col_name = source_col_info['name']
                                model_col_id = f"{model_id}.{model_col_name}"
                                
                                # Add model column node
                                self.G.add_node(model_col_id,
                                                id=model_col_id,
                                                name=model_col_name,
                                                full_name=f"{project_name}.{model_name}.{model_col_name}",
                                                type="column",
                                                data_type=source_col_info.get("data_type", "UNKNOWN"),
                                                data_source=ds_id,
                                                project=project_name,
                                                model=model_name,
                                                team=team_id,
                                                created_at=datetime.now().isoformat())
                                team_node_counts[team_id] += 1
                                
                                # Connect model column to model
                                self.G.add_edge(model_id, model_col_id, relationship="parent_child")
                                
                                # Connect model column to source column
                                self.G.add_edge(source_col_id, model_col_id, relationship="source_to_target")
                
                # If no staging models from sources, create some directly linked to tables
                if not stg_models and database_tables:
                    for table_id in random.sample(database_tables, min(5, len(database_tables))):
                        if team_node_counts[team_id] >= nodes_per_team:
                            return
                        
                        table_info = self.G.nodes[table_id]
                        model_name = f"stg_{table_info['name']}"
                        model_id = f"{ds_id}.{project_name}.{model_name}"
                        
                        # Add model node
                        self.G.add_node(model_id,
                                        id=model_id,
                                        name=model_name,
                                        full_name=f"{project_name}.{model_name}",
                                        type="model",
                                        model_type="staging",
                                        data_source=ds_id,
                                        project=project_name,
                                        team=team_id,
                                        created_at=datetime.now().isoformat())
                        team_node_counts[team_id] += 1
                        
                        # Connect model to table
                        self.G.add_edge(table_id, model_id, relationship="source_to_target")
                        stg_models.append(model_id)
                
                # Rest of the model generation remains the same
                # ...existing code for int_models and mart_models...
                
                # Create intermediate models
                int_models = []
                int_model_count = random.randint(5, 20)
                
                # Group staging models for intermediate models
                stg_model_groups = []
                remaining_stg = stg_models.copy()
                
                while remaining_stg:
                    group_size = min(random.randint(1, 3), len(remaining_stg))
                    group = random.sample(remaining_stg, group_size)
                    for m in group:
                        remaining_stg.remove(m)
                    stg_model_groups.append(group)
                
                for i, stg_group in enumerate(stg_model_groups):
                    if team_node_counts[team_id] >= nodes_per_team or i >= int_model_count:
                        break
                    
                    model_name = f"int_model_{i+1}"
                    model_id = f"{ds_id}.{project_name}.{model_name}"
                    
                    # Add model node
                    self.G.add_node(model_id,
                                    id=model_id,
                                    name=model_name,
                                    full_name=f"{project_name}.{model_name}",
                                    type="model",
                                    model_type="intermediate",
                                    data_source=ds_id,
                                    project=project_name,
                                    team=team_id,
                                    created_at=datetime.now().isoformat())
                    team_node_counts[team_id] += 1
                    
                    # Connect intermediate model to staging models
                    for stg_id in stg_group:
                        self.G.add_edge(stg_id, model_id, relationship="source_to_target")
                    
                    int_models.append(model_id)
                    
                    # Create columns for intermediate model if allowed
                    if can_create_columns:
                        col_count = random.randint(5, 15)
                        for j in range(col_count):
                            if team_node_counts[team_id] >= nodes_per_team:
                                break
                            
                            model_col_name = self._generate_random_field_name()
                            model_col_id = f"{model_id}.{model_col_name}"
                            
                            # Add model column node
                            self.G.add_node(model_col_id,
                                            id=model_col_id,
                                            name=model_col_name,
                                            full_name=f"{project_name}.{model_name}.{model_col_name}",
                                            type="column",
                                            data_type=self._get_random_data_type(),
                                            data_source=ds_id,
                                            project=project_name,
                                            model=model_name,
                                            team=team_id,
                                            created_at=datetime.now().isoformat())
                            team_node_counts[team_id] += 1
                            
                            # Connect column to model
                            self.G.add_edge(model_id, model_col_id, relationship="parent_child")
                            
                            # Connect to source columns from staging models
                            for stg_id in stg_group:
                                stg_columns = [n for n in self.G.neighbors(stg_id) if self.G.nodes[n].get("type") == "column"]
                                if stg_columns:
                                    source_cols = random.sample(stg_columns, min(2, len(stg_columns)))
                                    for src_col in source_cols:
                                        rel_type = random.choice(["source_to_target", "transforms", "references"])
                                        self.G.add_edge(src_col, model_col_id, relationship=rel_type)
                
                # Create mart models
                mart_model_count = random.randint(3, 10)
                mart_models = []
                
                # Group intermediate models for mart models
                int_model_groups = []
                remaining_int = int_models.copy()
                
                while remaining_int:
                    group_size = min(random.randint(1, 4), len(remaining_int))
                    group = random.sample(remaining_int, group_size)
                    for m in group:
                        remaining_int.remove(m)
                    int_model_groups.append(group)
                
                for i, int_group in enumerate(int_model_groups):
                    if team_node_counts[team_id] >= nodes_per_team or i >= mart_model_count:
                        break
                    
                    model_name = f"mart_model_{i+1}"
                    model_id = f"{ds_id}.{project_name}.{model_name}"
                    
                    # Add model node
                    self.G.add_node(model_id,
                                    id=model_id,
                                    name=model_name,
                                    full_name=f"{project_name}.{model_name}",
                                    type="model",
                                    model_type="mart",
                                    data_source=ds_id,
                                    project=project_name,
                                    team=team_id,
                                    created_at=datetime.now().isoformat())
                    team_node_counts[team_id] += 1
                    
                    # Connect mart model to intermediate models
                    for int_id in int_group:
                        self.G.add_edge(int_id, model_id, relationship="source_to_target")
                    
                    mart_models.append(model_id)
                    
                    # Create columns for mart model if allowed
                    if can_create_columns:
                        col_count = random.randint(5, 20)
                        for j in range(col_count):
                            if team_node_counts[team_id] >= nodes_per_team:
                                break
                            
                            model_col_name = self._generate_random_field_name()
                            model_col_id = f"{model_id}.{model_col_name}"
                            
                            # Add model column node
                            self.G.add_node(model_col_id,
                                            id=model_col_id,
                                            name=model_col_name,
                                            full_name=f"{project_name}.{model_name}.{model_col_name}",
                                            type="column",
                                            data_type=self._get_random_data_type(),
                                            data_source=ds_id,
                                            project=project_name,
                                            model=model_name,
                                            team=team_id,
                                            created_at=datetime.now().isoformat())
                            team_node_counts[team_id] += 1
                            
                            # Connect column to model
                            self.G.add_edge(model_id, model_col_id, relationship="parent_child")
                            
                            # Connect to columns from intermediate models
                            for int_id in int_group:
                                int_columns = [n for n in self.G.neighbors(int_id) if self.G.nodes[n].get("type") == "column"]
                                if int_columns:
                                    source_cols = random.sample(int_columns, min(2, len(int_columns)))
                                    for src_col in source_cols:
                                        rel_type = random.choice(["source_to_target", "transforms", "references", "aggregates"])
                                        self.G.add_edge(src_col, model_col_id, relationship=rel_type)

    def _generate_bi_assets(self, team, data_source, team_node_counts, nodes_per_team):
        """Generate BI assets (dashboards, reports, metrics)"""
        team_id = team["id"]
        ds_id = data_source["id"]
        
        if team_node_counts[team_id] >= nodes_per_team:
            return
        
        # Check if we can create BI assets for this data source
        valid_asset_types = self.get_valid_asset_types_for_data_source(ds_id)
        
        can_create_dashboards = "dashboard" in valid_asset_types
        can_create_reports = "report" in valid_asset_types
        can_create_metrics = "metric" in valid_asset_types
        can_create_dimensions = "dimension" in valid_asset_types
        can_create_measures = "measure" in valid_asset_types
        
        if not (can_create_dashboards or can_create_reports or can_create_metrics):
            return
        
        # Find potential source tables, views, and models to connect to BI assets
        potential_sources = [n for n in self.G.nodes() 
                            if self.G.nodes[n].get("type") in ["table", "view", "model"] and 
                               self.G.nodes[n].get("team") == team_id]
        
        if not potential_sources:
            return
        
        # Generate 2-10 dashboards if allowed
        if can_create_dashboards:
            dashboard_count = random.randint(2, 10)
            dashboards = []
            
            for i in range(dashboard_count):
                if team_node_counts[team_id] >= nodes_per_team:
                    return
                
                dashboard_name = self._generate_random_name("dashboard")
                dashboard_id = f"{ds_id}.{dashboard_name}"
                
                # Add dashboard node
                self.G.add_node(dashboard_id,
                                id=dashboard_id,
                                name=dashboard_name,
                                full_name=f"{ds_id} - {dashboard_name}",
                                type="dashboard",
                                data_source=ds_id,
                                team=team_id,
                                created_at=datetime.now().isoformat())
                team_node_counts[team_id] += 1
                dashboards.append(dashboard_id)
                
                # Generate reports if allowed
                if can_create_reports:
                    # Generate 2-15 reports per dashboard
                    report_count = random.randint(2, 15)
                    
                    for j in range(report_count):
                        if team_node_counts[team_id] >= nodes_per_team:
                            return
                        
                        report_name = self._generate_random_name("report")
                        report_id = f"{dashboard_id}.{report_name}"
                        
                        # Add report node
                        self.G.add_node(report_id,
                                        id=report_id,
                                        name=report_name,
                                        full_name=f"{ds_id} - {dashboard_name} - {report_name}",
                                        type="report",
                                        data_source=ds_id,
                                        dashboard=dashboard_name,
                                        team=team_id,
                                        created_at=datetime.now().isoformat())
                        team_node_counts[team_id] += 1
                        
                        # Connect report to dashboard
                        self.add_edge_with_validation(dashboard_id, report_id, "parent_child")
                        
                        # Connect report to source tables/views/models (1-3 sources per report)
                        source_count = min(random.randint(1, 3), len(potential_sources))
                        sources = random.sample(potential_sources, source_count)
                        
                        for source_id in sources:
                            self.add_edge_with_validation(source_id, report_id, "source_to_target")
                        
                        # Generate metrics if allowed
                        if can_create_metrics:
                            # Generate 2-10 metrics per report
                            metric_count = random.randint(2, 10)
                            
                            for k in range(metric_count):
                                if team_node_counts[team_id] >= nodes_per_team:
                                    return
                                
                                metric_name = self._generate_random_name("metric")
                                metric_id = f"{report_id}.{metric_name}"
                                
                                # Add metric node
                                self.G.add_node(metric_id,
                                                id=metric_id,
                                                name=metric_name,
                                                full_name=f"{ds_id} - {report_name} - {metric_name}",
                                                type="metric",
                                                data_source=ds_id,
                                                report=report_name,
                                                team=team_id,
                                                created_at=datetime.now().isoformat())
                                team_node_counts[team_id] += 1
                                
                                # Connect metric to report
                                self.add_edge_with_validation(report_id, metric_id, "parent_child")
                                
                                # Connect metric to columns in source tables
                                for source_id in sources:
                                    # Get columns from source
                                    columns = [n for n in self.G.neighbors(source_id) if self.G.nodes[n].get("type") == "column"]
                                    
                                    if columns:
                                        # Pick 1-3 columns to connect to this metric
                                        col_count = min(random.randint(1, 3), len(columns))
                                        source_columns = random.sample(columns, col_count)
                                        
                                        for col_id in source_columns:
                                            valid_rels = self.get_valid_relationships("column", "metric")
                                            if valid_rels:
                                                rel_type = random.choice(valid_rels)
                                                self.add_edge_with_validation(col_id, metric_id, rel_type)
                
                # Generate dimensions if allowed
                if can_create_dimensions and potential_sources:
                    # Generate 3-8 dimensions per dashboard
                    dimension_count = random.randint(3, 8)
                    
                    for j in range(dimension_count):
                        if team_node_counts[team_id] >= nodes_per_team:
                            return
                        
                        dimension_name = self._generate_random_name("dim")
                        dimension_id = f"{dashboard_id}.{dimension_name}"
                        
                        # Add dimension node
                        self.G.add_node(dimension_id,
                                        id=dimension_id,
                                        name=dimension_name,
                                        full_name=f"{ds_id} - {dashboard_name} - {dimension_name}",
                                        type="dimension",
                                        data_source=ds_id,
                                        dashboard=dashboard_name,
                                        team=team_id,
                                        created_at=datetime.now().isoformat())
                        team_node_counts[team_id] += 1
                        
                        # Link dimension to a source
                        source_id = random.choice(potential_sources)
                        self.add_edge_with_validation(source_id, dimension_id, "references")
                        
                        # Link to specific columns
                        columns = [n for n in self.G.neighbors(source_id) if self.G.nodes[n].get("type") == "column"]
                        if columns:
                            col_id = random.choice(columns)
                            self.add_edge_with_validation(col_id, dimension_id, "references")
                
                # Generate measures if allowed
                if can_create_measures and potential_sources:
                    # Generate 4-12 measures per dashboard
                    measure_count = random.randint(4, 12)
                    
                    for j in range(measure_count):
                        if team_node_counts[team_id] >= nodes_per_team:
                            return
                        
                        measure_name = self._generate_random_name("measure")
                        measure_id = f"{dashboard_id}.{measure_name}"
                        
                        # Add measure node
                        self.G.add_node(measure_id,
                                        id=measure_id,
                                        name=measure_name,
                                        full_name=f"{ds_id} - {dashboard_name} - {measure_name}",
                                        type="measure",
                                        data_source=ds_id,
                                        dashboard=dashboard_name,
                                        team=team_id,
                                        created_at=datetime.now().isoformat())
                        team_node_counts[team_id] += 1
                        
                        # Link measure to a source
                        source_id = random.choice(potential_sources)
                        self.add_edge_with_validation(source_id, measure_id, "references")
                        
                        # Link to specific columns
                        columns = [n for n in self.G.neighbors(source_id) if self.G.nodes[n].get("type") == "column"]
                        if columns:
                            col_id = random.choice(columns)
                            self.add_edge_with_validation(col_id, measure_id, "aggregates")

    def _generate_orchestration_assets(self, team, data_source, team_node_counts, nodes_per_team):
        """Generate orchestration assets (workflows, jobs)"""
        team_id = team["id"]
        ds_id = data_source["id"]
        
        if team_node_counts[team_id] >= nodes_per_team:
            return
        
        # Check if we can create orchestration assets for this data source
        valid_asset_types = self.get_valid_asset_types_for_data_source(ds_id)
        
        can_create_workflows = "workflow" in valid_asset_types
        can_create_jobs = "job" in valid_asset_types
        
        if not (can_create_workflows or can_create_jobs):
            return
        
        # Only continue if we can create both workflows and jobs,
        # since jobs need to be part of workflows
        if not (can_create_workflows and can_create_jobs):
            return
        
        # Generate 2-10 workflows
        workflow_count = random.randint(2, 10)
        
        for i in range(workflow_count):
            if team_node_counts[team_id] >= nodes_per_team:
                return
            
            workflow_name = self._generate_random_name("workflow")
            workflow_id = f"{ds_id}.{workflow_name}"
            
            # Add workflow node
            self.G.add_node(workflow_id,
                            id=workflow_id,
                            name=workflow_name,
                            full_name=f"{ds_id} - {workflow_name}",
                            type="workflow",
                            data_source=ds_id,
                            team=team_id,
                            created_at=datetime.now().isoformat())
            team_node_counts[team_id] += 1
            
            # Generate 5-20 jobs per workflow
            job_count = random.randint(5, 20)
            prev_job_id = None
            
            for j in range(job_count):
                if team_node_counts[team_id] >= nodes_per_team:
                    return
                
                job_name = self._generate_random_name("job")
                job_id = f"{workflow_id}.{job_name}"
                
                # Add job node
                self.G.add_node(job_id,
                                id=job_id,
                                name=job_name,
                                full_name=f"{ds_id} - {workflow_name} - {job_name}",
                                type="job",
                                data_source=ds_id,
                                workflow=workflow_name,
                                team=team_id,
                                created_at=datetime.now().isoformat())
                team_node_counts[team_id] += 1
                
                # Connect job to workflow
                self.add_edge_with_validation(workflow_id, job_id, "parent_child")
                
                # Connect job to previous job (job dependency)
                if prev_job_id:
                    self.add_edge_with_validation(prev_job_id, job_id, "depends_on")
                
                prev_job_id = job_id
            
            # Connect jobs to other assets (tables, models, etc.)
            jobs = [n for n in self.G.neighbors(workflow_id)]
            potential_targets = [n for n in self.G.nodes() 
                               if self.G.nodes[n].get("type") in ["table", "view", "model"] and 
                                  self.G.nodes[n].get("team") == team_id]
            
            if potential_targets and jobs:
                # For each job, connect to 0-2 targets
                for job_id in jobs:
                    if random.random() < 0.8:  # 80% chance of having a target
                        target_count = min(random.randint(1, 2), len(potential_targets))
                        targets = random.sample(potential_targets, target_count)
                        
                        for target_id in targets:
                            valid_rels = self.get_valid_relationships("job", self.G.nodes[target_id].get("type", "unknown"))
                            if valid_rels:
                                rel_type = random.choice(valid_rels)
                                self.add_edge_with_validation(job_id, target_id, rel_type)

    def _generate_streaming_assets(self, team, data_source, team_node_counts, nodes_per_team):
        """Generate streaming assets (topics)"""
        team_id = team["id"]
        ds_id = data_source["id"]
        
        if team_node_counts[team_id] >= nodes_per_team:
            return
        
        # Check if we can create streaming assets for this data source
        valid_asset_types = self.get_valid_asset_types_for_data_source(ds_id)
        
        can_create_topics = "topic" in valid_asset_types
        can_create_schemas = "schema" in valid_asset_types
        
        if not can_create_topics:
            return
        
        # Generate 5-20 topics
        topic_count = random.randint(5, 20)
        
        for i in range(topic_count):
            if team_node_counts[team_id] >= nodes_per_team:
                return
            
            topic_name = self._generate_random_name("topic")
            topic_id = f"{ds_id}.{topic_name}"
            
            # Add topic node
            self.G.add_node(topic_id,
                            id=topic_id,
                            name=topic_name,
                            full_name=f"{ds_id} - {topic_name}",
                            type="topic",
                            data_source=ds_id,
                            team=team_id,
                            created_at=datetime.now().isoformat())
            team_node_counts[team_id] += 1
            
            # Add schema for topic if allowed
            if can_create_schemas:
                schema_name = f"{topic_name}_schema"
                schema_id = f"{topic_id}.{schema_name}"
                
                # Add schema node for the topic
                self.G.add_node(schema_id,
                                id=schema_id,
                                name=schema_name,
                                full_name=f"{ds_id} - {topic_name} - Schema",
                                type="schema",
                                data_source=ds_id,
                                topic=topic_name,
                                team=team_id,
                                created_at=datetime.now().isoformat())
                team_node_counts[team_id] += 1
                
                # Connect schema to topic
                self.G.add_edge(topic_id, schema_id, relationship="parent_child")
            
            # Connect topic to producers and consumers
            potential_producers = [n for n in self.G.nodes() 
                                 if self.G.nodes[n].get("type") in ["job", "table"] and 
                                    self.G.nodes[n].get("team") == team_id]
            
            potential_consumers = [n for n in self.G.nodes() 
                                 if self.G.nodes[n].get("type") in ["job", "table", "model"] and 
                                    self.G.nodes[n].get("team") == team_id]
            
            # Connect to 0-3 producers
            if potential_producers:
                producer_count = min(random.randint(0, 3), len(potential_producers))
                if producer_count > 0:
                    producers = random.sample(potential_producers, producer_count)
                    for producer_id in producers:
                        self.add_edge_with_validation(producer_id, topic_id, "produces")
            
            # Connect to 0-5 consumers
            if potential_consumers:
                consumer_count = min(random.randint(0, 5), len(potential_consumers))
                if consumer_count > 0:
                    consumers = random.sample(potential_consumers, consumer_count)
                    for consumer_id in consumers:
                        self.add_edge_with_validation(topic_id, consumer_id, "consumes")

    def _generate_storage_assets(self, team, data_source, team_node_counts, nodes_per_team):
        """Generate storage assets (buckets)"""
        team_id = team["id"]
        ds_id = data_source["id"]
        
        if team_node_counts[team_id] >= nodes_per_team:
            return
        
        # Check if we can create storage assets for this data source
        valid_asset_types = self.get_valid_asset_types_for_data_source(ds_id)
        
        can_create_buckets = "bucket" in valid_asset_types
        
        if not can_create_buckets:
            return
        
        # Generate 2-10 buckets
        bucket_count = random.randint(2, 10)
        
        for i in range(bucket_count):
            if team_node_counts[team_id] >= nodes_per_team:
                return
            
            bucket_name = self._generate_random_name("bucket")
            bucket_id = f"{ds_id}.{bucket_name}"
            
            # Add bucket node
            self.G.add_node(bucket_id,
                            id=bucket_id,
                            name=bucket_name,
                            full_name=f"{ds_id} - {bucket_name}",
                            type="bucket",
                            data_source=ds_id,
                            team=team_id,
                            created_at=datetime.now().isoformat())
            team_node_counts[team_id] += 1
            
            # Connect bucket to jobs that produce or consume data from it
            potential_jobs = [n for n in self.G.nodes() 
                            if self.G.nodes[n].get("type") == "job" and 
                               self.G.nodes[n].get("team") == team_id]
            
            if potential_jobs:
                job_count = min(random.randint(1, 5), len(potential_jobs))
                jobs = random.sample(potential_jobs, job_count)
                
                for job_id in jobs:
                    # Random direction of relationship (job -> bucket or bucket -> job)
                    valid_rels = self.get_valid_relationships("job", "bucket")
                    if valid_rels:
                        rel_type = random.choice(valid_rels)
                        if random.random() < 0.5:
                            self.add_edge_with_validation(job_id, bucket_id, rel_type)
                        else:
                            self.add_edge_with_validation(bucket_id, job_id, rel_type)
            
            # Connect bucket to tables (ETL processes that load data from bucket to tables)
            potential_tables = [n for n in self.G.nodes() 
                              if self.G.nodes[n].get("type") == "table" and 
                                 self.G.nodes[n].get("team") == team_id]
            
            if potential_tables:
                table_count = min(random.randint(0, 3), len(potential_tables))
                if table_count > 0:
                    tables = random.sample(potential_tables, table_count)
                    for table_id in tables:
                        self.add_edge_with_validation(bucket_id, table_id, "populates")

    def _generate_cross_team_lineage(self):
        """Generate lineage connections between assets from different teams"""
        # Get nodes by team
        nodes_by_team = {}
        for team in self.teams:
            team_id = team["id"]
            nodes_by_team[team_id] = [n for n in self.G.nodes() if self.G.nodes[n].get("team") == team_id]
        
        # For each team, create connections to other teams' assets
        for src_team_id, src_nodes in nodes_by_team.items():
            # Filter source nodes to only include tables, views, and models
            src_data_assets = [n for n in src_nodes 
                             if self.G.nodes[n].get("type") in ["table", "view", "model", "topic"]]
            
            if not src_data_assets:
                continue
            
            # Connect to 1-3 other teams
            other_teams = [t["id"] for t in self.teams if t["id"] != src_team_id]
            if not other_teams:
                continue
                
            team_count = min(random.randint(1, 3), len(other_teams))
            target_teams = random.sample(other_teams, team_count)
            
            for tgt_team_id in target_teams:
                # Find potential target assets
                tgt_data_assets = [n for n in nodes_by_team[tgt_team_id] 
                                if self.G.nodes[n].get("type") in ["table", "view", "model"]]
                
                if not tgt_data_assets:
                    continue
                
                # Create 2-10 cross-team connections
                connection_count = min(random.randint(2, 10), len(src_data_assets), len(tgt_data_assets))
                
                for _ in range(connection_count):
                    src_asset = random.choice(src_data_assets)
                    tgt_asset = random.choice(tgt_data_assets)
                    
                    # Get source and target types
                    src_type = self.G.nodes[src_asset].get("type", "unknown")
                    tgt_type = self.G.nodes[tgt_asset].get("type", "unknown")
                    
                    # Get valid relationship types for this pair
                    valid_rels = self.get_valid_relationships(src_type, tgt_type)
                    
                    if valid_rels:
                        rel_type = random.choice(valid_rels)
                        self.add_edge_with_validation(src_asset, tgt_asset, rel_type)
                    
                        # Also create field-level lineage for some connections
                        if random.random() < 0.3:  # 30% chance
                            src_columns = [n for n in self.G.neighbors(src_asset) 
                                         if self.G.nodes[n].get("type") == "column"]
                            tgt_columns = [n for n in self.G.neighbors(tgt_asset) 
                                         if self.G.nodes[n].get("type") == "column"]
                            
                            if src_columns and tgt_columns:
                                # Create 1-5 field-level connections
                                field_count = min(random.randint(1, 5), len(src_columns), len(tgt_columns))
                                
                                for _ in range(field_count):
                                    src_col = random.choice(src_columns)
                                    tgt_col = random.choice(tgt_columns)
                                    
                                    valid_col_rels = self.get_valid_relationships("column", "column")
                                    if valid_col_rels:
                                        field_rel_type = random.choice(valid_col_rels)
                                        self.add_edge_with_validation(src_col, tgt_col, field_rel_type)

    def _add_additional_nodes(self, count):
        """Add additional nodes to reach the minimum node count"""
        # Distribute evenly across teams
        nodes_per_team = count // len(self.teams)
        extra_nodes = count % len(self.teams)
        
        team_allocation = {team["id"]: nodes_per_team for team in self.teams}
        
        # Distribute extra nodes
        for i in range(extra_nodes):
            team_id = self.teams[i % len(self.teams)]["id"]
            team_allocation[team_id] += 1
        
        # Add nodes for each team
        for team in self.teams:
            team_id = team["id"]
            nodes_to_add = team_allocation[team_id]
            
            if nodes_to_add <= 0:
                continue
            
            # Choose a random data source for this team
            # But ensure it supports the asset types we want to create
            valid_data_sources = []
            for ds in self.data_sources:
                if len(self.get_valid_asset_types_for_data_source(ds["id"])) > 0:
                    valid_data_sources.append(ds)
            
            if not valid_data_sources:
                continue
            
            data_source = random.choice(valid_data_sources)
            ds_id = data_source["id"]
            
            # Get valid asset types for this data source
            valid_asset_types = self.get_valid_asset_types_for_data_source(ds_id)
            
            if not valid_asset_types:
                continue
            
            for i in range(nodes_to_add):
                # Pick a valid asset type for this data source
                asset_type = random.choice(valid_asset_types)
                asset_name = self._generate_random_name(asset_type)
                asset_id = f"additional.{team_id}.{asset_type}.{asset_name}"
                
                # Add the node
                self.G.add_node(asset_id,
                                id=asset_id,
                                name=asset_name,
                                full_name=f"Additional {asset_type} - {asset_name}",
                                type=asset_type,
                                data_source=ds_id,
                                team=team_id,
                                created_at=datetime.now().isoformat())
                
                # Connect it to some existing nodes of compatible types
                existing_nodes = [n for n in self.G.nodes() 
                                if self.G.nodes[n].get("team") == team_id and 
                                self.G.nodes[n].get("id") != asset_id]  # Avoid self-connections
                
                if existing_nodes:
                    # Connect to 1-3 existing nodes
                    connection_count = min(random.randint(1, 3), len(existing_nodes))
                    targets = random.sample(existing_nodes, connection_count)
                    
                    for target_id in targets:
                        valid_rels = self.get_valid_relationships(asset_type, self.G.nodes[target_id].get("type", "unknown"))
                        if valid_rels:
                            rel_type = random.choice(valid_rels)
                            self.add_edge_with_validation(asset_id, target_id, rel_type)

    def _add_additional_edges(self, count):
        """Add additional edges to reach the target edge count"""
        # Get all nodes in the graph
        nodes = list(self.G.nodes())
        
        if len(nodes) < 2:
            return
        
        edges_added = 0
        max_attempts = count * 10  # Avoid infinite loop
        attempts = 0
        
        while edges_added < count and attempts < max_attempts:
            attempts += 1
            
            # Pick two random nodes
            source = random.choice(nodes)
            target = random.choice(nodes)
            
            # Avoid self-loops
            if source == target:
                continue
            
            # Avoid duplicate edges
            if self.G.has_edge(source, target):
                continue
            
            # Get types and validate relationship
            source_type = self.G.nodes[source].get("type", "unknown")
            target_type = self.G.nodes[target].get("type", "unknown")
            
            # Also validate that the relationship makes sense given the data source restrictions
            source_ds = self.G.nodes[source].get("data_source", "unknown")
            target_ds = self.G.nodes[target].get("data_source", "unknown")
            
            # Check if these types are valid for their data sources
            if not self.is_valid_asset_for_data_source(source_type, source_ds) or \
               not self.is_valid_asset_for_data_source(target_type, target_ds):
                continue
            
            valid_rels = self.get_valid_relationships(source_type, target_type)
            
            if valid_rels:
                rel_type = random.choice(valid_rels)
                if self.add_edge_with_validation(source, target, rel_type):
                    edges_added += 1

    def _create_orphaned_nodes(self):
        """
        Create orphaned nodes by removing all edges connected to selected nodes
        """
        print("Creating orphaned nodes...")
        all_nodes = list(self.G.nodes())
        num_orphans = int(len(all_nodes) * self.orphaned_node_percent)
        
        # Select nodes to orphan - prefer nodes that are not critical to the graph structure
        # Avoid orphaning schema nodes or parent nodes with many children
        candidate_nodes = [node for node in all_nodes if 
                          self.G.nodes[node].get('type') not in ['schema', 'workflow', 'dashboard'] and
                          len(list(self.G.successors(node))) <= 3 and
                          len(list(self.G.predecessors(node))) <= 3]
        
        if len(candidate_nodes) < num_orphans:
            candidate_nodes = all_nodes
        
        orphan_nodes = random.sample(candidate_nodes, min(num_orphans, len(candidate_nodes)))
        
        # Remove all edges connected to these nodes
        for node in orphan_nodes:
            # Get all edges connected to this node
            in_edges = list(self.G.in_edges(node))
            out_edges = list(self.G.out_edges(node))
            
            # Remove edges
            self.G.remove_edges_from(in_edges)
            self.G.remove_edges_from(out_edges)
            
            # Mark the node as orphaned
            self.G.nodes[node]['orphaned'] = True
            
        print(f"Created {len(orphan_nodes)} orphaned nodes")
    
    def _create_disconnected_subgraphs(self):
        """
        Create disconnected subgraphs by:
        1. Creating small groups of connected nodes
        2. Ensuring these groups are not connected to the main graph
        """
        print("Creating disconnected subgraphs...")
        
        for i in range(self.disconnected_subgraphs):
            # Create a small subgraph (5-15 nodes)
            subgraph_size = random.randint(5, 15)
            subgraph_team = random.choice(self.teams)["id"]
            
            # Select a random data source
            data_source = random.choice(self.data_sources)
            ds_id = data_source["id"]
            
            # Create central node for this subgraph
            central_type = random.choice(["table", "model", "dashboard", "workflow"])
            central_name = self._generate_random_name(f"disconnected_{central_type}")
            central_id = f"disconnected.{i}.{central_type}.{central_name}"
            
            # Add central node
            self.G.add_node(central_id,
                           id=central_id,
                           name=central_name,
                           full_name=f"Disconnected {central_type} - {central_name}",
                           type=central_type,
                           data_source=ds_id,
                           team=subgraph_team,
                           created_at=datetime.now().isoformat(),
                           is_disconnected_subgraph=True)
            
            # Create child nodes
            for j in range(subgraph_size - 1):
                # Choose appropriate child type based on central node
                if central_type == "table":
                    child_type = "column" if random.random() < 0.8 else "view"
                elif central_type == "model":
                    child_type = "column" if random.random() < 0.7 else "model"
                elif central_type == "dashboard":
                    child_type = random.choice(["report", "metric", "dimension"])
                else:  # workflow
                    child_type = "job"
                
                child_name = self._generate_random_name(f"disconnected_{child_type}")
                child_id = f"disconnected.{i}.{child_type}.{child_name}"
                
                # Add child node
                self.G.add_node(child_id,
                               id=child_id,
                               name=child_name,
                               full_name=f"Disconnected {child_type} - {child_name}",
                               type=child_type,
                               data_source=ds_id,
                               team=subgraph_team,
                               created_at=datetime.now().isoformat(),
                               is_disconnected_subgraph=True)
                
                # Connect to central node or another node in the subgraph
                if random.random() < 0.7 or j < 3:  # Ensure the first few nodes connect to central
                    # Connect to central node with appropriate relationship
                    valid_rels = self.get_valid_relationships(central_type, child_type)
                    if valid_rels:
                        rel_type = random.choice(valid_rels)
                        self.G.add_edge(central_id, child_id, relationship=rel_type)
                else:
                    # Connect to another node in the subgraph
                    # Get all subgraph nodes except the current child
                    subgraph_nodes = [node for node in self.G.nodes() 
                                     if self.G.nodes[node].get('is_disconnected_subgraph') == True and
                                     node != child_id]
                    
                    if subgraph_nodes:
                        potential_parent = random.choice(subgraph_nodes)
                        parent_type = self.G.nodes[potential_parent].get('type')
                        
                        valid_rels = self.get_valid_relationships(parent_type, child_type)
                        if valid_rels:
                            rel_type = random.choice(valid_rels)
                            self.G.add_edge(potential_parent, child_id, relationship=rel_type)
            
            # Create a few edges between nodes in the subgraph to ensure it's connected internally
            subgraph_nodes = [node for node in self.G.nodes() 
                             if self.G.nodes[node].get('is_disconnected_subgraph') == True]
            
            # Get all possible pairs of nodes in the subgraph
            for _ in range(min(5, len(subgraph_nodes))):
                source = random.choice(subgraph_nodes)
                target = random.choice(subgraph_nodes)
                
                # Avoid self-loops
                if source != target and not self.G.has_edge(source, target):
                    source_type = self.G.nodes[source].get('type')
                    target_type = self.G.nodes[target].get('type')
                    
                    valid_rels = self.get_valid_relationships(source_type, target_type)
                    if valid_rels:
                        rel_type = random.choice(valid_rels)
                        self.G.add_edge(source, target, relationship=rel_type)
        
        print(f"Created {self.disconnected_subgraphs} disconnected subgraphs")
    
    def _analyze_connectivity(self):
        """Analyze the connectivity of the graph"""
        # Convert to undirected graph to find connected components
        undirected = self.G.to_undirected()
        
        # Find connected components
        connected_components = list(nx.connected_components(undirected))
        
        # Count orphaned nodes (nodes with no connections)
        orphaned_nodes = [node for node in self.G.nodes() 
                        if self.G.in_degree(node) == 0 and self.G.out_degree(node) == 0]
        
        # Categorize components by size
        component_sizes = [len(comp) for comp in connected_components]
        
        # Get the largest connected component (main graph)
        largest_component_size = max(component_sizes) if component_sizes else 0
        largest_component_percentage = (largest_component_size / len(self.G.nodes())) * 100 if self.G.nodes() else 0
        
        return {
            "connected_components": len(connected_components),
            "component_sizes": component_sizes,
            "orphaned_nodes": len(orphaned_nodes),
            "orphaned_node_ids": orphaned_nodes,
            "largest_component_size": largest_component_size,
            "largest_component_percentage": largest_component_percentage,
            "disconnected_nodes_percentage": 100 - largest_component_percentage
        }
    
    def save_graph(self, format="gexf"):
        """Save the generated graph to file"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        if format == "gexf":
            filename = os.path.join(self.output_dir, f"lineage_graph_{timestamp}.gexf")
            nx.write_gexf(self.G, filename)
        elif format == "graphml":
            filename = os.path.join(self.output_dir, f"lineage_graph_{timestamp}.graphml")
            nx.write_graphml(self.G, filename)
        elif format == "json":
            filename = os.path.join(self.output_dir, f"lineage_graph_{timestamp}.json")
            data = {
                "nodes": [{"id": node, **self.G.nodes[node]} for node in self.G.nodes()],
                "edges": [{"source": u, "target": v, **self.G.edges[u, v]} for u, v in self.G.edges()]
            }
            with open(filename, 'w') as f:
                json.dump(data, f)
        else:
            raise ValueError(f"Unsupported output format: {format}")
        
        print(f"Graph saved to {filename}")
        return filename

    def get_graph_stats(self):
        """Get statistics about the generated graph"""
        # Count nodes by type
        node_types = defaultdict(int)
        for node in self.G.nodes():
            node_type = self.G.nodes[node].get("type", "unknown")
            node_types[node_type] += 1
        
        # Count edges by relationship type
        edge_types = defaultdict(int)
        for u, v in self.G.edges():
            rel_type = self.G.edges[u, v].get("relationship", "unknown")
            edge_types[rel_type] += 1
        
        # Count nodes by team
        team_nodes = defaultdict(int)
        for node in self.G.nodes():
            team = self.G.nodes[node].get("team", "unknown")
            team_nodes[team] += 1
        
        # Count nodes by data source
        ds_nodes = defaultdict(int)
        for node in self.G.nodes():
            ds = self.G.nodes[node].get("data_source", "unknown")
            ds_nodes[ds] += 1
            
        # Count nodes by popularity score range
        popularity_ranges = {
            "heavy_usage (70-100)": 0,
            "moderate_usage (40-69)": 0,
            "light_usage (1-39)": 0,
            "no_usage (-1)": 0
        }
        
        for node in self.G.nodes():
            score = self.G.nodes[node].get("score", -1)
            if score >= 70:
                popularity_ranges["heavy_usage (70-100)"] += 1
            elif score >= 40:
                popularity_ranges["moderate_usage (40-69)"] += 1
            elif score >= 1:
                popularity_ranges["light_usage (1-39)"] += 1
            else:
                popularity_ranges["no_usage (-1)"] += 1
        
        # Get connectivity stats
        connectivity_stats = self._analyze_connectivity()
        
        return {
            "total_nodes": len(self.G.nodes()),
            "total_edges": len(self.G.edges()),
            "node_types": dict(node_types),
            "edge_types": dict(edge_types),
            "team_nodes": dict(team_nodes),
            "data_source_nodes": dict(ds_nodes)
        }

    def analyze_graph(self):
        """Perform additional analysis on the graph"""
        results = {}
        
        # Calculate degree centrality
        results["degree_centrality"] = nx.degree_centrality(self.G)
        
        # Calculate in-degree and out-degree centrality
        results["in_degree_centrality"] = nx.in_degree_centrality(self.G)
        results["out_degree_centrality"] = nx.out_degree_centrality(self.G)
        
        # Find sources (no incoming edges) and sinks (no outgoing edges)
        results["sources"] = [n for n in self.G.nodes() if self.G.in_degree(n) == 0]
        results["sinks"] = [n for n in self.G.nodes() if self.G.out_degree(n) == 0]
        
        # Find strongly connected components
        results["strongly_connected_components"] = list(nx.strongly_connected_components(self.G))
        
        # Calculate average path length for the largest connected component
        undirected = self.G.to_undirected()
        connected_components = list(nx.connected_components(undirected))
        if connected_components:
            largest_component = max(connected_components, key=len)
            largest_component_subgraph = self.G.subgraph(largest_component).copy()
            
            try:
                results["average_path_length"] = nx.average_shortest_path_length(largest_component_subgraph)
            except:
                results["average_path_length"] = "Not applicable (not strongly connected)"
        else:
            results["average_path_length"] = "Not applicable (no connected components)"
        
        # Analyze disconnected components
        results["connectivity"] = self._analyze_connectivity()
        
        return results