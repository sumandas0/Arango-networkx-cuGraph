"""
Utility module for validating lineage relationships between different asset types
"""

class RelationshipValidator:
    """
    Validates relationships between different asset types in data lineage
    """
    
    def __init__(self):
        """Initialize the validator with default relationship rules"""
        self.valid_relationships = self._get_default_relationships()
    
    def _get_default_relationships(self):
        """Define the default valid relationships between different asset types"""
        return {
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
        """
        Check if a relationship between source and target types is valid
        
        Args:
            source_type: The type of the source node
            target_type: The type of the target node
            relationship_type: The type of relationship
            
        Returns:
            bool: True if the relationship is valid, False otherwise
        """
        # Check direct relationship
        if (source_type, target_type) in self.valid_relationships:
            return relationship_type in self.valid_relationships[(source_type, target_type)]
        
        # Check for any wildcard relationships
        if (source_type, "*") in self.valid_relationships:
            return relationship_type in self.valid_relationships[(source_type, "*")]
        
        if ("*", target_type) in self.valid_relationships:
            return relationship_type in self.valid_relationships[("*", target_type)]
        
        # Default: relationship not defined, so not valid
        return False
    
    def get_valid_relationships(self, source_type, target_type):
        """
        Get all valid relationship types between source and target asset types
        
        Args:
            source_type: The type of the source node
            target_type: The type of the target node
            
        Returns:
            list: List of valid relationship types, empty if none
        """
        if (source_type, target_type) in self.valid_relationships:
            return self.valid_relationships[(source_type, target_type)]
        
        # Check for wildcards
        if (source_type, "*") in self.valid_relationships:
            return self.valid_relationships[(source_type, "*")]
        
        if ("*", target_type) in self.valid_relationships:
            return self.valid_relationships[("*", target_type)]
        
        return []
    
    def add_relationship_rule(self, source_type, target_type, relationship_types):
        """
        Add a new relationship rule or update an existing one
        
        Args:
            source_type: The type of the source node
            target_type: The type of the target node
            relationship_types: List of valid relationship types
        """
        self.valid_relationships[(source_type, target_type)] = relationship_types
    
    def get_all_rules(self):
        """
        Get all relationship validation rules
        
        Returns:
            dict: All relationship rules
        """
        return self.valid_relationships
