from neo4j import GraphDatabase
from typing import Dict, List, Optional, Any
import logging
from model_store import ModelStore

class Neo4jStore:
    def __init__(self, uri: str, user: str, password: str):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
        self.model_store = ModelStore(uri, user, password)
        
    def close(self):
        self.driver.close()
        
    def register_model(self, name: str, version: str, schema: Dict, creator_id: int, 
                      is_public: bool = True, tags: List[str] = None):
        with self.driver.session() as session:
            return session.execute_write(self._create_model_node,
                name, version, schema, creator_id, is_public, tags or [])
    
    @staticmethod
    def _create_model_node(tx, name, version, schema, creator_id, is_public, tags):
        query = """
        MERGE (m:Model {name: $name})
        CREATE (v:Version {
            version: $version,
            schema: $schema,
            created_at: datetime(),
            is_public: $is_public
        })
        CREATE (m)-[:HAS_VERSION]->(v)
        WITH m, v
        MATCH (u:User {id: $creator_id})
        CREATE (u)-[:CREATED]->(v)
        FOREACH (tag IN $tags |
            MERGE (t:Tag {name: tag})
            CREATE (v)-[:TAGGED]->(t)
        )
        RETURN m.name as model, v.version as version
        """
        result = tx.run(query, name=name, version=version, schema=schema,
                       creator_id=creator_id, is_public=is_public, tags=tags)
        return result.single()

    def get_model_versions(self, name: str) -> List[Dict]:
        with self.driver.session() as session:
            return session.execute_read(self._get_model_versions, name)
    
    @staticmethod
    def _get_model_versions(tx, name):
        query = """
        MATCH (m:Model {name: $name})-[:HAS_VERSION]->(v:Version)
        OPTIONAL MATCH (v)<-[:CREATED]-(u:User)
        OPTIONAL MATCH (v)-[:TAGGED]->(t:Tag)
        RETURN v.version as version, v.schema as schema,
               v.created_at as created_at, v.is_public as is_public,
               u.id as creator_id,
               collect(t.name) as tags
        ORDER BY v.created_at DESC
        """
        result = tx.run(query, name=name)
        return [dict(record) for record in result]

    def search_models(self, query: str = None, tags: List[str] = None, 
                     creator_id: Optional[int] = None, embeddings: Optional[List[float]] = None,
                     limit: int = 5) -> List[Dict]:
        if embeddings:
            return self.model_store.search_similar_models(embeddings, limit)
        with self.driver.session() as session:
            return session.execute_read(self._search_models, query, tags, creator_id)
    
    @staticmethod
    def _search_models(tx, query, tags, creator_id):
        conditions = []
        params = {}
        
        if query:
            conditions.append("m.name =~ $query")
            params["query"] = f"(?i).*{query}.*"
        
        if creator_id:
            conditions.append("creator.id = $creator_id")
            params["creator_id"] = creator_id
            
        if tags:
            conditions.append("ALL(tag IN $tags WHERE (v)-[:TAGGED]->(:Tag {name: tag}))")
            params["tags"] = tags
            
        where_clause = " AND ".join(conditions)
        if where_clause:
            where_clause = f"WHERE {where_clause}"
            
        query = f"""
        MATCH (m:Model)-[:HAS_VERSION]->(v:Version)<-[:CREATED]-(creator:User)
        {where_clause}
        OPTIONAL MATCH (v)-[:TAGGED]->(t:Tag)
        RETURN m.name as model, v.version as version,
               v.schema as schema, v.created_at as created_at,
               v.is_public as is_public, creator.id as creator_id,
               collect(t.name) as tags
        ORDER BY v.created_at DESC
        """
        result = tx.run(query, **params)
        return [dict(record) for record in result]
