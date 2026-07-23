from neo4j import GraphDatabase
from typing import Dict, List, Optional, Any
import logging
import json
from datetime import datetime

logger = logging.getLogger(__name__)

class ModelStore:
    def __init__(self, uri: str, user: str, password: str):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
        
    def close(self):
        self.driver.close()
        
    def register_model(self, name: str, version: str, schema: Dict, creator_id: int,
                      embeddings: List[float], is_public: bool = True, tags: List[str] = None):
        with self.driver.session() as session:
            return session.execute_write(self._create_model_node,
                name, version, schema, creator_id, embeddings, is_public, tags or [])
    
    @staticmethod
    def _create_model_node(tx, name, version, schema, creator_id, embeddings, is_public, tags):
        query = """
        MERGE (m:Model {name: $name})
        CREATE (v:ModelVersion {
            version: $version,
            schema: $schema,
            embeddings: $embeddings,
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
                       creator_id=creator_id, embeddings=embeddings,
                       is_public=is_public, tags=tags)
        return result.single()

    def get_model_versions(self, name: str) -> List[Dict]:
        with self.driver.session() as session:
            return session.execute_read(self._get_model_versions, name)
    
    @staticmethod
    def _get_model_versions(tx, name):
        query = """
        MATCH (m:Model {name: $name})-[:HAS_VERSION]->(v:ModelVersion)
        OPTIONAL MATCH (v)<-[:CREATED]-(u:User)
        OPTIONAL MATCH (v)-[:TAGGED]->(t:Tag)
        RETURN v.version as version, v.schema as schema,
               v.embeddings as embeddings,
               v.created_at as created_at, v.is_public as is_public,
               u.id as creator_id,
               collect(t.name) as tags
        ORDER BY v.created_at DESC
        """
        result = tx.run(query, name=name)
        return [dict(record) for record in result]

    def search_similar_models(self, embeddings: List[float], limit: int = 5) -> List[Dict]:
        with self.driver.session() as session:
            return session.execute_read(self._search_similar_models, embeddings, limit)
    
    @staticmethod
    def _search_similar_models(tx, embeddings, limit):
        # Using cosine similarity in Neo4j
        query = """
        MATCH (m:Model)-[:HAS_VERSION]->(v:ModelVersion)
        WITH m, v, gds.similarity.cosine(v.embeddings, $embeddings) AS similarity
        WHERE similarity > 0.7
        RETURN m.name as model, v.version as version, v.schema as schema,
               v.created_at as created_at, similarity
        ORDER BY similarity DESC
        LIMIT $limit
        """
        result = tx.run(query, embeddings=embeddings, limit=limit)
        return [dict(record) for record in result]
