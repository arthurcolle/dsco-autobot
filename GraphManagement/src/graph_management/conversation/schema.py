"""
Schema generation for conversation graph.

Generates Cypher DDL for constraints and indexes.
"""


def generate_schema_ddl() -> str:
    """
    Generate Cypher DDL for conversation graph schema.

    Returns:
        Cypher statements to create constraints and indexes
    """
    statements = []

    # Constraints (unique IDs)
    statements.append(
        "CREATE CONSTRAINT conv_id IF NOT EXISTS "
        "FOR (c:Conversation) REQUIRE c.id IS UNIQUE;"
    )
    statements.append(
        "CREATE CONSTRAINT msg_id IF NOT EXISTS "
        "FOR (m:Message) REQUIRE m.id IS UNIQUE;"
    )
    statements.append(
        "CREATE CONSTRAINT blk_id IF NOT EXISTS "
        "FOR (b:Block) REQUIRE b.id IS UNIQUE;"
    )
    statements.append(
        "CREATE CONSTRAINT tc_id IF NOT EXISTS "
        "FOR (t:ToolCall) REQUIRE t.id IS UNIQUE;"
    )
    statements.append(
        "CREATE CONSTRAINT tr_id IF NOT EXISTS "
        "FOR (t:ToolResult) REQUIRE t.id IS UNIQUE;"
    )
    statements.append(
        "CREATE CONSTRAINT ent_name IF NOT EXISTS "
        "FOR (e:Entity) REQUIRE e.name IS UNIQUE;"
    )

    # B-tree indexes for lookups
    statements.append(
        "CREATE INDEX msg_conv IF NOT EXISTS "
        "FOR (m:Message) ON (m.conversation_id);"
    )
    statements.append(
        "CREATE INDEX msg_role IF NOT EXISTS "
        "FOR (m:Message) ON (m.role);"
    )
    statements.append(
        "CREATE INDEX blk_msg IF NOT EXISTS "
        "FOR (b:Block) ON (b.message_id);"
    )
    statements.append(
        "CREATE INDEX tc_msg IF NOT EXISTS "
        "FOR (t:ToolCall) ON (t.message_id);"
    )
    statements.append(
        "CREATE INDEX tr_call IF NOT EXISTS "
        "FOR (t:ToolResult) ON (t.call_id);"
    )

    # Fulltext indexes for search
    statements.append(
        "CREATE FULLTEXT INDEX msg_text IF NOT EXISTS "
        "FOR (m:Message) ON EACH [m.text];"
    )
    statements.append(
        "CREATE FULLTEXT INDEX conv_title IF NOT EXISTS "
        "FOR (c:Conversation) ON EACH [c.title];"
    )

    # Vector indexes (Neo4j 5+ syntax)
    statements.append(
        "CREATE VECTOR INDEX msg_content_emb IF NOT EXISTS "
        "FOR (m:Message) ON (m.embedding_content) "
        "OPTIONS {indexConfig: {"
        "  `vector.dimensions`: 1024, "
        "  `vector.similarity_function`: 'cosine'"
        "}};"
    )
    statements.append(
        "CREATE VECTOR INDEX blk_content_emb IF NOT EXISTS "
        "FOR (b:Block) ON (b.embedding_content) "
        "OPTIONS {indexConfig: {"
        "  `vector.dimensions`: 512, "
        "  `vector.similarity_function`: 'cosine'"
        "}};"
    )

    return "\n".join(statements)
