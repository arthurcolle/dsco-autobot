# Distributed, Systems & Co. — Platform Specification v1.0

## Executive Summary

This document specifies the complete architecture for an autonomous agent platform built on GraphSub, our proprietary Rust graph substrate. The platform enables developers to build, deploy, and manage AI agents that can reason, use tools, maintain memory, and operate autonomously.

---

## Part 1: Data Layer (GraphSub Schemas)

### 1.1 Core Entity Schemas

```rust
// ═══════════════════════════════════════════════════════════════════════════
// USER & AUTH
// ═══════════════════════════════════════════════════════════════════════════

NodeSchema::new("User")
    .field(FieldDef::new("id", FieldType::String).required().unique())
    .field(FieldDef::new("email", FieldType::String).required().unique())
    .field(FieldDef::new("password_hash", FieldType::String).required())
    .field(FieldDef::new("slug", FieldType::String).unique())
    .field(FieldDef::new("credits_usd", FieldType::Float64).default(0.0))
    .field(FieldDef::new("stripe_customer_id", FieldType::String))
    .field(FieldDef::new("created_at", FieldType::DateTime).indexed())
    .field(FieldDef::new("last_login", FieldType::DateTime))
    .strict();

NodeSchema::new("ApiKey")
    .field(FieldDef::new("id", FieldType::String).required().unique())
    .field(FieldDef::new("key_hash", FieldType::String).required())
    .field(FieldDef::new("name", FieldType::String))
    .field(FieldDef::new("prefix", FieldType::String).indexed())  // "dsk_abc..."
    .field(FieldDef::new("scopes", FieldType::StringArray))  // ["agents:read", "tools:execute"]
    .field(FieldDef::new("rate_limit", FieldType::Int32).default(1000))
    .field(FieldDef::new("created_at", FieldType::DateTime))
    .field(FieldDef::new("last_used", FieldType::DateTime))
    .field(FieldDef::new("expires_at", FieldType::DateTime))
    .strict();

EdgeSchema::new("OWNS_KEY")
    .from(vec!["User".into()])
    .to(vec!["ApiKey".into()]);

// ═══════════════════════════════════════════════════════════════════════════
// AGENT
// ═══════════════════════════════════════════════════════════════════════════

NodeSchema::new("Agent")
    .field(FieldDef::new("id", FieldType::String).required().unique())
    .field(FieldDef::new("name", FieldType::String).required())
    .field(FieldDef::new("description", FieldType::Text).fulltext())
    .field(FieldDef::new("system_prompt", FieldType::Text).fulltext())
    .field(FieldDef::new("model", FieldType::String).default("claude-sonnet-4-20250514"))
    .field(FieldDef::new("temperature", FieldType::Float32).default(0.7))
    .field(FieldDef::new("max_tokens", FieldType::Int32).default(4096))
    .field(FieldDef::new("max_turns", FieldType::Int32).default(50))
    .field(FieldDef::new("memory_config", FieldType::Json))
    .field(FieldDef::new("metadata", FieldType::Json))
    .field(FieldDef::new("created_at", FieldType::DateTime).indexed())
    .field(FieldDef::new("updated_at", FieldType::DateTime))
    .field(FieldDef::new("embedding", FieldType::Vector(1024)).vector_indexed())
    .embedding(EmbeddingConfig::new("semantic", vec!["name", "description", "system_prompt"], "embedding"))
    .strict();

NodeSchema::new("AgentRun")
    .field(FieldDef::new("id", FieldType::String).required().unique())
    .field(FieldDef::new("status", FieldType::String).indexed())  // pending|running|completed|failed|interrupted
    .field(FieldDef::new("trigger", FieldType::String))  // api|schedule|webhook|manual
    .field(FieldDef::new("input", FieldType::Json))
    .field(FieldDef::new("output", FieldType::Json))
    .field(FieldDef::new("error", FieldType::Text))
    .field(FieldDef::new("started_at", FieldType::DateTime).indexed())
    .field(FieldDef::new("completed_at", FieldType::DateTime))
    .field(FieldDef::new("total_tokens", FieldType::Int64))
    .field(FieldDef::new("total_cost_usd", FieldType::Float64))
    .field(FieldDef::new("tool_calls", FieldType::Int32))
    .field(FieldDef::new("turns", FieldType::Int32))
    .strict();

NodeSchema::new("Checkpoint")
    .field(FieldDef::new("id", FieldType::String).required().unique())
    .field(FieldDef::new("step", FieldType::Int32).indexed())
    .field(FieldDef::new("state", FieldType::Json))  // Serialized agent state
    .field(FieldDef::new("created_at", FieldType::DateTime))
    .strict();

EdgeSchema::new("OWNS_AGENT")
    .from(vec!["User".into()])
    .to(vec!["Agent".into()]);

EdgeSchema::new("HAS_RUN")
    .from(vec!["Agent".into()])
    .to(vec!["AgentRun".into()])
    .field(FieldDef::new("sequence", FieldType::Int32));

EdgeSchema::new("HAS_CHECKPOINT")
    .from(vec!["AgentRun".into()])
    .to(vec!["Checkpoint".into()]);

EdgeSchema::new("USES_TOOL")
    .from(vec!["Agent".into()])
    .to(vec!["Tool".into()])
    .field(FieldDef::new("priority", FieldType::Int32))
    .field(FieldDef::new("enabled", FieldType::Bool).default(true));

// ═══════════════════════════════════════════════════════════════════════════
// CONVERSATION
// ═══════════════════════════════════════════════════════════════════════════

NodeSchema::new("Conversation")
    .field(FieldDef::new("id", FieldType::String).required().unique())
    .field(FieldDef::new("title", FieldType::String))
    .field(FieldDef::new("system_prompt", FieldType::Text))
    .field(FieldDef::new("model", FieldType::String))
    .field(FieldDef::new("metadata", FieldType::Json))
    .field(FieldDef::new("message_count", FieldType::Int32).default(0))
    .field(FieldDef::new("total_tokens", FieldType::Int64).default(0))
    .field(FieldDef::new("created_at", FieldType::DateTime).indexed())
    .field(FieldDef::new("updated_at", FieldType::DateTime).indexed())
    .field(FieldDef::new("embedding", FieldType::Vector(1024)).vector_indexed())
    .embedding(EmbeddingConfig::new("semantic", vec!["title", "system_prompt"], "embedding").on_update())
    .strict();

NodeSchema::new("Message")
    .field(FieldDef::new("id", FieldType::String).required().unique())
    .field(FieldDef::new("role", FieldType::String).required().indexed())  // user|assistant|system|tool
    .field(FieldDef::new("content_text", FieldType::Text).fulltext())  // Aggregated text for search
    .field(FieldDef::new("tool_call_id", FieldType::String).indexed())  // For tool results
    .field(FieldDef::new("name", FieldType::String))  // Tool name for tool messages
    .field(FieldDef::new("created_at", FieldType::DateTime).indexed())
    .field(FieldDef::new("input_tokens", FieldType::Int32))
    .field(FieldDef::new("output_tokens", FieldType::Int32))
    .field(FieldDef::new("latency_ms", FieldType::Int32))
    .field(FieldDef::new("cost_usd", FieldType::Float64))
    .field(FieldDef::new("model", FieldType::String))
    .field(FieldDef::new("finish_reason", FieldType::String))  // stop|tool_calls|length|error
    .field(FieldDef::new("embedding", FieldType::Vector(1024)).vector_indexed())
    .embedding(EmbeddingConfig::new("semantic", vec!["content_text"], "embedding"))
    .strict();

NodeSchema::new("ContentBlock")
    .field(FieldDef::new("id", FieldType::String).required().unique())
    .field(FieldDef::new("type", FieldType::String).required().indexed())  // text|code|image|tool_use|tool_result
    .field(FieldDef::new("sequence", FieldType::Int32).indexed())
    .field(FieldDef::new("text", FieldType::Text).fulltext())
    .field(FieldDef::new("code", FieldType::Text))
    .field(FieldDef::new("language", FieldType::String))
    .field(FieldDef::new("tool_use_id", FieldType::String).indexed())
    .field(FieldDef::new("tool_name", FieldType::String))
    .field(FieldDef::new("input_json", FieldType::Json))
    .field(FieldDef::new("output_json", FieldType::Json))
    .field(FieldDef::new("is_error", FieldType::Bool).default(false))
    .field(FieldDef::new("media_type", FieldType::String))  // image/png, audio/mp3
    .field(FieldDef::new("media_url", FieldType::String))
    .field(FieldDef::new("embedding", FieldType::Vector(1024)).vector_indexed())
    .embedding(EmbeddingConfig::new("semantic", vec!["text", "code"], "embedding"));

EdgeSchema::new("OWNS_CONVERSATION")
    .from(vec!["User".into()])
    .to(vec!["Conversation".into()]);

EdgeSchema::new("AGENT_CONVERSATION")
    .from(vec!["Agent".into()])
    .to(vec!["Conversation".into()]);

EdgeSchema::new("RUN_CONVERSATION")
    .from(vec!["AgentRun".into()])
    .to(vec!["Conversation".into()]);

EdgeSchema::new("HAS_MESSAGE")
    .from(vec!["Conversation".into()])
    .to(vec!["Message".into()])
    .field(FieldDef::new("sequence", FieldType::Int32).indexed());

EdgeSchema::new("HAS_BLOCK")
    .from(vec!["Message".into()])
    .to(vec!["ContentBlock".into()])
    .field(FieldDef::new("sequence", FieldType::Int32));

EdgeSchema::new("REPLIES_TO")
    .from(vec!["Message".into()])
    .to(vec!["Message".into()]);

EdgeSchema::new("BRANCHES_FROM")
    .from(vec!["Message".into()])
    .to(vec!["Message".into()]);

// ═══════════════════════════════════════════════════════════════════════════
// MEMORY
// ═══════════════════════════════════════════════════════════════════════════

NodeSchema::new("Memory")
    .field(FieldDef::new("id", FieldType::String).required().unique())
    .field(FieldDef::new("type", FieldType::String).required().indexed())  // working|episodic|semantic|procedural
    .field(FieldDef::new("content", FieldType::Text).fulltext())
    .field(FieldDef::new("summary", FieldType::Text))
    .field(FieldDef::new("importance", FieldType::Float32).indexed().default(0.5))  // 0.0-1.0
    .field(FieldDef::new("access_count", FieldType::Int32).default(0))
    .field(FieldDef::new("last_accessed", FieldType::DateTime))
    .field(FieldDef::new("created_at", FieldType::DateTime).indexed())
    .field(FieldDef::new("expires_at", FieldType::DateTime))  // For working memory TTL
    .field(FieldDef::new("source_type", FieldType::String))  // conversation|tool|observation|consolidation
    .field(FieldDef::new("source_id", FieldType::String))
    .field(FieldDef::new("metadata", FieldType::Json))
    .field(FieldDef::new("embedding", FieldType::Vector(1024)).vector_indexed())
    .embedding(EmbeddingConfig::new("semantic", vec!["content", "summary"], "embedding"))
    .strict();

EdgeSchema::new("AGENT_MEMORY")
    .from(vec!["Agent".into()])
    .to(vec!["Memory".into()])
    .field(FieldDef::new("memory_type", FieldType::String));

EdgeSchema::new("REMEMBERS")
    .from(vec!["Memory".into()])
    .to(vec!["Memory".into()])
    .field(FieldDef::new("relation", FieldType::String))  // caused_by|similar_to|contradicts|supports
    .field(FieldDef::new("strength", FieldType::Float32));

EdgeSchema::new("CONSOLIDATED_FROM")
    .from(vec!["Memory".into()])
    .to(vec!["Memory".into()])
    .field(FieldDef::new("consolidation_type", FieldType::String));  // summarization|abstraction|merge

EdgeSchema::new("DERIVED_FROM_MESSAGE")
    .from(vec!["Memory".into()])
    .to(vec!["Message".into()]);

// ═══════════════════════════════════════════════════════════════════════════
// TOOL (enhanced from existing)
// ═══════════════════════════════════════════════════════════════════════════

NodeSchema::new("Tool")
    .field(FieldDef::new("id", FieldType::String).required().unique())
    .field(FieldDef::new("name", FieldType::String).required().unique())
    .field(FieldDef::new("description", FieldType::Text).fulltext())
    .field(FieldDef::new("version", FieldType::String).default("1.0.0"))
    .field(FieldDef::new("category", FieldType::String).indexed())
    .field(FieldDef::new("tags", FieldType::StringArray))
    .field(FieldDef::new("enabled", FieldType::Bool).default(true).indexed())
    .field(FieldDef::new("is_public", FieldType::Bool).default(false))
    .field(FieldDef::new("execution_count", FieldType::Int64).default(0))
    .field(FieldDef::new("avg_latency_ms", FieldType::Float32))
    .field(FieldDef::new("success_rate", FieldType::Float32))
    .field(FieldDef::new("created_at", FieldType::DateTime).indexed())
    .field(FieldDef::new("updated_at", FieldType::DateTime))
    .field(FieldDef::new("embedding", FieldType::Vector(1024)).vector_indexed())
    .embedding(EmbeddingConfig::new("semantic", vec!["name", "description"], "embedding"))
    .strict();

NodeSchema::new("ToolAction")
    .field(FieldDef::new("id", FieldType::String).required().unique())
    .field(FieldDef::new("name", FieldType::String).required())
    .field(FieldDef::new("description", FieldType::Text))
    .field(FieldDef::new("http_method", FieldType::String))  // GET|POST|PUT|DELETE
    .field(FieldDef::new("endpoint", FieldType::String))
    .field(FieldDef::new("auth_type", FieldType::String))  // none|bearer|api_key|oauth2|basic
    .field(FieldDef::new("auth_config", FieldType::Json))  // {header: "X-API-Key", env_var: "..."}
    .field(FieldDef::new("headers", FieldType::Json))
    .field(FieldDef::new("timeout_ms", FieldType::Int32).default(30000))
    .field(FieldDef::new("retry_count", FieldType::Int32).default(3))
    .field(FieldDef::new("retry_delay_ms", FieldType::Int32).default(1000))
    .field(FieldDef::new("is_streaming", FieldType::Bool).default(false))
    .field(FieldDef::new("is_async", FieldType::Bool).default(false))
    .strict();

NodeSchema::new("ToolSchema")
    .field(FieldDef::new("id", FieldType::String).required().unique())
    .field(FieldDef::new("input_schema", FieldType::Json).required())  // JSON Schema
    .field(FieldDef::new("output_schema", FieldType::Json))
    .field(FieldDef::new("example_input", FieldType::Json))
    .field(FieldDef::new("example_output", FieldType::Json))
    .strict();

NodeSchema::new("ToolExecution")
    .field(FieldDef::new("id", FieldType::String).required().unique())
    .field(FieldDef::new("status", FieldType::String).indexed())  // pending|running|succeeded|failed|timeout
    .field(FieldDef::new("input", FieldType::Json))
    .field(FieldDef::new("output", FieldType::Json))
    .field(FieldDef::new("error", FieldType::Text))
    .field(FieldDef::new("started_at", FieldType::DateTime).indexed())
    .field(FieldDef::new("completed_at", FieldType::DateTime))
    .field(FieldDef::new("latency_ms", FieldType::Int32))
    .field(FieldDef::new("retry_count", FieldType::Int32).default(0))
    .strict();

EdgeSchema::new("OWNS_TOOL")
    .from(vec!["User".into()])
    .to(vec!["Tool".into()]);

EdgeSchema::new("HAS_ACTION")
    .from(vec!["Tool".into()])
    .to(vec!["ToolAction".into()]);

EdgeSchema::new("HAS_SCHEMA")
    .from(vec!["ToolAction".into()])
    .to(vec!["ToolSchema".into()]);

EdgeSchema::new("TOOL_EXECUTION")
    .from(vec!["AgentRun".into()])
    .to(vec!["ToolExecution".into()])
    .field(FieldDef::new("sequence", FieldType::Int32));

EdgeSchema::new("EXECUTED_TOOL")
    .from(vec!["ToolExecution".into()])
    .to(vec!["Tool".into()]);

// ═══════════════════════════════════════════════════════════════════════════
// OBSERVABILITY
// ═══════════════════════════════════════════════════════════════════════════

NodeSchema::new("Trace")
    .field(FieldDef::new("id", FieldType::String).required().unique())
    .field(FieldDef::new("name", FieldType::String).required())
    .field(FieldDef::new("service", FieldType::String).indexed())
    .field(FieldDef::new("operation", FieldType::String).indexed())
    .field(FieldDef::new("status", FieldType::String).indexed())  // ok|error
    .field(FieldDef::new("started_at", FieldType::DateTime).indexed())
    .field(FieldDef::new("duration_ms", FieldType::Int32))
    .field(FieldDef::new("attributes", FieldType::Json))
    .field(FieldDef::new("events", FieldType::Json))  // [{name, timestamp, attributes}]
    .strict();

EdgeSchema::new("PARENT_SPAN")
    .from(vec!["Trace".into()])
    .to(vec!["Trace".into()]);

EdgeSchema::new("USER_TRACE")
    .from(vec!["User".into()])
    .to(vec!["Trace".into()]);

EdgeSchema::new("AGENT_TRACE")
    .from(vec!["AgentRun".into()])
    .to(vec!["Trace".into()]);

// ═══════════════════════════════════════════════════════════════════════════
// WEBHOOKS & EVENTS
// ═══════════════════════════════════════════════════════════════════════════

NodeSchema::new("Webhook")
    .field(FieldDef::new("id", FieldType::String).required().unique())
    .field(FieldDef::new("url", FieldType::String).required())
    .field(FieldDef::new("events", FieldType::StringArray))  // ["agent.run.completed", "tool.executed"]
    .field(FieldDef::new("secret", FieldType::String))  // For HMAC signing
    .field(FieldDef::new("enabled", FieldType::Bool).default(true))
    .field(FieldDef::new("failure_count", FieldType::Int32).default(0))
    .field(FieldDef::new("last_triggered", FieldType::DateTime))
    .field(FieldDef::new("created_at", FieldType::DateTime))
    .strict();

NodeSchema::new("WebhookDelivery")
    .field(FieldDef::new("id", FieldType::String).required().unique())
    .field(FieldDef::new("event_type", FieldType::String).indexed())
    .field(FieldDef::new("payload", FieldType::Json))
    .field(FieldDef::new("status", FieldType::String).indexed())  // pending|delivered|failed
    .field(FieldDef::new("status_code", FieldType::Int32))
    .field(FieldDef::new("response", FieldType::Text))
    .field(FieldDef::new("attempts", FieldType::Int32).default(0))
    .field(FieldDef::new("created_at", FieldType::DateTime).indexed())
    .field(FieldDef::new("delivered_at", FieldType::DateTime))
    .strict();

EdgeSchema::new("OWNS_WEBHOOK")
    .from(vec!["User".into()])
    .to(vec!["Webhook".into()]);

EdgeSchema::new("WEBHOOK_DELIVERY")
    .from(vec!["Webhook".into()])
    .to(vec!["WebhookDelivery".into()]);
```

---

## Part 2: Service APIs

### 2.1 Conversation Service

```yaml
openapi: 3.0.0
info:
  title: Conversation Service
  version: 1.0.0

paths:
  /conversations:
    post:
      summary: Create conversation
      requestBody:
        content:
          application/json:
            schema:
              type: object
              properties:
                title:
                  type: string
                system_prompt:
                  type: string
                model:
                  type: string
                  default: "claude-sonnet-4-20250514"
                metadata:
                  type: object
      responses:
        201:
          description: Created
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/Conversation'

    get:
      summary: List conversations
      parameters:
        - name: limit
          in: query
          schema:
            type: integer
            default: 20
        - name: offset
          in: query
          schema:
            type: integer
            default: 0
        - name: sort
          in: query
          schema:
            type: string
            enum: [created_at, updated_at, message_count]
      responses:
        200:
          description: Success
          content:
            application/json:
              schema:
                type: object
                properties:
                  conversations:
                    type: array
                    items:
                      $ref: '#/components/schemas/Conversation'
                  total:
                    type: integer
                  has_more:
                    type: boolean

  /conversations/search:
    post:
      summary: Semantic search conversations
      requestBody:
        content:
          application/json:
            schema:
              type: object
              required:
                - query
              properties:
                query:
                  type: string
                limit:
                  type: integer
                  default: 10
                threshold:
                  type: number
                  default: 0.7
      responses:
        200:
          description: Success
          content:
            application/json:
              schema:
                type: object
                properties:
                  results:
                    type: array
                    items:
                      type: object
                      properties:
                        conversation:
                          $ref: '#/components/schemas/Conversation'
                        score:
                          type: number

  /conversations/{conversation_id}:
    get:
      summary: Get conversation
      responses:
        200:
          description: Success
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/Conversation'

    patch:
      summary: Update conversation
      requestBody:
        content:
          application/json:
            schema:
              type: object
              properties:
                title:
                  type: string
                system_prompt:
                  type: string
                metadata:
                  type: object
      responses:
        200:
          description: Success

    delete:
      summary: Delete conversation
      responses:
        204:
          description: Deleted

  /conversations/{conversation_id}/messages:
    get:
      summary: Get messages
      parameters:
        - name: limit
          in: query
          schema:
            type: integer
            default: 50
        - name: before
          in: query
          schema:
            type: string
          description: Message ID to paginate before
        - name: after
          in: query
          schema:
            type: string
          description: Message ID to paginate after
      responses:
        200:
          description: Success
          content:
            application/json:
              schema:
                type: object
                properties:
                  messages:
                    type: array
                    items:
                      $ref: '#/components/schemas/Message'
                  has_more:
                    type: boolean

    post:
      summary: Add message
      requestBody:
        content:
          application/json:
            schema:
              type: object
              required:
                - role
                - content
              properties:
                role:
                  type: string
                  enum: [user, assistant, system, tool]
                content:
                  oneOf:
                    - type: string
                    - type: array
                      items:
                        $ref: '#/components/schemas/ContentBlock'
                tool_call_id:
                  type: string
                name:
                  type: string
      responses:
        201:
          description: Created
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/Message'

  /conversations/{conversation_id}/complete:
    post:
      summary: Generate completion (non-streaming)
      requestBody:
        content:
          application/json:
            schema:
              type: object
              properties:
                model:
                  type: string
                max_tokens:
                  type: integer
                temperature:
                  type: number
                tools:
                  type: array
                  items:
                    type: string
                  description: Tool IDs to make available
      responses:
        200:
          description: Success
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/Message'

  /conversations/{conversation_id}/stream:
    post:
      summary: Generate completion (streaming)
      requestBody:
        content:
          application/json:
            schema:
              type: object
              properties:
                model:
                  type: string
                max_tokens:
                  type: integer
                temperature:
                  type: number
                tools:
                  type: array
                  items:
                    type: string
      responses:
        200:
          description: Server-sent events stream
          content:
            text/event-stream:
              schema:
                type: string

  /conversations/{conversation_id}/branch:
    post:
      summary: Branch conversation from a message
      requestBody:
        content:
          application/json:
            schema:
              type: object
              required:
                - from_message_id
              properties:
                from_message_id:
                  type: string
                title:
                  type: string
      responses:
        201:
          description: Created
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/Conversation'

components:
  schemas:
    Conversation:
      type: object
      properties:
        id:
          type: string
        title:
          type: string
        system_prompt:
          type: string
        model:
          type: string
        message_count:
          type: integer
        total_tokens:
          type: integer
        created_at:
          type: string
          format: date-time
        updated_at:
          type: string
          format: date-time
        metadata:
          type: object

    Message:
      type: object
      properties:
        id:
          type: string
        role:
          type: string
          enum: [user, assistant, system, tool]
        content:
          type: array
          items:
            $ref: '#/components/schemas/ContentBlock'
        content_text:
          type: string
        tool_call_id:
          type: string
        name:
          type: string
        created_at:
          type: string
          format: date-time
        metrics:
          type: object
          properties:
            input_tokens:
              type: integer
            output_tokens:
              type: integer
            latency_ms:
              type: integer
            cost_usd:
              type: number

    ContentBlock:
      type: object
      properties:
        type:
          type: string
          enum: [text, code, image, tool_use, tool_result]
        text:
          type: string
        code:
          type: string
        language:
          type: string
        tool_use_id:
          type: string
        tool_name:
          type: string
        input:
          type: object
        output:
          type: object
        is_error:
          type: boolean
        media_type:
          type: string
        media_url:
          type: string
```

### 2.2 Agent Service

```yaml
openapi: 3.0.0
info:
  title: Agent Service
  version: 1.0.0

paths:
  /agents:
    post:
      summary: Create agent
      requestBody:
        content:
          application/json:
            schema:
              $ref: '#/components/schemas/AgentCreate'
      responses:
        201:
          description: Created
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/Agent'

    get:
      summary: List agents
      responses:
        200:
          description: Success
          content:
            application/json:
              schema:
                type: object
                properties:
                  agents:
                    type: array
                    items:
                      $ref: '#/components/schemas/Agent'

  /agents/search:
    post:
      summary: Semantic search agents
      requestBody:
        content:
          application/json:
            schema:
              type: object
              required:
                - query
              properties:
                query:
                  type: string
                limit:
                  type: integer
                  default: 10
      responses:
        200:
          description: Success

  /agents/{agent_id}:
    get:
      summary: Get agent
      responses:
        200:
          description: Success
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/Agent'

    patch:
      summary: Update agent
      requestBody:
        content:
          application/json:
            schema:
              $ref: '#/components/schemas/AgentUpdate'
      responses:
        200:
          description: Success

    delete:
      summary: Delete agent
      responses:
        204:
          description: Deleted

  /agents/{agent_id}/tools:
    get:
      summary: List agent's tools
      responses:
        200:
          description: Success

    post:
      summary: Add tool to agent
      requestBody:
        content:
          application/json:
            schema:
              type: object
              required:
                - tool_id
              properties:
                tool_id:
                  type: string
                priority:
                  type: integer
                  default: 0
      responses:
        201:
          description: Added

    delete:
      summary: Remove tool from agent
      requestBody:
        content:
          application/json:
            schema:
              type: object
              required:
                - tool_id
              properties:
                tool_id:
                  type: string
      responses:
        204:
          description: Removed

  /agents/{agent_id}/run:
    post:
      summary: Start agent run
      requestBody:
        content:
          application/json:
            schema:
              type: object
              properties:
                input:
                  type: object
                  description: Initial input/context for the agent
                conversation_id:
                  type: string
                  description: Existing conversation to continue
                max_turns:
                  type: integer
                  description: Override agent's max_turns
                stream:
                  type: boolean
                  default: false
      responses:
        200:
          description: Run started (streaming if requested)
        201:
          description: Run completed (non-streaming)
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/AgentRun'

  /agents/{agent_id}/runs:
    get:
      summary: List agent runs
      parameters:
        - name: status
          in: query
          schema:
            type: string
            enum: [pending, running, completed, failed, interrupted]
        - name: limit
          in: query
          schema:
            type: integer
            default: 20
      responses:
        200:
          description: Success
          content:
            application/json:
              schema:
                type: object
                properties:
                  runs:
                    type: array
                    items:
                      $ref: '#/components/schemas/AgentRun'

  /agents/{agent_id}/runs/{run_id}:
    get:
      summary: Get run status
      responses:
        200:
          description: Success
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/AgentRun'

  /agents/{agent_id}/runs/{run_id}/interrupt:
    post:
      summary: Interrupt running agent
      responses:
        200:
          description: Interrupted
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/AgentRun'

  /agents/{agent_id}/runs/{run_id}/resume:
    post:
      summary: Resume from checkpoint
      requestBody:
        content:
          application/json:
            schema:
              type: object
              properties:
                checkpoint_id:
                  type: string
                  description: Specific checkpoint to resume from (default: latest)
      responses:
        200:
          description: Resumed

components:
  schemas:
    AgentCreate:
      type: object
      required:
        - name
      properties:
        name:
          type: string
        description:
          type: string
        system_prompt:
          type: string
        model:
          type: string
          default: "claude-sonnet-4-20250514"
        temperature:
          type: number
          default: 0.7
        max_tokens:
          type: integer
          default: 4096
        max_turns:
          type: integer
          default: 50
        tool_ids:
          type: array
          items:
            type: string
        memory_config:
          $ref: '#/components/schemas/MemoryConfig'
        metadata:
          type: object

    AgentUpdate:
      type: object
      properties:
        name:
          type: string
        description:
          type: string
        system_prompt:
          type: string
        model:
          type: string
        temperature:
          type: number
        max_tokens:
          type: integer
        max_turns:
          type: integer
        memory_config:
          $ref: '#/components/schemas/MemoryConfig'
        metadata:
          type: object

    Agent:
      type: object
      properties:
        id:
          type: string
        name:
          type: string
        description:
          type: string
        system_prompt:
          type: string
        model:
          type: string
        temperature:
          type: number
        max_tokens:
          type: integer
        max_turns:
          type: integer
        memory_config:
          $ref: '#/components/schemas/MemoryConfig'
        tools:
          type: array
          items:
            type: object
            properties:
              tool_id:
                type: string
              name:
                type: string
              priority:
                type: integer
        created_at:
          type: string
          format: date-time
        updated_at:
          type: string
          format: date-time
        metadata:
          type: object

    MemoryConfig:
      type: object
      properties:
        working_memory_tokens:
          type: integer
          default: 8000
          description: Max tokens in working memory
        episodic_retrieval_k:
          type: integer
          default: 5
          description: Number of episodic memories to retrieve
        semantic_retrieval_k:
          type: integer
          default: 3
          description: Number of semantic memories to retrieve
        consolidation_threshold:
          type: number
          default: 0.8
          description: Importance threshold for auto-consolidation
        consolidation_mode:
          type: string
          enum: [auto, manual, off]
          default: auto

    AgentRun:
      type: object
      properties:
        id:
          type: string
        agent_id:
          type: string
        status:
          type: string
          enum: [pending, running, completed, failed, interrupted]
        trigger:
          type: string
          enum: [api, schedule, webhook, manual]
        input:
          type: object
        output:
          type: object
        error:
          type: string
        conversation_id:
          type: string
        started_at:
          type: string
          format: date-time
        completed_at:
          type: string
          format: date-time
        metrics:
          type: object
          properties:
            total_tokens:
              type: integer
            total_cost_usd:
              type: number
            tool_calls:
              type: integer
            turns:
              type: integer
            duration_ms:
              type: integer
        checkpoints:
          type: array
          items:
            type: object
            properties:
              id:
                type: string
              step:
                type: integer
              created_at:
                type: string
                format: date-time
```

### 2.3 Memory Service

```yaml
openapi: 3.0.0
info:
  title: Memory Service
  version: 1.0.0

paths:
  /memory/{agent_id}:
    get:
      summary: Get memory stats
      responses:
        200:
          description: Success
          content:
            application/json:
              schema:
                type: object
                properties:
                  agent_id:
                    type: string
                  working_memory_count:
                    type: integer
                  episodic_memory_count:
                    type: integer
                  semantic_memory_count:
                    type: integer
                  total_memories:
                    type: integer

  /memory/{agent_id}/store:
    post:
      summary: Store memory
      requestBody:
        content:
          application/json:
            schema:
              type: object
              required:
                - content
                - type
              properties:
                content:
                  type: string
                type:
                  type: string
                  enum: [working, episodic, semantic, procedural]
                importance:
                  type: number
                  default: 0.5
                source_type:
                  type: string
                  enum: [conversation, tool, observation, manual]
                source_id:
                  type: string
                metadata:
                  type: object
                expires_in_seconds:
                  type: integer
                  description: TTL for working memory
      responses:
        201:
          description: Stored
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/Memory'

  /memory/{agent_id}/retrieve:
    post:
      summary: Retrieve relevant memories
      requestBody:
        content:
          application/json:
            schema:
              type: object
              required:
                - query
              properties:
                query:
                  type: string
                types:
                  type: array
                  items:
                    type: string
                    enum: [working, episodic, semantic, procedural]
                  description: Filter by memory types
                limit:
                  type: integer
                  default: 10
                min_importance:
                  type: number
                  default: 0.0
                recency_weight:
                  type: number
                  default: 0.3
                  description: Weight for recency vs relevance (0-1)
      responses:
        200:
          description: Success
          content:
            application/json:
              schema:
                type: object
                properties:
                  memories:
                    type: array
                    items:
                      type: object
                      properties:
                        memory:
                          $ref: '#/components/schemas/Memory'
                        score:
                          type: number
                        recency_score:
                          type: number
                        combined_score:
                          type: number

  /memory/{agent_id}/consolidate:
    post:
      summary: Consolidate memories
      description: Move working memories to episodic, episodic to semantic
      requestBody:
        content:
          application/json:
            schema:
              type: object
              properties:
                source_type:
                  type: string
                  enum: [working, episodic]
                target_type:
                  type: string
                  enum: [episodic, semantic]
                strategy:
                  type: string
                  enum: [summarize, abstract, merge]
                  default: summarize
                memory_ids:
                  type: array
                  items:
                    type: string
                  description: Specific memories to consolidate (default: auto-select)
      responses:
        200:
          description: Consolidated
          content:
            application/json:
              schema:
                type: object
                properties:
                  consolidated_count:
                    type: integer
                  new_memories:
                    type: array
                    items:
                      $ref: '#/components/schemas/Memory'

  /memory/{agent_id}/forget:
    post:
      summary: Forget memories
      requestBody:
        content:
          application/json:
            schema:
              type: object
              properties:
                memory_ids:
                  type: array
                  items:
                    type: string
                older_than:
                  type: string
                  format: date-time
                importance_below:
                  type: number
                type:
                  type: string
                  enum: [working, episodic, semantic, procedural]
      responses:
        200:
          description: Forgotten
          content:
            application/json:
              schema:
                type: object
                properties:
                  deleted_count:
                    type: integer

  /memory/{agent_id}/working:
    get:
      summary: Get current working memory
      responses:
        200:
          description: Success
          content:
            application/json:
              schema:
                type: object
                properties:
                  memories:
                    type: array
                    items:
                      $ref: '#/components/schemas/Memory'
                  total_tokens:
                    type: integer

    delete:
      summary: Clear working memory
      responses:
        204:
          description: Cleared

components:
  schemas:
    Memory:
      type: object
      properties:
        id:
          type: string
        type:
          type: string
          enum: [working, episodic, semantic, procedural]
        content:
          type: string
        summary:
          type: string
        importance:
          type: number
        access_count:
          type: integer
        last_accessed:
          type: string
          format: date-time
        created_at:
          type: string
          format: date-time
        expires_at:
          type: string
          format: date-time
        source_type:
          type: string
        source_id:
          type: string
        metadata:
          type: object
```

---

## Part 3: Agent Runtime Implementation

### 3.1 Execution Loop (Python)

```python
# agent_runtime.py

import asyncio
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional, List, Dict, Any, AsyncIterator
from datetime import datetime
import uuid

class AgentStatus(Enum):
    IDLE = "idle"
    OBSERVING = "observing"
    THINKING = "thinking"
    ACTING = "acting"
    REMEMBERING = "remembering"
    COMPLETED = "completed"
    FAILED = "failed"
    INTERRUPTED = "interrupted"

@dataclass
class AgentContext:
    """Current execution context for the agent."""
    conversation_id: str
    messages: List[Dict[str, Any]] = field(default_factory=list)
    working_memory: List[Dict[str, Any]] = field(default_factory=list)
    retrieved_memories: List[Dict[str, Any]] = field(default_factory=list)
    available_tools: List[Dict[str, Any]] = field(default_factory=list)
    turn: int = 0
    total_tokens: int = 0
    total_cost: float = 0.0

@dataclass
class AgentConfig:
    """Agent configuration."""
    id: str
    name: str
    system_prompt: str
    model: str = "claude-sonnet-4-20250514"
    temperature: float = 0.7
    max_tokens: int = 4096
    max_turns: int = 50
    memory_config: Dict[str, Any] = field(default_factory=lambda: {
        "working_memory_tokens": 8000,
        "episodic_retrieval_k": 5,
        "semantic_retrieval_k": 3,
        "consolidation_threshold": 0.8,
        "consolidation_mode": "auto"
    })
    tool_ids: List[str] = field(default_factory=list)

class AgentRuntime:
    """
    Core agent execution runtime.

    Implements the OODA loop:
    - Observe: Gather context, retrieve memories
    - Orient: (implicit in prompt construction)
    - Decide: LLM call with tool definitions
    - Act: Execute tools or emit response

    Plus: Remember (update memories after each turn)
    """

    def __init__(
        self,
        config: AgentConfig,
        gateway_client,      # For LLM calls
        memory_client,       # For memory operations
        tool_client,         # For tool execution
        conversation_client, # For conversation storage
    ):
        self.config = config
        self.gateway = gateway_client
        self.memory = memory_client
        self.tools = tool_client
        self.conversations = conversation_client

        self.status = AgentStatus.IDLE
        self.context: Optional[AgentContext] = None
        self.run_id: Optional[str] = None
        self._interrupt_requested = False

    async def run(
        self,
        input_message: str,
        conversation_id: Optional[str] = None,
        stream: bool = False,
    ) -> AsyncIterator[Dict[str, Any]]:
        """
        Execute the agent loop.

        Yields events for streaming:
        - {"type": "status", "status": "observing"}
        - {"type": "thinking", "content": "..."}
        - {"type": "tool_call", "tool": "...", "input": {...}}
        - {"type": "tool_result", "tool": "...", "output": {...}}
        - {"type": "message", "content": "..."}
        - {"type": "memory", "action": "stored", "memory": {...}}
        - {"type": "done", "run": {...}}
        - {"type": "error", "error": "..."}
        """
        self.run_id = str(uuid.uuid4())
        self._interrupt_requested = False

        try:
            # Initialize context
            yield {"type": "status", "status": "initializing"}
            await self._initialize_context(input_message, conversation_id)

            # Main loop
            while self.context.turn < self.config.max_turns:
                if self._interrupt_requested:
                    yield {"type": "status", "status": "interrupted"}
                    self.status = AgentStatus.INTERRUPTED
                    break

                self.context.turn += 1

                # OBSERVE
                yield {"type": "status", "status": "observing"}
                self.status = AgentStatus.OBSERVING
                await self._observe()

                # THINK
                yield {"type": "status", "status": "thinking"}
                self.status = AgentStatus.THINKING
                response = await self._think(stream=stream)

                if stream:
                    async for chunk in response:
                        yield {"type": "thinking_chunk", "content": chunk}
                    response = await response.get_final()

                # ACT
                if response.tool_calls:
                    yield {"type": "status", "status": "acting"}
                    self.status = AgentStatus.ACTING

                    for tool_call in response.tool_calls:
                        yield {
                            "type": "tool_call",
                            "tool": tool_call.name,
                            "input": tool_call.input,
                            "id": tool_call.id
                        }

                        result = await self._execute_tool(tool_call)

                        yield {
                            "type": "tool_result",
                            "tool": tool_call.name,
                            "output": result.output,
                            "success": result.success,
                            "id": tool_call.id
                        }

                        # Add tool result to conversation
                        await self._add_tool_result(tool_call, result)
                else:
                    # Emit assistant message
                    yield {
                        "type": "message",
                        "role": "assistant",
                        "content": response.content
                    }

                    # Add to conversation
                    await self._add_message("assistant", response.content, response.metrics)

                # REMEMBER
                yield {"type": "status", "status": "remembering"}
                self.status = AgentStatus.REMEMBERING
                memories = await self._remember(response)

                for mem in memories:
                    yield {"type": "memory", "action": "stored", "memory": mem}

                # Check if done
                if response.finish_reason == "stop" and not response.tool_calls:
                    break

            # Finalize
            self.status = AgentStatus.COMPLETED
            run_summary = await self._finalize_run()
            yield {"type": "done", "run": run_summary}

        except Exception as e:
            self.status = AgentStatus.FAILED
            yield {"type": "error", "error": str(e)}
            raise

    def interrupt(self):
        """Request interruption of the current run."""
        self._interrupt_requested = True

    async def _initialize_context(self, input_message: str, conversation_id: Optional[str]):
        """Initialize or load conversation context."""
        if conversation_id:
            # Load existing conversation
            conv = await self.conversations.get(conversation_id)
            messages = await self.conversations.get_messages(conversation_id)
        else:
            # Create new conversation
            conv = await self.conversations.create(
                title=f"Agent Run: {self.config.name}",
                system_prompt=self.config.system_prompt,
                model=self.config.model
            )
            messages = []

        # Load tools
        tools = []
        for tool_id in self.config.tool_ids:
            tool = await self.tools.get(tool_id)
            tools.append(tool)

        self.context = AgentContext(
            conversation_id=conv.id,
            messages=messages,
            available_tools=tools,
        )

        # Add user message
        await self._add_message("user", input_message)

    async def _observe(self):
        """Gather context: retrieve relevant memories."""
        # Get recent context for memory retrieval
        recent_context = self._get_recent_context()

        # Retrieve episodic memories
        episodic = await self.memory.retrieve(
            agent_id=self.config.id,
            query=recent_context,
            types=["episodic"],
            limit=self.config.memory_config["episodic_retrieval_k"]
        )

        # Retrieve semantic memories
        semantic = await self.memory.retrieve(
            agent_id=self.config.id,
            query=recent_context,
            types=["semantic"],
            limit=self.config.memory_config["semantic_retrieval_k"]
        )

        # Get working memory
        working = await self.memory.get_working(self.config.id)

        self.context.working_memory = working
        self.context.retrieved_memories = episodic + semantic

    async def _think(self, stream: bool = False):
        """Make LLM call with full context."""
        # Build messages with system prompt + memories + conversation
        messages = self._build_messages()

        # Build tool definitions
        tools = self._build_tool_definitions()

        # Call LLM
        response = await self.gateway.complete(
            model=self.config.model,
            messages=messages,
            tools=tools,
            max_tokens=self.config.max_tokens,
            temperature=self.config.temperature,
            stream=stream
        )

        # Track metrics
        self.context.total_tokens += response.metrics.total_tokens
        self.context.total_cost += response.metrics.cost_usd

        return response

    def _build_messages(self) -> List[Dict[str, Any]]:
        """Build message list with system prompt and memories."""
        messages = []

        # System prompt with injected memories
        system_content = self.config.system_prompt

        if self.context.working_memory:
            system_content += "\n\n## Working Memory\n"
            for mem in self.context.working_memory:
                system_content += f"- {mem['content']}\n"

        if self.context.retrieved_memories:
            system_content += "\n\n## Relevant Memories\n"
            for mem in self.context.retrieved_memories:
                system_content += f"- [{mem['type']}] {mem['content']}\n"

        messages.append({"role": "system", "content": system_content})

        # Add conversation messages
        messages.extend(self.context.messages)

        return messages

    def _build_tool_definitions(self) -> List[Dict[str, Any]]:
        """Build tool definitions for LLM."""
        return [
            {
                "type": "function",
                "function": {
                    "name": tool["name"],
                    "description": tool["description"],
                    "parameters": tool["input_schema"]
                }
            }
            for tool in self.context.available_tools
        ]

    async def _execute_tool(self, tool_call) -> Dict[str, Any]:
        """Execute a tool call."""
        tool = next(
            (t for t in self.context.available_tools if t["name"] == tool_call.name),
            None
        )

        if not tool:
            return {
                "success": False,
                "output": {"error": f"Tool '{tool_call.name}' not found"}
            }

        result = await self.tools.execute(
            tool_id=tool["id"],
            input_data=tool_call.input
        )

        return result

    async def _add_message(self, role: str, content: Any, metrics: Optional[Dict] = None):
        """Add message to conversation."""
        message = await self.conversations.add_message(
            conversation_id=self.context.conversation_id,
            role=role,
            content=content,
            metrics=metrics
        )
        self.context.messages.append(message)

    async def _add_tool_result(self, tool_call, result):
        """Add tool result to conversation."""
        await self._add_message(
            role="tool",
            content=result.output,
            tool_call_id=tool_call.id,
            name=tool_call.name
        )

    async def _remember(self, response) -> List[Dict[str, Any]]:
        """Update memories based on the turn."""
        memories = []

        # Store significant content as working memory
        if response.content and len(response.content) > 100:
            mem = await self.memory.store(
                agent_id=self.config.id,
                content=response.content[:500],  # Truncate for working memory
                type="working",
                source_type="conversation",
                source_id=self.context.conversation_id,
                expires_in_seconds=3600  # 1 hour TTL
            )
            memories.append(mem)

        # Store tool results as episodic memories
        if response.tool_calls:
            for tool_call in response.tool_calls:
                mem = await self.memory.store(
                    agent_id=self.config.id,
                    content=f"Used tool '{tool_call.name}' with input {tool_call.input}",
                    type="episodic",
                    source_type="tool",
                    importance=0.6,
                    metadata={"tool_name": tool_call.name}
                )
                memories.append(mem)

        # Auto-consolidate if configured
        if self.config.memory_config["consolidation_mode"] == "auto":
            if self.context.turn % 5 == 0:  # Every 5 turns
                consolidated = await self.memory.consolidate(
                    agent_id=self.config.id,
                    source_type="working",
                    target_type="episodic",
                    strategy="summarize"
                )
                memories.extend(consolidated)

        return memories

    def _get_recent_context(self) -> str:
        """Get recent conversation context for memory retrieval."""
        recent_messages = self.context.messages[-5:]  # Last 5 messages
        return "\n".join(
            f"{m['role']}: {m.get('content_text', str(m.get('content', '')))[:200]}"
            for m in recent_messages
        )

    async def _finalize_run(self) -> Dict[str, Any]:
        """Finalize the run and return summary."""
        return {
            "id": self.run_id,
            "agent_id": self.config.id,
            "status": self.status.value,
            "conversation_id": self.context.conversation_id,
            "turns": self.context.turn,
            "total_tokens": self.context.total_tokens,
            "total_cost_usd": self.context.total_cost,
        }
```

### 3.2 Memory Manager

```python
# memory_manager.py

from dataclasses import dataclass
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta
import math

@dataclass
class MemoryRetrievalConfig:
    """Configuration for memory retrieval."""
    recency_weight: float = 0.3  # Weight for recency vs relevance
    importance_weight: float = 0.2  # Weight for importance score
    decay_rate: float = 0.1  # Exponential decay rate for recency

class MemoryManager:
    """
    Manages agent memory with multiple memory types and intelligent retrieval.

    Memory Types:
    - Working: Short-term, high-access, expires quickly
    - Episodic: Event-based, time-stamped experiences
    - Semantic: Abstract knowledge, facts, concepts
    - Procedural: Learned skills, patterns, procedures
    """

    def __init__(self, graphsub_client, embedding_client):
        self.graph = graphsub_client
        self.embedder = embedding_client

    async def store(
        self,
        agent_id: str,
        content: str,
        type: str,
        importance: float = 0.5,
        source_type: Optional[str] = None,
        source_id: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        expires_in_seconds: Optional[int] = None,
    ) -> Dict[str, Any]:
        """Store a new memory."""
        # Compute embedding
        embedding = await self.embedder.embed(content)

        # Calculate expiration
        expires_at = None
        if expires_in_seconds:
            expires_at = datetime.utcnow() + timedelta(seconds=expires_in_seconds)

        # Create memory node
        memory = await self.graph.create_node("Memory", {
            "id": str(uuid.uuid4()),
            "type": type,
            "content": content,
            "importance": importance,
            "access_count": 0,
            "created_at": datetime.utcnow(),
            "expires_at": expires_at,
            "source_type": source_type,
            "source_id": source_id,
            "metadata": metadata or {},
            "embedding": embedding
        })

        # Create edge to agent
        await self.graph.create_edge(
            "AGENT_MEMORY",
            from_node=("Agent", agent_id),
            to_node=("Memory", memory["id"]),
            properties={"memory_type": type}
        )

        return memory

    async def retrieve(
        self,
        agent_id: str,
        query: str,
        types: Optional[List[str]] = None,
        limit: int = 10,
        min_importance: float = 0.0,
        config: Optional[MemoryRetrievalConfig] = None,
    ) -> List[Dict[str, Any]]:
        """
        Retrieve relevant memories using hybrid scoring.

        Score = relevance * (1 - recency_weight - importance_weight)
              + recency * recency_weight
              + importance * importance_weight
        """
        config = config or MemoryRetrievalConfig()

        # Compute query embedding
        query_embedding = await self.embedder.embed(query)

        # Build type filter
        type_filter = ""
        if types:
            type_list = ", ".join(f'"{t}"' for t in types)
            type_filter = f"AND m.type IN [{type_list}]"

        # Vector search with filters
        results = await self.graph.query(f"""
            CALL db.index.vector.queryNodes('memory_embedding', $k, $queryEmbedding)
            YIELD node AS m, score AS relevance
            WHERE (m)<-[:AGENT_MEMORY]-(:Agent {{id: $agent_id}})
            AND m.importance >= $min_importance
            AND (m.expires_at IS NULL OR m.expires_at > datetime())
            {type_filter}
            RETURN m, relevance
            ORDER BY relevance DESC
            LIMIT $limit
        """, {
            "k": limit * 2,  # Fetch more for filtering
            "queryEmbedding": query_embedding,
            "agent_id": agent_id,
            "min_importance": min_importance,
            "limit": limit
        })

        # Compute hybrid scores
        now = datetime.utcnow()
        scored_memories = []

        for record in results:
            memory = record["m"]
            relevance = record["relevance"]

            # Recency score (exponential decay)
            age_hours = (now - memory["created_at"]).total_seconds() / 3600
            recency = math.exp(-config.decay_rate * age_hours)

            # Importance score
            importance = memory["importance"]

            # Combined score
            combined = (
                relevance * (1 - config.recency_weight - config.importance_weight)
                + recency * config.recency_weight
                + importance * config.importance_weight
            )

            scored_memories.append({
                "memory": memory,
                "relevance_score": relevance,
                "recency_score": recency,
                "combined_score": combined
            })

        # Sort by combined score and limit
        scored_memories.sort(key=lambda x: x["combined_score"], reverse=True)
        scored_memories = scored_memories[:limit]

        # Update access counts
        for item in scored_memories:
            await self._increment_access(item["memory"]["id"])

        return scored_memories

    async def consolidate(
        self,
        agent_id: str,
        source_type: str,
        target_type: str,
        strategy: str = "summarize",
        memory_ids: Optional[List[str]] = None,
    ) -> List[Dict[str, Any]]:
        """
        Consolidate memories from source to target type.

        Strategies:
        - summarize: Create summary of multiple memories
        - abstract: Extract abstract concepts/patterns
        - merge: Merge similar memories
        """
        # Get source memories
        if memory_ids:
            memories = await self._get_memories_by_ids(memory_ids)
        else:
            memories = await self._get_memories_for_consolidation(
                agent_id, source_type
            )

        if not memories:
            return []

        new_memories = []

        if strategy == "summarize":
            # Summarize using LLM
            content = "\n".join(m["content"] for m in memories)
            summary = await self._summarize(content)

            mem = await self.store(
                agent_id=agent_id,
                content=summary,
                type=target_type,
                importance=max(m["importance"] for m in memories),
                source_type="consolidation",
                metadata={"strategy": "summarize", "source_count": len(memories)}
            )
            new_memories.append(mem)

            # Create consolidation edges
            for source_mem in memories:
                await self.graph.create_edge(
                    "CONSOLIDATED_FROM",
                    from_node=("Memory", mem["id"]),
                    to_node=("Memory", source_mem["id"]),
                    properties={"consolidation_type": "summarization"}
                )

        elif strategy == "abstract":
            # Extract abstract patterns
            patterns = await self._extract_patterns(memories)

            for pattern in patterns:
                mem = await self.store(
                    agent_id=agent_id,
                    content=pattern,
                    type=target_type,
                    importance=0.7,
                    source_type="consolidation",
                    metadata={"strategy": "abstract"}
                )
                new_memories.append(mem)

        elif strategy == "merge":
            # Cluster similar memories
            clusters = await self._cluster_memories(memories)

            for cluster in clusters:
                if len(cluster) > 1:
                    merged = await self._merge_cluster(cluster)
                    mem = await self.store(
                        agent_id=agent_id,
                        content=merged,
                        type=target_type,
                        importance=max(m["importance"] for m in cluster),
                        source_type="consolidation",
                        metadata={"strategy": "merge", "merged_count": len(cluster)}
                    )
                    new_memories.append(mem)

        # Optionally delete source memories (working memory)
        if source_type == "working":
            for mem in memories:
                await self.forget(agent_id, memory_ids=[mem["id"]])

        return new_memories

    async def forget(
        self,
        agent_id: str,
        memory_ids: Optional[List[str]] = None,
        older_than: Optional[datetime] = None,
        importance_below: Optional[float] = None,
        type: Optional[str] = None,
    ) -> int:
        """Delete memories matching criteria."""
        conditions = ["(m)<-[:AGENT_MEMORY]-(:Agent {id: $agent_id})"]
        params = {"agent_id": agent_id}

        if memory_ids:
            conditions.append("m.id IN $memory_ids")
            params["memory_ids"] = memory_ids

        if older_than:
            conditions.append("m.created_at < $older_than")
            params["older_than"] = older_than

        if importance_below:
            conditions.append("m.importance < $importance_below")
            params["importance_below"] = importance_below

        if type:
            conditions.append("m.type = $type")
            params["type"] = type

        where_clause = " AND ".join(conditions)

        result = await self.graph.query(f"""
            MATCH (m:Memory)
            WHERE {where_clause}
            DETACH DELETE m
            RETURN count(m) AS deleted
        """, params)

        return result[0]["deleted"]

    async def get_working(self, agent_id: str) -> List[Dict[str, Any]]:
        """Get all working memories for an agent."""
        result = await self.graph.query("""
            MATCH (a:Agent {id: $agent_id})-[:AGENT_MEMORY]->(m:Memory)
            WHERE m.type = 'working'
            AND (m.expires_at IS NULL OR m.expires_at > datetime())
            RETURN m
            ORDER BY m.created_at DESC
        """, {"agent_id": agent_id})

        return [r["m"] for r in result]

    async def _increment_access(self, memory_id: str):
        """Increment access count and update last_accessed."""
        await self.graph.query("""
            MATCH (m:Memory {id: $memory_id})
            SET m.access_count = m.access_count + 1,
                m.last_accessed = datetime()
        """, {"memory_id": memory_id})

    async def _summarize(self, content: str) -> str:
        """Use LLM to summarize content."""
        # Implementation would call the gateway
        pass

    async def _extract_patterns(self, memories: List[Dict]) -> List[str]:
        """Use LLM to extract abstract patterns."""
        pass

    async def _cluster_memories(self, memories: List[Dict]) -> List[List[Dict]]:
        """Cluster memories by embedding similarity."""
        pass

    async def _merge_cluster(self, cluster: List[Dict]) -> str:
        """Merge a cluster of similar memories."""
        pass
```

---

## Part 4: Python SDK

```python
# distributed/__init__.py

from .client import Client
from .agents import Agent, AgentRun
from .conversations import Conversation, Message
from .tools import Tool
from .memory import Memory

__version__ = "0.1.0"
__all__ = ["Client", "Agent", "AgentRun", "Conversation", "Message", "Tool", "Memory"]
```

```python
# distributed/client.py

from typing import Optional
import httpx
from .agents import AgentsResource
from .conversations import ConversationsResource
from .tools import ToolsResource
from .memory import MemoryResource

class Client:
    """
    Distributed Systems API Client.

    Usage:
        client = Client(api_key="dsk_...")

        # Create an agent
        agent = client.agents.create(
            name="Research Assistant",
            system_prompt="You are a helpful research assistant.",
            tools=["web_search", "calculator"]
        )

        # Run the agent
        run = agent.run("Find the latest AI research papers")

        # Or stream the execution
        for event in agent.run("Find papers", stream=True):
            print(event)
    """

    def __init__(
        self,
        api_key: str,
        base_url: str = "https://api.distributed.systems",
        timeout: float = 30.0,
    ):
        self.api_key = api_key
        self.base_url = base_url.rstrip("/")
        self._client = httpx.AsyncClient(
            base_url=self.base_url,
            headers={
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json",
            },
            timeout=timeout,
        )

        # Resources
        self.agents = AgentsResource(self)
        self.conversations = ConversationsResource(self)
        self.tools = ToolsResource(self)
        self.memory = MemoryResource(self)

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        await self._client.aclose()

    async def _request(
        self,
        method: str,
        path: str,
        **kwargs
    ) -> dict:
        response = await self._client.request(method, path, **kwargs)
        response.raise_for_status()
        return response.json()

    async def _stream(
        self,
        method: str,
        path: str,
        **kwargs
    ):
        async with self._client.stream(method, path, **kwargs) as response:
            response.raise_for_status()
            async for line in response.aiter_lines():
                if line.startswith("data: "):
                    yield json.loads(line[6:])
```

```python
# distributed/agents.py

from dataclasses import dataclass
from typing import Optional, List, Dict, Any, AsyncIterator
import json

@dataclass
class Agent:
    id: str
    name: str
    description: Optional[str]
    system_prompt: str
    model: str
    temperature: float
    max_tokens: int
    max_turns: int
    tools: List[Dict[str, Any]]
    memory_config: Dict[str, Any]
    created_at: str
    updated_at: str

    _client: Any = None

    async def run(
        self,
        input: str,
        conversation_id: Optional[str] = None,
        stream: bool = False,
        max_turns: Optional[int] = None,
    ) -> "AgentRun | AsyncIterator[Dict[str, Any]]":
        """
        Execute the agent.

        Args:
            input: Initial input/prompt for the agent
            conversation_id: Continue an existing conversation
            stream: If True, yields events as they occur
            max_turns: Override agent's max_turns

        Returns:
            AgentRun if stream=False, async iterator of events if stream=True
        """
        payload = {
            "input": {"message": input},
            "stream": stream,
        }
        if conversation_id:
            payload["conversation_id"] = conversation_id
        if max_turns:
            payload["max_turns"] = max_turns

        if stream:
            return self._stream_run(payload)
        else:
            response = await self._client._request(
                "POST",
                f"/agents/{self.id}/run",
                json=payload
            )
            return AgentRun(**response, _client=self._client)

    async def _stream_run(self, payload: dict) -> AsyncIterator[Dict[str, Any]]:
        async for event in self._client._stream(
            "POST",
            f"/agents/{self.id}/run",
            json=payload
        ):
            yield event

    async def add_tool(self, tool_id: str, priority: int = 0):
        """Add a tool to the agent."""
        await self._client._request(
            "POST",
            f"/agents/{self.id}/tools",
            json={"tool_id": tool_id, "priority": priority}
        )

    async def remove_tool(self, tool_id: str):
        """Remove a tool from the agent."""
        await self._client._request(
            "DELETE",
            f"/agents/{self.id}/tools",
            json={"tool_id": tool_id}
        )

    async def list_runs(
        self,
        status: Optional[str] = None,
        limit: int = 20
    ) -> List["AgentRun"]:
        """List agent runs."""
        params = {"limit": limit}
        if status:
            params["status"] = status

        response = await self._client._request(
            "GET",
            f"/agents/{self.id}/runs",
            params=params
        )
        return [AgentRun(**r, _client=self._client) for r in response["runs"]]


@dataclass
class AgentRun:
    id: str
    agent_id: str
    status: str
    trigger: str
    input: Dict[str, Any]
    output: Optional[Dict[str, Any]]
    error: Optional[str]
    conversation_id: str
    started_at: str
    completed_at: Optional[str]
    metrics: Dict[str, Any]

    _client: Any = None

    async def interrupt(self) -> "AgentRun":
        """Interrupt a running agent."""
        response = await self._client._request(
            "POST",
            f"/agents/{self.agent_id}/runs/{self.id}/interrupt"
        )
        return AgentRun(**response, _client=self._client)

    async def resume(self, checkpoint_id: Optional[str] = None) -> "AgentRun":
        """Resume from a checkpoint."""
        payload = {}
        if checkpoint_id:
            payload["checkpoint_id"] = checkpoint_id

        response = await self._client._request(
            "POST",
            f"/agents/{self.agent_id}/runs/{self.id}/resume",
            json=payload
        )
        return AgentRun(**response, _client=self._client)

    async def get_conversation(self) -> "Conversation":
        """Get the conversation for this run."""
        response = await self._client._request(
            "GET",
            f"/conversations/{self.conversation_id}"
        )
        from .conversations import Conversation
        return Conversation(**response, _client=self._client)


class AgentsResource:
    def __init__(self, client):
        self._client = client

    async def create(
        self,
        name: str,
        system_prompt: str,
        description: Optional[str] = None,
        model: str = "claude-sonnet-4-20250514",
        temperature: float = 0.7,
        max_tokens: int = 4096,
        max_turns: int = 50,
        tools: Optional[List[str]] = None,
        memory_config: Optional[Dict[str, Any]] = None,
    ) -> Agent:
        """Create a new agent."""
        payload = {
            "name": name,
            "system_prompt": system_prompt,
            "model": model,
            "temperature": temperature,
            "max_tokens": max_tokens,
            "max_turns": max_turns,
        }
        if description:
            payload["description"] = description
        if tools:
            payload["tool_ids"] = tools
        if memory_config:
            payload["memory_config"] = memory_config

        response = await self._client._request("POST", "/agents", json=payload)
        return Agent(**response, _client=self._client)

    async def get(self, agent_id: str) -> Agent:
        """Get an agent by ID."""
        response = await self._client._request("GET", f"/agents/{agent_id}")
        return Agent(**response, _client=self._client)

    async def list(self, limit: int = 20, offset: int = 0) -> List[Agent]:
        """List all agents."""
        response = await self._client._request(
            "GET",
            "/agents",
            params={"limit": limit, "offset": offset}
        )
        return [Agent(**a, _client=self._client) for a in response["agents"]]

    async def search(self, query: str, limit: int = 10) -> List[Agent]:
        """Semantic search for agents."""
        response = await self._client._request(
            "POST",
            "/agents/search",
            json={"query": query, "limit": limit}
        )
        return [Agent(**r["agent"], _client=self._client) for r in response["results"]]

    async def delete(self, agent_id: str):
        """Delete an agent."""
        await self._client._request("DELETE", f"/agents/{agent_id}")
```

---

## Part 5: Implementation Checklist

### Phase 1: Data Layer
- [ ] Implement GraphSub schemas (Part 1)
- [ ] Create migration scripts
- [ ] Set up vector indices
- [ ] Write schema validation tests

### Phase 2: Core Services
- [ ] Implement Conversation Service
  - [ ] CRUD operations
  - [ ] Message streaming
  - [ ] Vector search
  - [ ] Branching
- [ ] Implement Memory Service
  - [ ] Store/retrieve
  - [ ] Consolidation
  - [ ] Forgetting
  - [ ] Working memory TTL

### Phase 3: Agent Runtime
- [ ] Implement AgentRuntime class
- [ ] Implement MemoryManager class
- [ ] Implement checkpoint/resume
- [ ] Write execution tests

### Phase 4: API Layer
- [ ] Implement FastAPI routes
- [ ] Add authentication middleware
- [ ] Add rate limiting
- [ ] Add request tracing
- [ ] Deploy to Modal

### Phase 5: SDK
- [ ] Implement Python SDK
- [ ] Write SDK tests
- [ ] Publish to PyPI
- [ ] Write documentation

### Phase 6: Observability
- [ ] Implement tracing
- [ ] Add metrics collection
- [ ] Build dashboard
- [ ] Set up alerts

---

## Appendix: Event Types for Webhooks

```yaml
events:
  # Agent events
  agent.created:
    payload: { agent_id, name, created_at }
  agent.updated:
    payload: { agent_id, changes }
  agent.deleted:
    payload: { agent_id }
  agent.run.started:
    payload: { run_id, agent_id, trigger, input }
  agent.run.completed:
    payload: { run_id, agent_id, output, metrics }
  agent.run.failed:
    payload: { run_id, agent_id, error }
  agent.run.interrupted:
    payload: { run_id, agent_id, checkpoint_id }

  # Conversation events
  conversation.created:
    payload: { conversation_id, title }
  conversation.message.added:
    payload: { conversation_id, message_id, role }

  # Tool events
  tool.created:
    payload: { tool_id, name }
  tool.executed:
    payload: { execution_id, tool_id, input, output, latency_ms }
  tool.failed:
    payload: { execution_id, tool_id, error }

  # Memory events
  memory.stored:
    payload: { memory_id, agent_id, type }
  memory.consolidated:
    payload: { agent_id, source_type, target_type, count }

  # Billing events
  billing.threshold_reached:
    payload: { user_id, threshold, current_usage }
  billing.credits_low:
    payload: { user_id, remaining_credits }
```
