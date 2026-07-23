# Matching Engine - AI Labor Marketplace

A massive-scale autonomous matching system that intelligently routes jobs to 2300 specialized AI domain agents. Built for billions of concurrent task instances with computer-use capabilities.

## 🎯 Overview

The Matching Engine is a hybrid AI/human labor marketplace that:

- **Automatically matches jobs** to the most qualified workers from 2300 specialized domain agents
- **Optimizes for multiple dimensions**: capability match, domain expertise, cost, quality, and availability
- **Supports billions of instances** through distributed agent architecture
- **Enables computer use** for applicable tasks (coding, data analysis, automation)
- **Routes to multiple backends**: Parallel.ai for research, Anthropic Claude for reasoning, custom MCP servers

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                  Client Applications                         │
│         (Web, Mobile, API, MCP Clients)                     │
└─────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────┐
│                    Job Submission API                        │
│  POST /v1/matching/jobs - Submit new job                    │
│  GET  /v1/matching/jobs/{id} - Get status                   │
│  GET  /v1/matching/jobs/{id}/result - Get result            │
│  GET  /v1/matching/jobs/{id}/events - Stream updates (SSE)  │
└─────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────┐
│                   Matching Engine Core                       │
│  • Multi-dimensional scoring algorithm                       │
│  • Load balancing across agent instances                    │
│  • Cost/quality optimization                                 │
│  • Real-time availability tracking                           │
└─────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────┐
│                 Worker Registry (2300 Agents)                │
│                                                              │
│  Finance (250)    │  Medicine (200)   │  Law (200)          │
│  Technology (300) │  Science (250)    │  Engineering (200)  │
│  Business (150)   │  Marketing (150)  │  Data Science (150) │
│  Creative (120)   │  Education (100)  │  Social Sciences (80)│
│  Consulting (100) │  Government (80)  │  Agriculture (50)   │
│  Real Estate (70) │  ...and more                            │
└─────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────┐
│                   Execution Backends                         │
│                                                              │
│  Parallel.ai       │  Anthropic Claude  │  Custom MCP       │
│  (Research)        │  (Reasoning)       │  (Specialized)    │
│  • Deep research   │  • Computer use    │  • Domain tools   │
│  • Web search      │  • Code execution  │  • Custom APIs    │
│  • Data gathering  │  • Multi-modal     │  • Integrations   │
└─────────────────────────────────────────────────────────────┘
```

## 🚀 Quick Start

### 1. Start the Server

```bash
# Make sure you have a .env file with API_KEY set
python -m parallel_websearch_server.main
```

The server will start at `http://127.0.0.1:2040` and automatically initialize the 2300 domain agents.

### 2. Submit a Job

```bash
curl -X POST http://127.0.0.1:2040/v1/matching/jobs \
  -H "Content-Type: application/json" \
  -d '{
    "description": "Analyze Q4 earnings for Tesla and provide investment insights",
    "job_type": "research",
    "priority": "high",
    "requirements": {
      "min_quality_score": 0.85,
      "max_cost_usd": 2.0,
      "preferred_domains": ["finance"]
    }
  }'
```

Response:
```json
{
  "job_id": "job_a3f92bc14e67",
  "status": "matched",
  "created_at": "2025-01-08T10:30:00Z",
  "estimated_cost_usd": 1.20,
  "estimated_duration_seconds": 45,
  "matched_workers": ["agent_finance_investment_analysis_003"]
}
```

### 3. Check Status

```bash
curl http://127.0.0.1:2040/v1/matching/jobs/job_a3f92bc14e67
```

### 4. Get Result

```bash
curl http://127.0.0.1:2040/v1/matching/jobs/job_a3f92bc14e67/result?wait=true
```

## 📬 Agent Mailbox Notifications

Background research can run for minutes. Instead of keeping an SSE stream open, give the job a `notification_channel`
and let the agent poll or long-poll its mailbox.

```bash
curl -X POST http://127.0.0.1:2040/v1/matching/jobs \
  -H "Content-Type: application/json" \
  -d '{
    "description": "Run the Parallel web search processor for all Apple Vision Pro reviews",
    "job_type": "research",
    "notification_channel": "agent:vision-pro",
    "metadata": {"agent_session_id": "session-123"}
  }'
```

When the job finishes (or fails) the server writes an event to `/v1/matching/mailbox/<channel>`:

```bash
curl "http://127.0.0.1:2040/v1/matching/mailbox/agent:vision-pro?max_messages=5"
```

Response:

```jsonc
{
  "channel_id": "agent:vision-pro",
  "pending_messages": 0,
  "messages": [
    {
      "message_id": "msg_63a8d2147db0",
      "event_type": "job.completed",
      "job_id": "job_8efd0ba32cf1",
      "payload": {
        "status": "completed",
        "worker_id": "agent_finance_investment_analysis_003",
        "result": { "...": "..." }
      },
      "created_at": "2025-01-10T18:22:08.121Z"
    }
  ]
}
```

Confirm receipt (and delete the messages) with:

```bash
curl -X POST http://127.0.0.1:2040/v1/matching/mailbox/agent:vision-pro/ack \
  -H "Content-Type: application/json" \
  -d '{"message_ids": ["msg_63a8d2147db0"]}'
```

Mailbox endpoints sit alongside SSE streaming, so agents can choose either push or pull style notifications.

## 📊 The 2300 Domain Agents

### Agent Distribution by Domain

| Domain | Count | Specializations (Examples) |
|--------|-------|---------------------------|
| **Technology** | 300 | Software Engineering, Cybersecurity, Cloud Architecture, ML/AI, DevOps, Blockchain |
| **Finance** | 250 | Investment Analysis, Trading, Risk Assessment, Portfolio Management, Crypto, Derivatives |
| **Science** | 250 | Physics, Chemistry, Biology, Astronomy, Environmental Science, Materials Science |
| **Law** | 200 | Contract Law, Corporate Law, IP, Litigation, Tax, Criminal, Privacy |
| **Medicine** | 200 | Cardiology, Neurology, Oncology, Radiology, Surgery, Pharmacology, Genetics |
| **Engineering** | 200 | Mechanical, Electrical, Civil, Aerospace, Chemical, Biomedical, Robotics |
| **Business** | 150 | Strategy, Operations, HR, Supply Chain, Product Management, Sales |
| **Marketing** | 150 | Digital Marketing, Content, SEO, Social Media, Brand Strategy, Analytics |
| **Data Science** | 150 | Data Analysis, ML, Statistical Modeling, Visualization, NLP, Computer Vision |
| **Creative** | 120 | Writing, Design, Video, Music, Animation, Art Direction |
| **Education** | 100 | Curriculum Design, EdTech, Assessment, Tutoring, Learning Analytics |
| **Consulting** | 100 | Management, Strategy, Technology, Financial, Operations, Digital Transformation |
| **Social Sciences** | 80 | Psychology, Sociology, Economics, Political Science, Behavioral Economics |
| **Government** | 80 | Policy Analysis, Regulatory Affairs, International Relations, Intelligence |
| **Real Estate** | 70 | Commercial, Residential, Valuation, Investment, Development |
| **Agriculture** | 50 | Precision Ag, Crop Science, Sustainable Farming, Agricultural Tech |

### Agent Capabilities

Each agent has:
- **Primary domain expertise** (e.g., "finance/quantitative_finance")
- **Capability profile** with proficiency scores (0-1):
  - Research & analysis
  - Writing & communication
  - Code execution (for technical domains)
  - Computer use (for automation tasks)
  - Domain-specific capabilities
- **Performance metrics**:
  - Average quality score
  - Success rate
  - Response time
  - Total jobs completed
- **Cost model** ($0.05 - $5.00 per task based on complexity)
- **Capacity limits** (50-200 concurrent tasks per agent)

## 🎯 Matching Algorithm

The matching engine uses a **multi-dimensional weighted scoring** system:

### Scoring Dimensions

1. **Capability Match (30%)**: Does the agent have required skills?
   - Exact capability matches
   - Proficiency levels
   - Coverage of requirements

2. **Domain Match (25%)**: Does the agent specialize in this area?
   - Primary domain alignment
   - Related domain experience
   - Specialization depth

3. **Availability (20%)**: Can the agent take on more work?
   - Current load vs. capacity
   - Response time
   - Queue length

4. **Cost (15%)**: Is it within budget?
   - Cost vs. budget constraint
   - Value for money
   - Historical ROI

5. **Quality (10%)**: Track record of excellence?
   - Average quality score
   - Success rate
   - Client feedback

### Example Match

For a job requesting:
- "Analyze cryptocurrency market trends"
- Domain: finance
- Capabilities: research, data_analysis
- Max cost: $1.50

The engine would:
1. Filter agents with finance domain + required capabilities
2. Score each candidate on all dimensions
3. Select highest overall score
4. Verify availability and increment load
5. Route to appropriate backend (likely Parallel.ai for research)

## 🔧 Job Types

| Type | Description | Typical Agents |
|------|-------------|---------------|
| `research` | Deep research, data gathering, analysis | Any domain with research capability |
| `data_labeling` | Classification, annotation, tagging | Data science, specific domain experts |
| `content_creation` | Writing, editing, creative work | Creative, marketing, communication experts |
| `analysis` | Data analysis, insights, recommendations | Data science, finance, science, business |
| `code_generation` | Software development, scripting | Technology, data science, engineering |
| `computer_use` | Automation, browser tasks, tool use | Technology agents with computer-use capability |
| `general` | Any task - engine auto-classifies | Best match across all agents |

## 📡 API Reference

### Submit Job

```http
POST /v1/matching/jobs
```

**Request body:**
```json
{
  "description": "string",
  "job_type": "research | data_labeling | content_creation | analysis | code_generation | computer_use | general",
  "priority": "low | normal | high | urgent",
  "requirements": {
    "min_quality_score": 0.0-1.0,
    "max_cost_usd": number,
    "max_duration_seconds": number,
    "required_capabilities": ["string"],
    "preferred_domains": ["string"],
    "allow_multi_agent": boolean,
    "computer_use_required": boolean
  },
  "context": {},
  "output_schema": {},
  "metadata": {}
}
```

### Get Job Status

```http
GET /v1/matching/jobs/{job_id}
```

**Response:**
```json
{
  "job_id": "string",
  "worker_id": "string",
  "status": "pending | queued | matched | in_progress | completed | failed",
  "progress_pct": 0-100,
  "current_step": "string",
  "started_at": "datetime",
  "updated_at": "datetime"
}
```

### Get Job Result

```http
GET /v1/matching/jobs/{job_id}/result?wait=false
```

**Query params:**
- `wait` (boolean): Block until completion (max 5 min)

**Response:**
```json
{
  "job_id": "string",
  "status": "completed | failed",
  "output": {},
  "worker_id": "string",
  "quality_score": 0.0-1.0,
  "actual_cost_usd": number,
  "actual_duration_seconds": number,
  "completed_at": "datetime"
}
```

### Stream Job Events

```http
GET /v1/matching/jobs/{job_id}/events
```

Server-Sent Events stream with real-time updates.

### List Jobs

```http
GET /v1/matching/jobs?status=completed&limit=100
```

**Query params:**
- `status`: Filter by status
- `limit`: Max results (1-1000)

### List Workers

```http
GET /v1/matching/workers?domain=finance&limit=100
```

**Query params:**
- `domain`: Filter by domain
- `capability`: Filter by capability
- `available_only`: Only show available workers
- `limit`: Max results (1-2300)

### Get Worker Info

```http
GET /v1/matching/workers/{worker_id}
```

Returns full worker profile with capabilities, metrics, and current load.

### System Statistics

```http
GET /v1/matching/stats
```

Returns system-wide metrics:
- Jobs by status
- Worker availability
- Performance metrics
- Cost analytics

## 💻 Python Client Example

```python
import asyncio
import httpx

async def run_job():
    async with httpx.AsyncClient() as client:
        # Submit job
        response = await client.post(
            "http://127.0.0.1:2040/v1/matching/jobs",
            json={
                "description": "Write a technical blog post about Rust async programming",
                "job_type": "content_creation",
                "priority": "normal",
                "requirements": {
                    "preferred_domains": ["technology"],
                    "max_cost_usd": 1.0
                }
            }
        )
        job = response.json()
        job_id = job["job_id"]

        # Get result (blocking)
        result = await client.get(
            f"http://127.0.0.1:2040/v1/matching/jobs/{job_id}/result",
            params={"wait": "true"}
        )

        print(result.json())

asyncio.run(run_job())
```

## 🎬 Demo

Run the comprehensive demo:

```bash
python examples/matching_engine_demo.py
```

This demonstrates:
1. Worker discovery across domains
2. Single job submission and tracking
3. Batch multi-domain processing
4. System monitoring and analytics

## 🔮 Future Enhancements

- [ ] **Multi-agent collaboration**: Jobs that require multiple specialists
- [ ] **Human-in-the-loop**: Escalation to human workers for complex tasks
- [ ] **Learning & optimization**: Continuous improvement of matching algorithm
- [ ] **Custom agent training**: Fine-tune agents on domain-specific data
- [ ] **Result validation**: Automated quality checking and verification
- [ ] **Cost optimization**: Dynamic pricing and budget management
- [ ] **Priority queues**: SLA-based job prioritization
- [ ] **Real-time analytics**: Live dashboards and monitoring
- [ ] **Integration marketplace**: Connect to external tools and platforms
- [ ] **White-label API**: Deploy as private agent fleet

## 📈 Scaling to Billions

The architecture supports massive scale through:

1. **Horizontal scaling**: Each of 2300 agents can spawn unlimited instances
2. **Load balancing**: Intelligent distribution across agent instances
3. **Backend diversity**: Route to optimal AI provider (Anthropic, Parallel, etc.)
4. **Queue management**: Async job processing with priority scheduling
5. **Caching**: Results caching for repeated queries
6. **Monitoring**: Real-time metrics and auto-scaling triggers

## 🤝 Integration with Existing Systems

The Matching Engine integrates seamlessly with:

- **Parallel.ai**: For web research and data gathering tasks
- **Anthropic Claude**: For reasoning, computer use, and multi-modal tasks
- **MCP Servers**: For specialized tools and domain-specific integrations
- **Webhooks**: For async result delivery
- **SSE Streaming**: For real-time progress updates
- **REST API**: Standard HTTP interface for any client

## 📝 License

MIT

## 🙋 Support

- API Documentation: http://127.0.0.1:2040/docs
- GitHub Issues: https://github.com/yourusername/parallel-ai/issues
- Example Code: `/examples/matching_engine_demo.py`
