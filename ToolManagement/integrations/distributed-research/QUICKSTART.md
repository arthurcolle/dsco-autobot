# Matching Engine Quick Start

## Installation

```bash
# Install dependencies
pip install -r requirements.txt

# Or using the virtual environment
source .venv/bin/activate
pip install -r requirements.txt
```

## Start the Server

```bash
# Make sure .env file exists with your API_KEY
python -m parallel_websearch_server.main
```

The server will start at http://127.0.0.1:2040 and automatically initialize 2300 domain agents.

## Verify Installation

```bash
# Test worker registry
python -c "from parallel_websearch_server.worker_registry import get_registry; r = get_registry(); print(f'✓ {len(r.agents)} agents loaded')"

# Test matching engine
python -c "
from parallel_websearch_server.worker_registry import get_registry
from parallel_websearch_server.matching_engine import MatchingEngine
from parallel_websearch_server.matching_models import JobInput, JobType, JobPriority

registry = get_registry()
engine = MatchingEngine(registry)

job = JobInput(
    description='Test job',
    job_type=JobType.RESEARCH,
    priority=JobPriority.NORMAL
)

result = engine.match_job(job, 'test_001')
print(f'✓ Matched to: {result.selected_worker_id}')
"
```

## Quick Test

Once the server is running, test it:

```bash
# Submit a job
curl -X POST http://127.0.0.1:2040/v1/matching/jobs \
  -H "Content-Type: application/json" \
  -d '{
    "description": "Analyze the latest AI research trends",
    "job_type": "research",
    "priority": "normal",
    "requirements": {
      "preferred_domains": ["technology", "data_science"]
    }
  }'

# Check system stats
curl http://127.0.0.1:2040/v1/matching/stats

# List workers
curl http://127.0.0.1:2040/v1/matching/workers?limit=10
```

## Asynchronous Notifications (Mailbox)

Give a job a `notification_channel` to receive a completion event later:

```bash
curl -X POST http://127.0.0.1:2040/v1/matching/jobs \
  -H "Content-Type: application/json" \
  -d '{
    "description": "Deep research on small-molecule weight loss pipelines",
    "job_type": "research",
    "notification_channel": "agent-demo-mailbox"
  }'
```

Poll (or long-poll) the mailbox:

```bash
curl "http://127.0.0.1:2040/v1/matching/mailbox/agent-demo-mailbox?max_messages=5"
```

ACK delivered messages once processed:

```bash
curl -X POST http://127.0.0.1:2040/v1/matching/mailbox/agent-demo-mailbox/ack \
  -H "Content-Type: application/json" \
  -d '{"message_ids": ["msg_deadbeef123"]}'
```

## Run the Demo

```bash
python examples/matching_engine_demo.py
```

## View API Documentation

Open your browser to:
- Interactive docs: http://127.0.0.1:2040/docs
- ReDoc: http://127.0.0.1:2040/redoc

## Next Steps

See [MATCHING_ENGINE.md](MATCHING_ENGINE.md) for complete documentation.
