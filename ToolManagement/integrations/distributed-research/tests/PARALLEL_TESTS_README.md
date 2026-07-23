# Parallel Schema Validation Tests

## Overview

This test suite (`test_parallel_schema_validation.py`) addresses the production errors related to schema validation in the `manage_task_run` endpoint, specifically:

- **Status code 422**: Validation errors
- **"Auto schema is not supported for processor base"** errors
- Missing required fields in parallel requests
- Malformed schema structures

## Test Coverage

### 1. Schema Validation Errors (`TestSchemaValidationErrors`)

Tests individual schema validation edge cases:

- ✅ **AutoSchema with BASE processor** - Tests the specific error you're seeing where AutoSchema isn't supported for certain processor types
- ✅ **Missing required fields** - Tests requests with missing `processor` or `input` fields
- ✅ **Invalid processor types** - Tests with non-existent processor types
- ✅ **Malformed JSON schemas** - Tests schemas with incorrect structure
- ✅ **Empty input validation** - Tests edge case of empty input strings
- ✅ **Null task_spec** - Tests optional field handling

### 2. Parallel Schema Validation (`TestParallelSchemaValidation`)

Tests concurrent requests with mixed scenarios:

- ✅ **Mixed valid/invalid parallel requests** - 3 valid (LITE with TextSchema) + 2 invalid (BASE with AutoSchema) running concurrently
- ✅ **Different schema types in parallel** - Text, JSON, and complex schemas running simultaneously
- ✅ **Multiple processor types** - LITE, CORE, PRO processors running in parallel

### 3. High Concurrency Stress Tests (`TestHighConcurrencyStress`)

Stress tests to identify issues under load:

- ✅ **20 concurrent managed runs** - Tests system behavior with high concurrent load
- ✅ **30 concurrent mixed success/failure** - 20 successful + 10 validation errors
- ✅ **Concurrent requests with timeouts** - 5 completing + 5 timing out

### 4. Complex Schema Structures (`TestComplexSchemaStructures`)

Tests complex and nested schemas:

- ✅ **Deeply nested JSON schemas** - Tests 4-level nested object structures
- ✅ **Parallel complex schemas** - Multiple different complex schemas running concurrently

## Running the Tests

### Run all parallel schema validation tests:

```bash
pytest tests/test_parallel_schema_validation.py -v
```

### Run specific test class:

```bash
# Schema validation errors only
pytest tests/test_parallel_schema_validation.py::TestSchemaValidationErrors -v

# Parallel validation tests
pytest tests/test_parallel_schema_validation.py::TestParallelSchemaValidation -v

# Stress tests
pytest tests/test_parallel_schema_validation.py::TestHighConcurrencyStress -v

# Complex schemas
pytest tests/test_parallel_schema_validation.py::TestComplexSchemaStructures -v
```

### Run specific test:

```bash
# Test the specific error you're seeing
pytest tests/test_parallel_schema_validation.py::TestSchemaValidationErrors::test_auto_schema_with_base_processor -v
```

### Run with detailed output:

```bash
pytest tests/test_parallel_schema_validation.py -vv --tb=long
```

## Key Findings

Based on the error logs and code analysis:

### Root Cause of "Auto schema is not supported for processor base"

This error comes from the **upstream Parallel.ai API**, not from the middleware server. The API has restrictions on which processor types can use AutoSchema:

- ❌ **BASE processor**: Does NOT support AutoSchema
- ✅ **LITE, CORE, PRO, etc.**: Support AutoSchema

### Validation Flow

1. Request comes to middleware server (`/v1/tasks/runs/managed`)
2. Middleware validates Pydantic models (basic structure)
3. Request forwarded to upstream Parallel.ai API
4. **Upstream API performs processor-specific schema validation**
5. If invalid, returns 422 with detailed error

### Schema Options by Processor

| Processor | TextSchema | JsonSchema | AutoSchema |
|-----------|------------|------------|------------|
| LITE      | ✅         | ✅         | ✅         |
| BASE      | ✅         | ✅         | ❌         |
| CORE      | ✅         | ✅         | ✅         |
| PRO       | ✅         | ✅         | ✅         |
| ULTRA     | ✅         | ✅         | ✅         |

## Test Results Interpretation

### Expected Test Outcomes:

1. **AutoSchema with BASE processor** → Should return 422 error ✅
2. **Missing required fields** → Should return 422 error ✅
3. **Valid schemas with LITE/CORE/PRO** → Should succeed ✅
4. **High concurrency (20+ parallel)** → All should process correctly ✅
5. **Mixed valid/invalid parallel** → Should handle both gracefully ✅

### Performance Benchmarks:

The stress tests measure:
- **Throughput**: How many concurrent managed runs can complete
- **Error handling**: Graceful degradation with validation errors
- **Timeout handling**: Proper 504 responses when tasks timeout

Example output:
```
✓ Completed 20 concurrent managed runs in 0.45s
✓ Handled 30 concurrent requests: 20 success, 10 failures
```

## Integration with Existing Tests

The new tests complement existing test coverage:

- **test_task_runs_comprehensive.py** (690 lines)
  - Tests basic task run lifecycle
  - Tests managed runs with success/failure
  - Has some concurrent tests (5 parallel)

- **test_parallel_schema_validation.py** (NEW - 800+ lines)
  - **Focuses specifically on schema validation errors**
  - **Higher concurrency (20-30 parallel requests)**
  - **Tests processor-schema compatibility**
  - **Tests complex nested schemas**

## Debugging Production Errors

If you see these errors in production:

### Error: "Auto schema is not supported for processor base"

**Cause**: Request using BASE processor with AutoSchema

**Fix**: Use TextSchema or JsonSchema instead:

```python
# ❌ DON'T
{
    "processor": "base",
    "input": "...",
    "task_spec": {
        "output_schema": {"type": "auto"}
    }
}

# ✅ DO
{
    "processor": "base",
    "input": "...",
    "task_spec": {
        "output_schema": {"type": "text"}
    }
}

# OR use a different processor
{
    "processor": "lite",  # LITE supports AutoSchema
    "input": "...",
    "task_spec": {
        "output_schema": {"type": "auto"}
    }
}
```

### Error: Missing required fields

**Cause**: Request missing `processor` or `input`

**Fix**: Ensure all required fields are present:

```python
# ✅ Required fields
{
    "processor": "lite",  # REQUIRED
    "input": "your input text",  # REQUIRED
    "task_spec": {  # OPTIONAL but recommended
        "output_schema": {"type": "text"}
    }
}
```

## Contributing

When adding new tests:

1. Follow the existing class structure
2. Use descriptive test names (e.g., `test_auto_schema_with_base_processor`)
3. Add comprehensive docstrings
4. Use the `TaskRunFactory` for creating test data
5. Mock HTTP responses appropriately
6. Test both success and failure paths

## Test Dependencies

```toml
[project.optional-dependencies]
dev = [
    "pytest>=7.4.0",
    "pytest-asyncio>=0.21.0",
    "pytest-httpx>=0.21.0",
]
```

Install with:
```bash
pip install -e ".[dev]"
```

## CI/CD Integration

Add to your CI pipeline:

```yaml
- name: Run parallel schema validation tests
  run: |
    pytest tests/test_parallel_schema_validation.py -v --tb=short
    pytest tests/test_parallel_schema_validation.py::TestHighConcurrencyStress -v
```

## Related Files

- `/parallel_websearch_server/routes/tasks.py:311` - `manage_task_run` endpoint implementation
- `/parallel_websearch_server/models.py:276-280` - `TaskSpec` and schema models
- `/tests/fixtures.py` - Test fixtures and factories
- `/tests/conftest.py` - Pytest configuration

## Summary Statistics

- **Total new tests**: 18 test methods
- **Test lines**: ~800 lines
- **Concurrency tested**: Up to 30 parallel requests
- **Processor types tested**: 5 (LITE, BASE, CORE, PRO, ULTRA)
- **Schema types tested**: 3 (text, json, auto)
- **Edge cases covered**: 10+
