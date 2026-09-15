# Distributed Publish source provenance

The production Substack client in this package is vendored from:

- source checkout: `distributed-publish-mcp/substack_client.py`
- source package: `distributed-publish-mcp` version `1.0.0`
- integration wrapper: `native/distributed_publish_mcp/server.py`

The source server provides stdio MCP. This wrapper is the DSCO core-integration
deployment: it adds HTTP MCP, bearer-protected execution, registry announce,
core capability metadata, event/webhook tools, and Modal lifecycle handling.

When updating the client, copy it byte-for-byte from the source checkout, run
both repositories' test suites, and verify the HTTP `initialize`, `tools/list`,
and guarded `tools/call` path before deploying.
