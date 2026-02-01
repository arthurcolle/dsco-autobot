"""Entry point for running the Registry Wrapper MCP as a module."""

import sys

if __name__ == "__main__":
    from .server import RegistryWrapperMCP
    import argparse
    import logging

    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=4005)
    parser.add_argument("--registry-url", type=str, default="http://localhost:2016")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO)

    server = RegistryWrapperMCP(port=args.port, registry_url=args.registry_url)
    server.run()
