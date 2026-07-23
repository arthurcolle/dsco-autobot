import argparse
import os
from .server import create_server

parser = argparse.ArgumentParser(description="Distributed Document Q&A MCP")
parser.add_argument("--port", type=int, default=int(os.environ.get("PORT", "4006")))
args = parser.parse_args()

server = create_server(args.port)
server.run()
