import argparse
import os

from .server import create_server

parser = argparse.ArgumentParser(description="Finance Fabric MCP")
parser.add_argument("--port", type=int, default=int(os.environ.get("PORT", "4009")))
args = parser.parse_args()

create_server(args.port).run()
