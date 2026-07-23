import argparse
import os

from .server import create_server

parser = argparse.ArgumentParser(description="Integration Fabric MCP")
parser.add_argument("--port", type=int, default=int(os.environ.get("PORT", "4010")))
args = parser.parse_args()

create_server(args.port).run()
