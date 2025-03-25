import json
import os
from typing import Any

import psycopg2
from psycopg2.extras import RealDictCursor
import uvicorn
import argparse
import datetime
import decimal

from mcp.server.fastmcp import FastMCP
from mcp.server.sse import SseServerTransport

from starlette.applications import Starlette
from starlette.requests import Request
from starlette.routing import Mount, Route

from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Initialize FastMCP server for postgres tools (SSE)
mcp = FastMCP("postgres")

# --- Constants and Global Variables ---
SCHEMA_RESOURCE = "schema://main"
DATABASE_URL = None  # Will be set later from command-line argument or .env


def execute_query(query, params=None):
    # Connect using a single connection string (DSN)
    conn = psycopg2.connect(DATABASE_URL)
    try:
        with conn.cursor() as cur:
            cur.execute(query, params)
            # If it's a SELECT query, fetch and return results.
            if query.strip().upper().startswith("SELECT"):
                results = cur.fetchall()
                return results
            # For non-SELECT queries, commit changes.
            conn.commit()
    finally:
        conn.close()


# --- Resource Handlers ---
@mcp.resource(SCHEMA_RESOURCE)
async def get_schema() -> str:
    
    rows = execute_query(
        "SELECT table_name, column_name, data_type FROM information_schema.columns WHERE table_schema='public'"
    )

    # Convert each row (a tuple) into a dictionary.
    schema_data = [
        {"table_name": row[0], "column_name": row[1], "data_type": row[2]}
        for row in rows
    ]

    # Build the resources list using the converted schema_data.
    resources = [
        {
            "uri": f"table-schema://{item['table_name']}",
            "mimeType": "application/json",
            "name": f"\"{item['table_name']}\" database schema",
        }
        for item in schema_data
    ]

    return json.dumps({ "resources" : resources}, indent=2)



@mcp.resource("table-schema://{table}")
async def get_table_schema(table: str) -> str:
    
    rows = execute_query(
        f"SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '{table}'"
    )

    # Manually create dictionaries from each row
    schema_data = [
        {"column_name": row[0], "data_type": row[0]}
        for row in rows
    ]

    return json.dumps({
            "uri": "table-schema://{table}",
            "mimeType": "application/json",
            "text": json.dumps(schema_data, indent=2)
            }, indent=2)



class CustomEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, (datetime.date, datetime.datetime)):
            return obj.isoformat()
        if isinstance(obj, decimal.Decimal):
            return float(obj)
        return super(CustomEncoder, self).default(obj)
    

# readonly query support
def fetch_query(sql):
    # Connect using a single connection string (DSN)
    conn = psycopg2.connect(DATABASE_URL)
    try:
        # Start a transaction block
        with conn:
            # Use a RealDictCursor to fetch rows as dictionaries
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("SET TRANSACTION READ ONLY")
                cur.execute(sql)
                rows = cur.fetchall()
                return rows
    finally:
        conn.close()


# --- Register the "query" tool using the mcp.tool() decorator with positional arguments ---
@mcp.tool("query", "Run a read-only SQL query")
async def query_tool(sql: str) -> str:
    rows = fetch_query(sql)
    return json.dumps([dict(row) for row in rows], indent=2, cls=CustomEncoder)


# --- Starlette App and SSE Transport ---

def create_starlette_app(mcp_server: FastMCP, *, debug: bool = False) -> Starlette:
    """Create a Starlette application to serve the MCP server over SSE."""
    sse = SseServerTransport("/messages/")

    async def handle_sse(request: Request) -> None:
        async with sse.connect_sse(
            request.scope,
            request.receive,
            request._send,  # type: ignore
        ) as (read_stream, write_stream):
            await mcp_server.run(
                read_stream,
                write_stream,
                mcp_server.create_initialization_options(),
            )

    return Starlette(
        debug=debug,
        routes=[
            Route("/sse", endpoint=handle_sse),
            Mount("/messages/", app=sse.handle_post_message),
        ],
    )

# --- Main Entrypoint ---

if __name__ == "__main__":

    mcp_server = mcp._mcp_server  # noqa: WPS437

    # Use .env variable PORT if available; default to 8080
    DEFAULT_PORT = int(os.getenv("PORT", "8080"))
    DEFAULT_DATABASE_URL = os.getenv("DATABASE_URL")

    parser = argparse.ArgumentParser(description="Run MCP SSE-based Postgres server")
    parser.add_argument("--host", default="0.0.0.0", help="Host to bind to")
    parser.add_argument("--port", type=int, default=DEFAULT_PORT, help="Port to listen on")
    parser.add_argument("--database", type=str, default=DEFAULT_DATABASE_URL,
                        help="Database URL (or set DATABASE_URL in .env)")
    args = parser.parse_args()

    if not args.database:
        print("Error: A database URL must be provided either via --database or in the .env file (DATABASE_URL).")
        exit(1)

    DATABASE_URL = args.database

    starlette_app = create_starlette_app(mcp_server, debug=True)

    try:
        uvicorn.run(starlette_app, host=args.host, port=args.port)
    except (KeyboardInterrupt):
        print("Shutdown requested...exiting gracefully.")
