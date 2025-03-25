import json
import os
from typing import Any

import psycopg2
from psycopg2.extras import RealDictCursor
import uvicorn
import argparse
import datetime
import decimal
from pydantic import AnyUrl

from mcp.server import NotificationOptions, Server
from mcp.server.sse import SseServerTransport
import mcp.types as types

from starlette.applications import Starlette
from starlette.requests import Request
from starlette.routing import Mount, Route

from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Initialize FastMCP server for postgres tools (SSE)
mcp = Server("postgres")  #FastMCP("postgres")

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

@mcp.list_resources()
async def list_schema_resources() -> list[types.Resource]:
    """
    List available database schema resources.
    Each resource corresponds to a distinct table in the public schema.
    """
    # Query for distinct table names.
    rows = execute_query(
        "SELECT DISTINCT table_name FROM information_schema.columns WHERE table_schema='public'"
    )
    # Create a set of unique table names.
    table_names = {row[0] for row in rows}

    # Build the list of resource objects.
    resources = [
        types.Resource(
            uri=AnyUrl(f"table-schema://{table}"),
            mimeType="application/json",
            name=f'"{table}" database schema',
            description=f"Schema details for table '{table}'",
        )
        for table in table_names
    ]
    return resources


# read resource handler
@mcp.read_resource()
async def handle_read_resource(uri: AnyUrl) -> str:
    print(f"Handling read_resource request for URI: {uri}")

    if uri.scheme != "table-schema":
        print(f"Unsupported URI scheme: {uri.scheme}")
        return f"Unsupported URI scheme: {uri.scheme}"

    table = str(uri).replace("table-schema://", "")

    rows = execute_query(
        f"SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '{table}'"
    )

    # Manually create dictionaries from each row
    schema_data = [
        {"column_name": row[0], "data_type": row[0]}
        for row in rows
    ]

    return json.dumps(schema_data, indent=2)  



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


@mcp.call_tool()
async def handle_call_tool(
    name: str, arguments: dict[str, Any] | None
) -> list[types.TextContent | types.ImageContent | types.EmbeddedResource]:
    try:

        #print(f"Handling call_tool request for tool: {name} with arguments: {arguments}")
        if name == "query":

            rows = fetch_query(arguments["sql"])

            return [types.TextContent(type="text", text=json.dumps([dict(row) for row in rows], indent=2, cls=CustomEncoder))]
        else:   
            return [types.TextContent(type="text", text=f"Unknown tool: {name}")]
      
    except Exception as e:
        return [types.TextContent(type="text", text=f"Error: {str(e)}")]

# list tools endpoint
@mcp.list_tools()
async def handle_list_tools() -> list[types.Tool]:
    return [
        types.Tool(
            name="query",
            description="Run a read-only SQL query",
            inputSchema={
                "type": "object",
                "properties": {
                    "sql": {"type": "string"}
                },
                "required": ["sql"],
            },
        )
    ]
    

# --- Starlette App and SSE Transport ---

def create_starlette_app(mcp_server: Server, *, debug: bool = False) -> Starlette:
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

    starlette_app = create_starlette_app(mcp, debug=True)

    try:
        uvicorn.run(starlette_app, host=args.host, port=args.port)
    except (KeyboardInterrupt):
        print("Shutdown requested...exiting gracefully.")
