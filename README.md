# mcp
Model Context Protocol research and experiment

# content

1) ./mcp-jupyter-stdio-client

A sample MCP client based on stdio model C/S to chat with official postgres MCP server and get the query results back.

2) ./mcp-jupyter-sse-model

A sample MCP client based on SSE model to test out postgres MCP server supporting SSE. 

Following functions are supported:
* list_tools
* call_tool
* list_resources
* read_resource


./mcp-jupyter-sse-model/server/postgres

The rebuilt postgres MCP server using SSE model from official postgres MCP server  (https://github.com/modelcontextprotocol/servers/tree/main/src/postgres)

    postgres.py    ==> MCP server based on FastMCP

    postgres-base.py  ==> MCP server based on base MCP server 








