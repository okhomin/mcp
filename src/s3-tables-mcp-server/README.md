# AWS Labs awslabs.documentdb-mcp-server MCP Server

An AWS Labs Model Context Protocol (MCP) server for awslabs.documentdb-mcp-server

## Instructions

Instructions for using this awslabs.documentdb-mcp-server MCP server. This can be used by clients to improve the LLM's understanding of available tools, resources, etc. It can be thought of like a 'hint' to the model. For example, this information MAY be added to the system prompt. Important to be clear, direct, and detailed.

## TODO (REMOVE AFTER COMPLETING)

* [ ] Optionally add an ["RFC issue"](https://github.com/awslabs/mcp/issues) for the community to review
* [ ] Generate a `uv.lock` file with `uv sync` -> See [Getting Started](https://docs.astral.sh/uv/getting-started/)
* [ ] Remove the example tools in `./awslabs/awslabs.documentdb_mcp_server_mcp_server/server.py`
* [ ] Add your own tool(s) following the [DESIGN_GUIDELINES.md](https://github.com/awslabs/mcp/blob/main/DESIGN_GUIDELINES.md)
* [ ] Keep test coverage at or above the `main` branch - NOTE: GitHub Actions run this command for CodeCov metrics `uv run --frozen pytest --cov --cov-branch --cov-report=term-missing`
* [ ] Document the MCP Server in this "README.md"
* [ ] Add a section for this awslabs.documentdb-mcp-server MCP Server at the top level of this repository "../../README.md"
* [ ] Create the "../../doc/servers/awslabs.documentdb-mcp-server-mcp-server.md" file with these contents:

    ```markdown
    ---
    title: awslabs.documentdb-mcp-server MCP Server
    ---

    {% include "../../src/awslabs.documentdb-mcp-server-mcp-server/README.md" %}
    ```
  
* [ ] Reference within the "../../doc/index.md" like this:

    ```markdown
    ### awslabs.documentdb-mcp-server MCP Server
    
    An AWS Labs Model Context Protocol (MCP) server for awslabs.documentdb-mcp-server
    
    **Features:**
    
    - Feature one
    - Feature two
    - ...

    Instructions for using this awslabs.documentdb-mcp-server MCP server. This can be used by clients to improve the LLM's understanding of available tools, resources, etc. It can be thought of like a 'hint' to the model. For example, this information MAY be added to the system prompt. Important to be clear, direct, and detailed.
    
    [Learn more about the awslabs.documentdb-mcp-server MCP Server](servers/awslabs.documentdb-mcp-server-mcp-server.md)
    ```

* [ ] Submit a PR and pass all the checks
