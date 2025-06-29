# AWS S3 Tables MCP Server

An AWS Labs Model Context Protocol (MCP) server for AWS S3 Tables that enables AI assistants to interact with S3-based table storage.

## Overview

The S3 Tables MCP Server simplifies the management of S3-based tables by providing capabilities to create and query tables, generate tables directly from CSV files uploaded to S3, and access metadata through the S3 Metadata Table. This allows for streamlined data operations and easier integration with S3-stored datasets.

## Features

- **Table Bucket Management**: Create, delete, and manage table buckets
- **Namespace Management**: Create and delete namespaces within table buckets
- **Table Management**: Create, delete, rename, and manage individual tables
- **Maintenance Configuration**: Set up and manage maintenance configurations for tables and buckets
- **Policy Management**: Manage resource policies for tables and buckets
- **Metadata Management**: Handle table metadata and locations
- **Read-Only Mode**: Optional security feature to restrict operations to read-only operations
- **Athena Integration**: Execute SQL queries directly against S3 Tables using Amazon Athena for seamless data analysis and reporting
- **CSV to Table Conversion**: Automatically generate S3 Tables from CSV files uploaded to S3 buckets, streamlining data ingestion workflows
- **Metadata Discovery**: Access comprehensive table metadata through the S3 Metadata Table for enhanced data governance and cataloging

## Prerequisites

1. Install `uv` from [Astral](https://docs.astral.sh/uv/getting-started/installation/) or the [GitHub README](https://github.com/astral-sh/uv#installation)
2. Install Python using `uv python install 3.10`
3. Set up AWS credentials with access to AWS services
   - You need an AWS account with appropriate permissions
   - Configure AWS credentials with `aws configure` or environment variables

## Setup

### Installation

Configure the MCP server in your MCP client configuration (e.g., for Amazon Q Developer CLI, edit `~/.aws/amazonq/mcp.json`):

```json
{
  "mcpServers": {
    "awslabs.s3-tables-mcp-server": {
      "command": "uvx",
      "args": ["awslabs.s3-tables-mcp-server@latest"],
      "env": {
        "AWS_PROFILE": "your-aws-profile",
        "AWS_REGION": "us-east-1"
      }
    }
  }
}
```

or docker after a successful `docker build -t awslabs/s3-tables-mcp-server.`:

```file
# fictitious `.env` file with AWS temporary credentials
AWS_ACCESS_KEY_ID=<from the profile you set up>
AWS_SECRET_ACCESS_KEY=<from the profile you set up>
AWS_SESSION_TOKEN=<from the profile you set up>
```

```json
{
  "mcpServers": {
    "awslabs.s3-tables-mcp-server": {
      "command": "docker",
      "args": [
        "run",
        "--rm",
        "--interactive",
        "--env-file",
        "/full/path/to/file/above/.env",
        "awslabs/s3-tables-mcp-server:latest"
      ],
      "env": {},
      "disabled": false,
      "autoApprove": []
    }
  }
}
```

## Server Configuration Options

The AWS S3 Tables MCP Server supports several command-line arguments that can be used to configure its behavior:

### `--allow-write`

Enables tools that create or modify resources in the user's AWS account. When this flag is not enabled, the server runs in read-only mode that only allows read operations. This enhances security by preventing any modifications to the tables. In read-only mode:

- Read operations (`list_table_buckets`, `list_namespaces`, `list_tables`) work normally
- Write operations (`create_table_bucket`, `delete_table_bucket`, `INSERT, UPDATE sql statements`, etc.) are blocked and return a permission error

This mode is particularly useful for:
- Demonstration environments
- Security-sensitive applications
- Integration with public-facing AI assistants
- Protecting production tables from unintended modifications

Example:
```json
{
  "mcpServers": {
    "awslabs.s3-tables-mcp-server": {
      "command": "uvx",
      "args": [
        "awslabs.s3-tables-mcp-server@latest",
        "--allow-write"
      ],
      "env": {
        "AWS_PROFILE": "your-aws-profile",
        "AWS_REGION": "us-east-1"
      }
    }
  }
}
```

## Usage Examples

| Prompt | Description |
|--------|-------------|
| `Query all available metadata about test-bucket` | Retrieves comprehensive metadata information for a specific table bucket, including namespaces, tables, and configuration details |
| `Find top 3 customers by spending in the transactions table` | Executes a SQL query using Athena to analyze customer transaction data and identify the highest-spending customers |
| `Create a table bucket with name hello-world` | Creates a new S3 Tables bucket for organizing and managing table data with the specified name |
| `Create an s3 table from s3://my-bucket/data.csv` | Automatically generates an S3 Table from an existing CSV file in S3, enabling immediate querying and analysis of the data |
| `List all tables in the sales namespace` | Displays all available tables within a specific namespace for data discovery and exploration |
| `Show the schema for customer_data table` | Retrieves the table structure and column definitions to understand the data format and types |
| `Run a query to find monthly revenue trends` | Performs data analysis using SQL queries to extract business insights from stored table data |

## Security Considerations

When using this MCP server, consider:

- The MCP server needs permissions to create and manage AWS S3 Tables resources
- Resource creation is disabled by default, enable it by setting the `--allow-write` flag
- Follow the principle of least privilege when setting up IAM permissions
- Use separate AWS profiles for different environments (dev, test, prod)

## Troubleshooting

- If you encounter permission errors, verify your IAM user has the correct policies attached
- For connection issues, check network configurations and security groups
- For general AWS S3 Tables issues, consult the [AWS S3 Tables documentation](https://docs.aws.amazon.com/s3/)
