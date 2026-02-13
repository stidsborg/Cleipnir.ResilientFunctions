# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Core Architecture

**Cleipnir Resilient Functions** is a .NET framework implementing the saga pattern for crash-resilient function execution. The system ensures functions complete despite failures, restarts, and deployments.

### Key Components

- **Core Library** (`Core/Cleipnir.ResilientFunctions/`): Main framework implementation
  - `FunctionsRegistry`: Central registry for resilient function registration and management
  - `Storage/`: Persistence layer interfaces and in-memory implementations
  - `Domain/`: Core domain models (FlowId, FlowType, Effect, States, etc.)
  - `CoreRuntime/`: Runtime execution engine with watchdogs and invocation handling
  - `Messaging/`: Message-based workflow support

- **Store Implementations** (`Stores/`): Database-specific persistence layers
  - `PostgreSQL/`: PostgreSQL store implementation
  - `SqlServer/`: SQL Server store implementation
  - `MariaDB/`: MariaDB store implementation
  - Each includes both implementation and test projects

- **CLI Tool** (`Cli/`): Command-line interface for framework operations

- **Samples** (`Samples/`): Example implementations demonstrating framework usage

## Development Commands

### Building
```bash
dotnet build Cleipnir.ResilientFunctions.sln
```

### Testing
Run all tests with database dependencies:
```bash
# Start test databases
docker compose -f "docker-compose.yml" up -d

# Ensure database connections
dotnet run --project ./Stores/EnsureDatabaseConnections/EnsureDatabaseConnections.csproj --no-build

# Run core tests
dotnet test ./Core/Cleipnir.ResilientFunctions.Tests --no-build --logger "console;verbosity=detailed"

# Run database-specific tests
dotnet test ./Stores/MariaDB/Cleipnir.ResilientFunctions.MariaDB.Tests --no-build --logger "console;verbosity=detailed"
dotnet test ./Stores/PostgreSQL/Cleipnir.ResilientFunctions.PostgreSQL.Tests --no-build --logger "console;verbosity=detailed"
dotnet test ./Stores/SqlServer/Cleipnir.ResilientFunctions.SqlServer.Tests --no-build --logger "console;verbosity=detailed"

# Clean up
docker compose -f "docker-compose.yml" down
```

### Running Single Tests
```bash
dotnet test <specific-test-project-path> --filter "TestMethodName" --no-build --logger "console;verbosity=detailed"
```

## Database Setup

The project uses Docker Compose for test databases with these credentials:
- **SQL Server**: SA password: "Pa55word!", port 1433
- **PostgreSQL**: POSTGRES_PASSWORD: "Pa55word!", port 5432
- **MariaDB**: MARIADB_ROOT_PASSWORD: "Pa55word!", port 3306

## Key Framework Concepts

- **Resilient Functions**: Functions registered with the framework that automatically retry until completion
- **Flow Types**: Logical groupings of similar function executions
- **Flow Instances**: Specific executions identified by FlowId
- **Effects**: Idempotent operations captured for crash consistency
- **States**: Persistent workflow state that survives crashes
- **Messaging**: Event-driven communication between workflow components
- **Watchdogs**: Background processes monitoring and resuming crashed or postponed executions

## Project Structure Patterns

- Test projects follow naming: `<MainProject>.Tests`
- Store implementations include both library and test projects
- Core framework is database-agnostic with pluggable store implementations
- All projects target .NET 10.0

## Framework Usage Patterns

Functions are registered using `FunctionsRegistry` and can be:
- **Actions**: `RegisterAction()` for void-returning functions
- **Functions**: `RegisterFunc()` for value-returning functions
- **Parameterless**: `RegisterParamless()` for functions without input parameters

The framework supports both direct function invocation and message-based workflows using `workflow.Messages` for event-driven scenarios.
- never run docker compose assume it is running in the background
- do not change white spaces for non affected code
- to reset test databases run `bash reset_docker` followed by `dotnet run --project ./Stores/EnsureDatabaseConnections/EnsureDatabaseConnections.csproj` (it retries internally, no sleep needed)