using System.Collections.Generic;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Storage;
using Npgsql;

namespace Cleipnir.ResilientFunctions.PostgreSQL;

public class PostgresCorrelationStore : ICorrelationStore
{
    private readonly string _connectionString;
    private readonly string _tablePrefix;

    public PostgresCorrelationStore(string connectionString, string tablePrefix = "")
    {
        _connectionString = connectionString;
        _tablePrefix = tablePrefix;
    }

    private string? _initializeSql;
    public async Task Initialize()
    {
        await using var conn = await CreateConnection();
        _initializeSql ??= @$"
            CREATE TABLE IF NOT EXISTS {_tablePrefix}_correlations (
                type VARCHAR(255),
                instance VARCHAR(255),
                correlation VARCHAR(255) NOT NULL,
                PRIMARY KEY(type, instance, correlation)
            );

            CREATE UNIQUE INDEX IF NOT EXISTS idx_{_tablePrefix}_correlations
            ON {_tablePrefix}_correlations(correlation, type, instance);";
        var command = new NpgsqlCommand(_initializeSql, conn);
        await command.ExecuteNonQueryAsync();
    }

    private string? _truncateSql;
    public async Task Truncate()
    {
        await using var conn = await CreateConnection();
        _truncateSql ??= $"TRUNCATE TABLE {_tablePrefix}_correlations";
        var command = new NpgsqlCommand(_truncateSql, conn);
        await command.ExecuteNonQueryAsync();
    }

    private string? _setCorrelationSql;
    public async Task SetCorrelation(FlowId flowId, string correlationId)
    {
        var (flowType, flowInstance) = flowId;
        
        await using var conn = await CreateConnection();
        _setCorrelationSql ??= $@"
          INSERT INTO {_tablePrefix}_correlations 
              (type, instance, correlation)
          VALUES
              ($1, $2, $3) 
          ON CONFLICT (type, instance, correlation) DO NOTHING";
        
        await using var command = new NpgsqlCommand(_setCorrelationSql, conn)
        {
            Parameters =
            {
                new() {Value = flowType.Value},
                new() {Value = flowInstance.Value},
                new() {Value = correlationId}
            }
        };

        await command.ExecuteNonQueryAsync();
    }

    private string? _getCorrelationsSql;
    public async Task<IReadOnlyList<FlowId>> GetCorrelations(string correlationId)
    {
        await using var conn = await CreateConnection();
        _getCorrelationsSql ??= @$"
            SELECT type, instance
            FROM {_tablePrefix}_correlations
            WHERE correlation = $1";
        await using var command = new NpgsqlCommand(_getCorrelationsSql, conn)
        {
            Parameters =
            {
                new() { Value = correlationId }
            }
        };

        await using var reader = await command.ExecuteReaderAsync();

        var functions = new List<FlowId>();
        while (await reader.ReadAsync())
        {
            var functionType = reader.GetString(0);
            var functionInstance = reader.GetString(1);
            
            functions.Add(new FlowId(functionType, functionInstance));
        }

        return functions;
    }

    private string? _getCorrelationsForFunction;
    public async Task<IReadOnlyList<string>> GetCorrelations(FlowId flowId)
    {
        var (typeId, instanceId) = flowId;
        await using var conn = await CreateConnection();
        _getCorrelationsForFunction ??= @$"
            SELECT correlation
            FROM {_tablePrefix}_correlations
            WHERE type = $1 AND instance = $2";
        await using var command = new NpgsqlCommand(_getCorrelationsForFunction, conn)
        {
            Parameters =
            {
                new() { Value = typeId.Value },
                new() { Value = instanceId.Value }
            }
        };

        await using var reader = await command.ExecuteReaderAsync();

        var correlations = new List<string>();
        while (await reader.ReadAsync())
        {
            var correlation = reader.GetString(0);
            correlations.Add(correlation);
        }

        return correlations;
    }

    private string? _removeCorrelationsSql;
    public async Task RemoveCorrelations(FlowId flowId)
    {
        var (flowType, flowInstance) = flowId;
        
        await using var conn = await CreateConnection();
        _removeCorrelationsSql ??= $@"
          DELETE FROM {_tablePrefix}_correlations 
          WHERE type = $1 AND instance = $2";
        
        await using var command = new NpgsqlCommand(_removeCorrelationsSql, conn)
        {
            Parameters =
            {
                new() {Value = flowType.Value},
                new() {Value = flowInstance.Value},
            }
        };

        await command.ExecuteNonQueryAsync();
    }

    private string? _removeCorrelationSql;
    public async Task RemoveCorrelation(FlowId flowId, string correlationId)
    {
        var (flowType, flowInstance) = flowId;
        
        await using var conn = await CreateConnection();
        _removeCorrelationSql ??= $@"
          DELETE FROM {_tablePrefix}_correlations 
          WHERE type = $1 AND instance = $2 AND correlation = $3";
        
        await using var command = new NpgsqlCommand(_removeCorrelationSql, conn)
        {
            Parameters =
            {
                new() {Value = flowType.Value},
                new() {Value = flowInstance.Value},
                new() {Value = correlationId},
            }
        };

        await command.ExecuteNonQueryAsync();
    }
    
    private async Task<NpgsqlConnection> CreateConnection()
    {
        var conn = new NpgsqlConnection(_connectionString);
        await conn.OpenAsync();
        return conn;
    }
}