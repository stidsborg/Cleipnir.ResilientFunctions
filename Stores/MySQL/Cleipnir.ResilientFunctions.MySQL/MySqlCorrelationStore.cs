﻿using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Storage;
using MySqlConnector;

namespace Cleipnir.ResilientFunctions.MySQL;

public class MySqlCorrelationStore : ICorrelationStore
{
    private readonly string _connectionString;
    private readonly string _tablePrefix;

    public MySqlCorrelationStore(string connectionString, string tablePrefix = "")
    {
        _connectionString = connectionString;
        _tablePrefix = tablePrefix;
    }

    private string? _initialize;
    public async Task Initialize()
    {
        await using var conn = await CreateConnection();
        _initialize ??= @$"
            CREATE TABLE IF NOT EXISTS {_tablePrefix}_correlations (
                type VARCHAR(200) NOT NULL,
                instance VARCHAR(200) NOT NULL,
                correlation VARCHAR(200) NOT NULL,
                PRIMARY KEY (type, instance, correlation),
                INDEX (correlation, type, instance)
            );";
        var command = new MySqlCommand(_initialize, conn);
        await command.ExecuteNonQueryAsync();
    }

    private string? _truncateSql;
    public async Task Truncate()
    {
        await using var conn = await CreateConnection();
        _truncateSql ??= $"TRUNCATE TABLE {_tablePrefix}_correlations";
        var command = new MySqlCommand(_truncateSql, conn);
        await command.ExecuteNonQueryAsync();
    }

    private string? _setCorrelationSql;
    public async Task SetCorrelation(FlowId flowId, string correlationId)
    {
        var (flowType, flowInstance) = flowId;
        await using var conn = await CreateConnection();
        _setCorrelationSql ??= $@"
          INSERT IGNORE INTO {_tablePrefix}_correlations 
              (type, instance, correlation)
          VALUES
              (?, ?, ?)";
        
        await using var command = new MySqlCommand(_setCorrelationSql, conn)
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

    private string? _getCorrelationsSql;
    public async Task<IReadOnlyList<FlowId>> GetCorrelations(string correlationId)
    {
        await using var conn = await CreateConnection();
        _getCorrelationsSql ??= @$"
            SELECT type, instance
            FROM {_tablePrefix}_correlations
            WHERE correlation = ?";
        await using var command = new MySqlCommand(_getCorrelationsSql, conn)
        {
            Parameters =
            {
                new() { Value = correlationId },
            }
        };

        await using var reader = await command.ExecuteReaderAsync();

        var states = new List<FlowId>();
        while (await reader.ReadAsync())
        {
            var functionType = reader.GetString(0);
            var functionInstance = reader.GetString(1);
            states.Add(new FlowId(functionType, functionInstance));
        }

        return states;
    }

    private string? _getInstancesForFlowTypeAndCorrelation;
    public async Task<IReadOnlyList<FlowInstance>> GetCorrelations(FlowType flowType, string correlationId)
    {
        await using var conn = await CreateConnection();
        _getInstancesForFlowTypeAndCorrelation ??= @$"
            SELECT instance
            FROM {_tablePrefix}_correlations
            WHERE type = ? AND correlation = ?";
        await using var command = new MySqlCommand(_getInstancesForFlowTypeAndCorrelation, conn)
        {
            Parameters =
            {
                new() { Value = flowType.Value },
                new() { Value = correlationId },
            }
        };

        await using var reader = await command.ExecuteReaderAsync();

        var correlations = new List<FlowInstance>();
        while (await reader.ReadAsync())
        {
            var correlation = reader.GetString(0);
            correlations.Add(correlation);
        }

        return correlations;
    }

    private string? _getCorrelationsForFunctionSql;
    public async Task<IReadOnlyList<string>> GetCorrelations(FlowId flowId)
    {
        var (typeId, instanceId) = flowId;
        await using var conn = await CreateConnection();
        _getCorrelationsForFunctionSql ??= @$"
            SELECT correlation
            FROM {_tablePrefix}_correlations
            WHERE type = ? AND instance = ?";
        await using var command = new MySqlCommand(_getCorrelationsForFunctionSql, conn)
        {
            Parameters =
            {
                new() { Value = typeId.Value },
                new() { Value = instanceId.Value },
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
        var (typeId, instanceId) = flowId;
        await using var conn = await CreateConnection();
        _removeCorrelationsSql ??= $"DELETE FROM {_tablePrefix}_correlations WHERE type = ? AND instance = ?";
        await using var command = new MySqlCommand(_removeCorrelationsSql, conn)
        {
            Parameters =
            {
                new() { Value =  typeId.Value },
                new() { Value =  instanceId.Value },
            }
        };

        await command.ExecuteNonQueryAsync();
    }

    private string? _removeCorrelationSql;
    public async Task RemoveCorrelation(FlowId flowId, string correlationId)
    {
        var (typeId, instanceId) = flowId;
        await using var conn = await CreateConnection();
        _removeCorrelationSql ??= $"DELETE FROM {_tablePrefix}_correlations WHERE type = ? AND instance = ? AND correlation = ?";
        await using var command = new MySqlCommand(_removeCorrelationSql, conn)
        {
            Parameters =
            {
                new() { Value =  typeId.Value },
                new() { Value =  instanceId.Value },
                new() { Value =  correlationId },
            }
        };

        await command.ExecuteNonQueryAsync();
    }
    
    private async Task<MySqlConnection> CreateConnection()
    {
        var conn = new MySqlConnection(_connectionString);
        await conn.OpenAsync();
        return conn;
    }
}