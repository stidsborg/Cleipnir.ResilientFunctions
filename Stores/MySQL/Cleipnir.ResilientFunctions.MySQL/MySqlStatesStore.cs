﻿using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Storage.Utils;
using MySqlConnector;

namespace Cleipnir.ResilientFunctions.MySQL;

public class MySqlStatesStore : IStatesStore
{
    private readonly string _connectionString;
    private readonly string _tablePrefix;

    public MySqlStatesStore(string connectionString, string tablePrefix = "")
    {
        _connectionString = connectionString;
        _tablePrefix = tablePrefix;
    }

    private string? _initialize;
    public async Task Initialize()
    {
        await using var conn = await CreateConnection();
        _initialize ??= @$"
            CREATE TABLE IF NOT EXISTS {_tablePrefix}_states (
                id VARCHAR(450) PRIMARY KEY,
                state TEXT NOT NULL
            );";
        var command = new MySqlCommand(_initialize, conn);
        await command.ExecuteNonQueryAsync();
    }

    private string? _truncateSql;
    public async Task Truncate()
    {
        await using var conn = await CreateConnection();
        _truncateSql ??= $"TRUNCATE TABLE {_tablePrefix}_states";
        var command = new MySqlCommand(_truncateSql, conn);
        await command.ExecuteNonQueryAsync();
    }

    private string? _upsertStateSql;
    public async Task UpsertState(FlowId flowId, StoredState storedState)
    {
        var (flowType, flowInstance) = flowId;
        await using var conn = await CreateConnection();
        _upsertStateSql ??= $@"
          INSERT INTO {_tablePrefix}_states 
              (id, state)
          VALUES
              (?, ?)  
           ON DUPLICATE KEY UPDATE
                state = VALUES(state)";
        
        await using var command = new MySqlCommand(_upsertStateSql, conn)
        {
            Parameters =
            {
                new() {Value = Escaper.Escape(flowType.Value, flowInstance.Value, storedState.StateId.Value)},
                new() {Value = storedState.StateJson},
            }
        };

        await command.ExecuteNonQueryAsync();
    }

    private string? _getStatesSql;
    public async Task<IEnumerable<StoredState>> GetStates(FlowId flowId)
    {
        await using var conn = await CreateConnection();
        _getStatesSql ??= @$"
            SELECT id, state
            FROM {_tablePrefix}_states
            WHERE id LIKE ?";
        await using var command = new MySqlCommand(_getStatesSql, conn)
        {
            Parameters =
            {
                new() {Value = Escaper.Escape(flowId.Type.Value, flowId.Instance.Value) + "%"},
            }
        };

        await using var reader = await command.ExecuteReaderAsync();

        var states = new List<StoredState>();
        while (await reader.ReadAsync())
        {
            var id = reader.GetString(0);
            var stateId = Escaper.Unescape(id)[2];
            var json = reader.GetString(1);
            states.Add(new StoredState(stateId, json));
        }

        return states;
    }

    private string? _removeStatesSql;
    public async Task RemoveState(FlowId flowId, StateId stateId)
    {
        await using var conn = await CreateConnection();
        _removeStatesSql ??= $"DELETE FROM {_tablePrefix}_states WHERE id = ?";
        var id = Escaper.Escape(flowId.Type.Value, flowId.Instance.Value, stateId.Value);
        await using var command = new MySqlCommand(_removeStatesSql, conn)
        {
            Parameters = { new() { Value = id } }
        };

        await command.ExecuteNonQueryAsync();
    }

    private string? _removeSql;
    public async Task Remove(FlowId flowId)
    {
        await using var conn = await CreateConnection();
        _removeSql ??= $"DELETE FROM {_tablePrefix}_states WHERE id LIKE ?";
        var id = Escaper.Escape(flowId.Type.Value, flowId.Instance.Value) + $"{Escaper.Separator}%";
        await using var command = new MySqlCommand(_removeSql, conn)
        {
            Parameters = { new() { Value = id } }
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