﻿using System.Collections.Generic;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Storage.Utils;
using Npgsql;

namespace Cleipnir.ResilientFunctions.PostgreSQL;

public class PostgreSqlStatesStore(string connectionString, string tablePrefix = "") : IStatesStore
{
    private string? _initializeSql;
    public async Task Initialize()
    {
        await using var conn = await CreateConnection();
        _initializeSql ??= @$"
            CREATE TABLE IF NOT EXISTS {tablePrefix}_states (
                id VARCHAR(450) PRIMARY KEY,
                state TEXT NOT NULL
            );";
        var command = new NpgsqlCommand(_initializeSql, conn);
        await command.ExecuteNonQueryAsync();
    }

    private string? _truncateSql;
    public async Task Truncate()
    {
        await using var conn = await CreateConnection();
        _truncateSql ??= $"TRUNCATE TABLE {tablePrefix}_states";
        var command = new NpgsqlCommand(_truncateSql, conn);
        await command.ExecuteNonQueryAsync();
    }

    private string? _upsertStateSql;
    public async Task UpsertState(FlowId flowId, StoredState storedState)
    {
        var (flowType, flowInstance) = flowId;
        
        await using var conn = await CreateConnection();
        _upsertStateSql ??= $@"
          INSERT INTO {tablePrefix}_states 
              (id, state)
          VALUES
              ($1, $2) 
          ON CONFLICT (id) 
          DO 
            UPDATE SET state = EXCLUDED.state";
        
        await using var command = new NpgsqlCommand(_upsertStateSql, conn)
        {
            Parameters =
            {
                new() {Value = Escaper.Escape(flowType.Value, flowInstance.Value, storedState.StateId.Value)},
                new() {Value = storedState.StateJson}
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
            FROM {tablePrefix}_states
            WHERE id LIKE $1";
        await using var command = new NpgsqlCommand(_getStatesSql, conn)
        {
            Parameters =
            {
                new() { Value = Escaper.Escape(flowId.Type.Value, flowId.Instance.Value) + "%" }
            }
        };

        await using var reader = await command.ExecuteReaderAsync();

        var functions = new List<StoredState>();
        while (await reader.ReadAsync())
        {
            var id = reader.GetString(0);
            var stateId = Escaper.Unescape(id)[2];
            var state = reader.GetString(1);
            functions.Add(new StoredState(stateId, state));
        }

        return functions;
    }

    private string? _removeStateSql;
    public async Task RemoveState(FlowId flowId, StateId stateId)
    {
        await using var conn = await CreateConnection();
        _removeStateSql ??= $"DELETE FROM {tablePrefix}_states WHERE id = $1";
        
        var id = Escaper.Escape(flowId.Type.Value, flowId.Instance.Value, stateId.Value);
        await using var command = new NpgsqlCommand(_removeStateSql, conn)
        {
            Parameters = { new() {Value = id } }
        };

        await command.ExecuteNonQueryAsync();
    }

    private string? _removeSql;
    public async Task Remove(FlowId flowId)
    {
        await using var conn = await CreateConnection();
        _removeSql ??= $"DELETE FROM {tablePrefix}_states WHERE id LIKE $1";
        
        var idPrefix = Escaper.Escape(flowId.Type.Value, flowId.Instance.Value) + $"{Escaper.Separator}%";
        await using var command = new NpgsqlCommand(_removeSql, conn)
        {
            Parameters = { new() {Value = idPrefix } }
        };

        await command.ExecuteNonQueryAsync();
    }

    private async Task<NpgsqlConnection> CreateConnection()
    {
        var conn = new NpgsqlConnection(connectionString);
        await conn.OpenAsync();
        return conn;
    }
}