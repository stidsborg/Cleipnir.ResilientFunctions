using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Storage;
using Npgsql;

namespace Cleipnir.ResilientFunctions.PostgreSQL;

public class PostgreSqlTypeStore(string connectionString, string tablePrefix = "") : ITypeStore
{
    private readonly string _tablePrefix = tablePrefix.ToLower();

    private string? _initializeSql;
    public async Task Initialize()
    {
        await using var conn = await CreateConnection();
        _initializeSql ??= @$"
            CREATE TABLE IF NOT EXISTS {_tablePrefix}_types (
                type VARCHAR(255) PRIMARY KEY,
                ref INT GENERATED ALWAYS AS IDENTITY
            );";
        var command = new NpgsqlCommand(_initializeSql, conn);
        await command.ExecuteNonQueryAsync();
    }

    private string? _truncateSql;
    public async Task Truncate()
    {
        await using var conn = await CreateConnection();
        _truncateSql ??= $"TRUNCATE TABLE {_tablePrefix}_types";
        var command = new NpgsqlCommand(_truncateSql, conn);
        await command.ExecuteNonQueryAsync();
    }
    
    private async Task<NpgsqlConnection> CreateConnection()
    {
        var conn = new NpgsqlConnection(connectionString);
        await conn.OpenAsync();
        return conn;
    }
    
    public async Task<int> InsertOrGetFlowType(FlowType flowType)
    {
        await using var conn = await CreateConnection();
     
        var sql = @$"
            INSERT INTO {_tablePrefix}_types 
                (type)
            VALUES
                ($1) 
            ON CONFLICT DO NOTHING";
        
        await using var command = new NpgsqlCommand(sql, conn)
        {
            Parameters =
            {
                new() { Value = flowType.Value }
            }
        };

        await command.ExecuteNonQueryAsync();

        return await GetRef(flowType);
    }
    
    private async Task<int> GetRef(FlowType flowType)
    {
        await using var conn = await CreateConnection();
        var sql = @$"    
            SELECT ref
            FROM {_tablePrefix}_types
            WHERE type = $1";
        
        await using var command = new NpgsqlCommand(sql, conn)
        {
            Parameters =
            {
                new() { Value = flowType.Value }
            }
        };
        
        var value = (int?) await command.ExecuteScalarAsync();
        if (!value.HasValue)
            throw new InvalidOperationException($"Unexpected missing reference for type: '{flowType.Value}'");

        return value.Value;
    } 

    public async Task<IReadOnlyDictionary<FlowType, int>> GetAllFlowTypes()
    {
        await using var conn = await CreateConnection();
        var sql = $"SELECT type, ref FROM {_tablePrefix}_types";

        await using var command = new NpgsqlCommand(sql, conn);
        var dict = new Dictionary<FlowType, int>();
        await using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            var flowType = reader.GetString(0);
            var value = reader.GetInt32(1);
            dict[flowType] = value;
        }

        return dict;
    }
}