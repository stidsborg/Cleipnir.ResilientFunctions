using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Storage;
using Microsoft.Data.SqlClient;

namespace Cleipnir.ResilientFunctions.SqlServer;

public class SqlServerTypeStore(string connectionString, string tablePrefix = "") : ITypeStore
{
    public async Task Initialize()
    {
        await using var conn = await CreateConnection();
        
        var sql = @$"            
            CREATE TABLE {tablePrefix}_Types (
                Type NVARCHAR(255) PRIMARY KEY,
                Ref INT IDENTITY(1, 1)                                            
            );";
        var command = new SqlCommand(sql, conn);
        try
        {
            await command.ExecuteNonQueryAsync();    
        } catch (SqlException exception) when (exception.Number == 2714) {}
    }

    public async Task Truncate()
    {
        await using var conn = await CreateConnection();
        var sql = $"TRUNCATE TABLE {tablePrefix}_Types";
        var command = new SqlCommand(sql, conn);
        await command.ExecuteNonQueryAsync();
    }
    
    public async Task<int> InsertOrGetFlowType(FlowType flowType)
    {
        await using var conn = await CreateConnection();
        var sql = @$"
                    BEGIN
                        IF NOT EXISTS (SELECT * FROM {tablePrefix}_Types WHERE Type = @Type) 
                        BEGIN
                            INSERT INTO {tablePrefix}_Types VALUES (@Type)
                        END
                    END";
        
        await using var command = new SqlCommand(sql, conn);
        command.Parameters.AddWithValue("@Type", flowType.Value);
        await command.ExecuteNonQueryAsync();

        return await GetRef(flowType);
    }
    
    private async Task<int> GetRef(FlowType flowType)
    {
        await using var conn = await CreateConnection();
        var sql = @$"    
            SELECT Ref
            FROM {tablePrefix}_types
            WHERE type = @FlowType";

        await using var command = new SqlCommand(sql, conn);
        command.Parameters.AddWithValue("@FlowType", flowType.Value);
        
        var value = (int?) await command.ExecuteScalarAsync();
        if (!value.HasValue)
            throw new InvalidOperationException($"Unexpected missing reference for type: '{flowType.Value}'");

        return value.Value;
    } 

    public async Task<IReadOnlyDictionary<FlowType, int>> GetAllFlowTypes()
    {
        await using var conn = await CreateConnection();
        var sql = @$"    
            SELECT Ref, Type
            FROM {tablePrefix}_Types";
        
        await using var command = new SqlCommand(sql, conn);
        var dict = new Dictionary<FlowType, int>();
        
        await using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            var value = reader.GetInt32(0); 
            var flowType = new FlowType(reader.GetString(1));
            dict.TryAdd(flowType, value);
        }

        return dict;
    }
    
    private async Task<SqlConnection> CreateConnection()
    {
        var conn = new SqlConnection(connectionString);
        await conn.OpenAsync();
        return conn;
    }
}