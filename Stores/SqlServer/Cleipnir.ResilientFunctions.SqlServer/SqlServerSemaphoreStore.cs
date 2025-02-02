using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Storage;
using Microsoft.Data.SqlClient;

namespace Cleipnir.ResilientFunctions.SqlServer;

public class SqlServerSemaphoreStore(string connectionString, string tablePrefix = "") : ISemaphoreStore
{
    private string? _initializeSql;
    public async Task Initialize()
    {
        await using var conn = await CreateConnection();

        _initializeSql ??= @$"
        CREATE TABLE {tablePrefix}_Semaphores (
            Type NVARCHAR(150),
            Instance NVARCHAR(150),
            Position INT NOT NULL,
            Owner NVARCHAR(MAX),                     
            PRIMARY KEY (Type, Instance, Position)
        );";
        var command = new SqlCommand(_initializeSql, conn);
        try
        {
            await command.ExecuteNonQueryAsync();    
        } catch (SqlException exception) when (exception.Number == 2714) {}
    }
    
    private string? _truncateSql;
    public async Task Truncate()
    {
        await using var conn = await CreateConnection();
        _truncateSql ??= $"TRUNCATE TABLE {tablePrefix}_Semaphores;";
        var command = new SqlCommand(_truncateSql, conn);
        await command.ExecuteNonQueryAsync();
    }

    public async Task<bool> Acquire(string group, string instance, StoredId storedId, int maximumCount)
        => await Acquire(group, instance, storedId, maximumCount, depth: 0);
    
    private string? _acquireSql;
    private async Task<bool> Acquire(string group, string instance, StoredId storedId, int maximumCount, int depth)
    {
        await using var conn = await CreateConnection();
        
        _acquireSql ??= @$"
            INSERT INTO {tablePrefix}_Semaphores           
            SELECT @Type, 
                   @Instance, 
                   (SELECT COALESCE(MAX(Position), -1) + 1 
                    FROM {tablePrefix}_Semaphores 
                    WHERE Type = @Type AND Instance = @Instance), 
                   @Owner
            WHERE NOT EXISTS (
                SELECT 1
                FROM {tablePrefix}_Semaphores
                WHERE Type = @Type AND Instance = @Instance AND Owner = @Owner
            );

            SELECT Owner FROM {tablePrefix}_Semaphores WHERE Type = @Type AND Instance = @Instance ORDER BY Position";
        
        try
        {
            var command = new SqlCommand(_acquireSql, conn);
            command.Parameters.AddWithValue("@Type", group);
            command.Parameters.AddWithValue("@Instance", instance);
            command.Parameters.AddWithValue("@Owner", storedId.Serialize());

            await using var reader = await command.ExecuteReaderAsync();
            //var hasNextResult = await reader.NextResultAsync();

            var i = 0;
            while (await reader.ReadAsync())
            {
                var owner = StoredId.Deserialize(reader.GetString(0));
                if (owner == storedId)
                    return i < maximumCount;
                
                i++;   
            }

            return false;
        }
        catch (SqlException e)
        {
            if (depth == 10 || (e.Number != SqlError.DEADLOCK_VICTIM && e.Number != SqlError.UNIQUENESS_VIOLATION)) 
                throw;
            
            // ReSharper disable once DisposeOnUsingVariable
            await conn.DisposeAsync();
            await Task.Delay(Random.Shared.Next(50, 250));
            return await Acquire(group, instance, storedId, maximumCount, depth + 1); 
        }
    }

    private string? _releaseSql;
    public async Task<IReadOnlyList<StoredId>> Release(string group, string instance, StoredId storedId, int maximumCount)
    {
        await using var conn = await CreateConnection();
        
        _releaseSql ??= @$"    
            DELETE FROM {tablePrefix}_Semaphores
            WHERE Type = @Type AND Instance = @Instance AND Owner = @Owner;

            SELECT TOP(@Limit) Owner 
            FROM {tablePrefix}_Semaphores
            WHERE Type = @Type AND Instance = @Instance
            ORDER BY Position;";
        
        var command = new SqlCommand(_releaseSql, conn);
        command.Parameters.AddWithValue("@Type", group);
        command.Parameters.AddWithValue("@Instance", instance);
        command.Parameters.AddWithValue("@Owner", storedId.Serialize());
        command.Parameters.AddWithValue("@Limit", maximumCount);

        await using var reader = await command.ExecuteReaderAsync();
        var ids = new List<StoredId>();
        while (await reader.ReadAsync())
            ids.Add(StoredId.Deserialize(reader.GetString(0)));

        return ids;
    }
    
    private string? _queuedSql;
    public async Task<IReadOnlyList<StoredId>> GetQueued(string group, string instance, int count)
    {
        await using var conn = await CreateConnection();
        _queuedSql ??= @$"    
            SELECT TOP(@Limit) Owner 
            FROM {tablePrefix}_Semaphores
            WHERE Type = @Type AND Instance = @Instance
            ORDER BY Position;
            ";
        var command = new SqlCommand(_queuedSql, conn);
        command.Parameters.AddWithValue("@Type", group);
        command.Parameters.AddWithValue("@Instance", instance);
        command.Parameters.AddWithValue("@Limit", count);

        await using var reader = await command.ExecuteReaderAsync();
        var ids = new List<StoredId>();
        while (await reader.ReadAsync())
            ids.Add(StoredId.Deserialize(reader.GetString(0)));

        return ids;
    }

    private async Task<SqlConnection> CreateConnection()
    {
        var conn = new SqlConnection(connectionString);
        await conn.OpenAsync();
        return conn;
    }
}