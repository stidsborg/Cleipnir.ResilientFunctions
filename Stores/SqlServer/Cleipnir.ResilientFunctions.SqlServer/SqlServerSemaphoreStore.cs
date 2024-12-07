using System.Collections.Generic;
using System.Linq;
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
    
    private string? _takeSql;
    public async Task<bool> Acquire(string group, string instance, StoredId storedId, int semaphoreCount)
    {
        await using var conn = await CreateConnection();
        
        _takeSql ??= @$"
            INSERT INTO {tablePrefix}_Semaphores
            OUTPUT INSERTED.Position
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
            );";
        var command = new SqlCommand(_takeSql, conn);
        command.Parameters.AddWithValue("@Type", group);
        command.Parameters.AddWithValue("@Instance", instance);
        command.Parameters.AddWithValue("@Owner", storedId.Serialize());

        var position = await command.ExecuteScalarAsync();
        
        if (position is null) 
            return (await GetQueued(group, instance, semaphoreCount)).Any(id => id == storedId);

        return (int) position < semaphoreCount;
    }

    private string? _releaseSql;
    public async Task<IReadOnlyList<StoredId>> Release(string group, string instance, StoredId storedId, int semaphoreCount)
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
        command.Parameters.AddWithValue("@Limit", semaphoreCount);

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