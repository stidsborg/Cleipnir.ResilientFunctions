using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Storage;
using Npgsql;

namespace Cleipnir.ResilientFunctions.PostgreSQL;

public class PostgreSqlSemaphoreStore(string connectionString, string tablePrefix = "") : ISemaphoreStore
{
    private string? _initializeSql;
    public async Task Initialize()
    {
        await using var conn = await CreateConnection();
        _initializeSql ??= @$"
            CREATE TABLE IF NOT EXISTS {tablePrefix}_semaphores (
                type VARCHAR(150) NOT NULL,
                instance VARCHAR(150) NOT NULL,
                position INT NOT NULL,
                owner TEXT NOT NULL,                          
                PRIMARY KEY (type, instance, position)
            );";
        
        var command = new NpgsqlCommand(_initializeSql, conn);
        await command.ExecuteNonQueryAsync();
    }
    
    private string? _truncateSql;
    public async Task Truncate()
    {
        await using var conn = await CreateConnection();
        _truncateSql ??= $"TRUNCATE TABLE {tablePrefix}_semaphores;";
        var command = new NpgsqlCommand(_truncateSql, conn);
        await command.ExecuteNonQueryAsync();
    }

    public async Task<bool> Acquire(string group, string instance, StoredId storedId, int maximumCount)
        => await Acquire(group, instance, storedId, maximumCount, depth: 0);
    
    private string? _takeSql;
    private string? _readAfterTakeSql;
    private async Task<bool> Acquire(string group, string instance, StoredId storedId, int maximumCount, int depth)
    {
        _takeSql ??= @$"    
            INSERT INTO {tablePrefix}_semaphores
                SELECT 
                    $1, 
                    $2, 
                    (SELECT COALESCE(MAX(position), -1) + 1 FROM {tablePrefix}_semaphores WHERE type = $1 AND instance = $2),
                    $3
                 WHERE NOT EXISTS (SELECT 1 FROM {tablePrefix}_semaphores WHERE type = $1 AND instance = $2 AND owner = $3);";
        
        _readAfterTakeSql ??= $"SELECT owner FROM {tablePrefix}_semaphores WHERE type = $1 AND instance = $2 ORDER BY position;";
        
        await using var conn = await CreateConnection();
        try
        {
            await using var batch = new NpgsqlBatch(conn)
            {
                BatchCommands =
                {
                    new NpgsqlBatchCommand(_takeSql)
                    {
                        Parameters =
                        {
                            new() { Value = group },
                            new() { Value = instance },
                            new() { Value = storedId.Serialize() }
                        },
                    },
                    new NpgsqlBatchCommand(_readAfterTakeSql) {
                        Parameters =
                        {
                            new() { Value = group },
                            new() { Value = instance },
                            new() { Value = storedId.Serialize() }
                        },
                    },
                }
            };

            await using var reader = await batch.ExecuteReaderAsync();
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
        catch (PostgresException e) when (e.SqlState == "23505")
        {
            // ReSharper disable once DisposeOnUsingVariable
            await conn.DisposeAsync(); //eagerly free taken connection
            if (depth == 10) throw;
            
            await Task.Delay(Random.Shared.Next(10, 250));
            return await Acquire(group, instance, storedId, maximumCount, depth + 1);
        }
    }

    private string? _releaseSql;
    private string? _releaseGetQueuedSql;
    public async Task<IReadOnlyList<StoredId>> Release(string group, string instance, StoredId storedId, int maximumCount)
    {
        await using var conn = await CreateConnection();
        await using var batch = new NpgsqlBatch(conn);

        {
            _releaseSql ??= @$"    
            DELETE FROM {tablePrefix}_semaphores
            WHERE type = $1 AND instance = $2 AND owner = $3;";
            var command = new NpgsqlBatchCommand(_releaseSql)
            {
                Parameters =
                {
                    new() { Value = group },
                    new() { Value = instance },
                    new() { Value = storedId.Serialize() }
                }
            };
            batch.BatchCommands.Add(command);            
        }

        {
            _releaseGetQueuedSql ??= @$"    
            SELECT owner 
            FROM {tablePrefix}_semaphores
            WHERE type = $1 AND instance = $2
            ORDER BY position
            LIMIT $3;
            ";
            
            var command = new NpgsqlBatchCommand(_releaseGetQueuedSql)
            {
                Parameters =
                {
                    new() { Value = group },
                    new() { Value = instance },
                    new() { Value = maximumCount }
                }
            };
            batch.BatchCommands.Add(command);            
        }

        await using var reader = await batch.ExecuteReaderAsync();
        var ids = new List<StoredId>();
        while (await reader.ReadAsync())
            ids.Add(StoredId.Deserialize(reader.GetString(0)));

        return ids;
    }

    private string? _getQueued;
    public async Task<IReadOnlyList<StoredId>> GetQueued(string group, string instance, int count)
    {
        await using var conn = await CreateConnection();
        _getQueued ??= @$"    
            SELECT owner 
            FROM {tablePrefix}_semaphores
            WHERE type = $1 AND instance = $2
            ORDER BY position
            LIMIT $3;
            ";
        var command = new NpgsqlCommand(_getQueued, conn)
        {
            Parameters =
            {
                new() { Value = group },
                new() { Value = instance },
                new() { Value = count }
            }
        };
        
        await using var reader = await command.ExecuteReaderAsync();
        var ids = new List<StoredId>();
        while (await reader.ReadAsync())
            ids.Add(StoredId.Deserialize(reader.GetString(0)));

        return ids;
    }

    private async Task<NpgsqlConnection> CreateConnection()
    {
        var conn = new NpgsqlConnection(connectionString);
        await conn.OpenAsync();
        return conn;
    }
}