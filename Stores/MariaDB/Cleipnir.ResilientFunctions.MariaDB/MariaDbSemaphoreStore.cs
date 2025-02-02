using Cleipnir.ResilientFunctions.Storage;
using MySqlConnector;

namespace Cleipnir.ResilientFunctions.MariaDb;

public class MariaDbSemaphoreStore(string connectionString, string tablePrefix = "") : ISemaphoreStore
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
        
        var command = new MySqlCommand(_initializeSql, conn);
        await command.ExecuteNonQueryAsync();
    }
    
    private string? _truncateSql;
    public async Task Truncate()
    {
        await using var conn = await CreateConnection();
        _truncateSql ??= $"TRUNCATE TABLE {tablePrefix}_semaphores;";
        var command = new MySqlCommand(_truncateSql, conn);
        await command.ExecuteNonQueryAsync();
    }
    
    public async Task<bool> Acquire(string group, string instance, StoredId storedId, int maximumCount)
        => await Acquire(group, instance, storedId, maximumCount, depth: 0);
    
    private string? _takeSql;
    private async Task<bool> Acquire(string group, string instance, StoredId storedId, int maximumCount, int depth)
    {
        _takeSql ??= @$"
            IF (NOT EXISTS (SELECT 1 FROM {tablePrefix}_semaphores WHERE type = ? AND instance = ? AND owner = ?))
            THEN
                INSERT INTO {tablePrefix}_semaphores
                (type, instance, position, owner)
                SELECT ?, ?, (COALESCE(MAX(position), -1) + 1), ?
                FROM {tablePrefix}_semaphores;            
            END IF;

            SELECT owner FROM {tablePrefix}_semaphores WHERE type = ? AND instance = ? ORDER BY position;";

        await using var conn = await CreateConnection();
        try
        {
            var command = new MySqlCommand(_takeSql, conn)
            {
                Parameters =
                {
                    new() { Value = group },
                    new() { Value = instance },
                    new() { Value = storedId.Serialize() },

                    new() { Value = group },
                    new() { Value = instance },
                    new() { Value = storedId.Serialize() },
                    
                    new() { Value = group },
                    new() { Value = instance },
                    new() { Value = storedId.Serialize() }
                }
            };

            await using var reader = await command.ExecuteReaderAsync();
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
        catch (MySqlException e) when (e.Number == 1213) //deadlock found when trying to get lock; try restarting transaction
        {
            // ReSharper disable once DisposeOnUsingVariable
            await conn.DisposeAsync(); //eagerly free taken connection
            if (depth == 10) throw;

            await Task.Delay(Random.Shared.Next(10, 250));
            return await Acquire(group, instance, storedId, maximumCount, depth + 1);
        }
    }

    private string? _releaseSql;
    public async Task<IReadOnlyList<StoredId>> Release(string group, string instance, StoredId storedId, int maximumCount)
    {
        await using var conn = await CreateConnection();
        
        _releaseSql ??= @$"    
           DELETE FROM {tablePrefix}_semaphores
           WHERE type = ? AND instance = ? AND owner = ?;               

           SELECT owner 
           FROM {tablePrefix}_semaphores
           WHERE type = ? AND instance = ?
           ORDER BY position
           LIMIT ?;";
        
        await using var command = new MySqlCommand(_releaseSql, conn)
        {
            Parameters =
            {
                new() { Value = group },
                new() { Value = instance },
                new() { Value = storedId.Serialize() },
                
                new() { Value = group },
                new() { Value = instance },
                new() { Value = maximumCount },
            }
        };

        await using var reader = await command.ExecuteReaderAsync();
        var ids = new List<StoredId>();
        while (await reader.ReadAsync())
            ids.Add(StoredId.Deserialize(reader.GetString(0)));

        return ids;
    }

    private string? _getQueuedSql;
    public async Task<IReadOnlyList<StoredId>> GetQueued(string group, string instance, int count)
    {
        await using var conn = await CreateConnection();
        
        _getQueuedSql ??= @$"               
            SELECT owner 
            FROM {tablePrefix}_semaphores
            WHERE type = ? AND instance = ?
            ORDER BY position
            LIMIT ?;
            ";
        var command = new MySqlCommand(_getQueuedSql, conn)
        {
            Parameters =
            {
                new() { Value = group },
                new() { Value = instance },
                new() { Value = count },
            }
        };

        await using var reader = await command.ExecuteReaderAsync();
        var ids = new List<StoredId>();
        while (await reader.ReadAsync())
            ids.Add(StoredId.Deserialize(reader.GetString(0)));

        return ids;
    }

    private async Task<MySqlConnection> CreateConnection()
    {
        var conn = new MySqlConnection(connectionString);
        await conn.OpenAsync();
        return conn;
    }
}