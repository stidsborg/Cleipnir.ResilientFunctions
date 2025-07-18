using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Storage;
using Npgsql;

namespace Cleipnir.ResilientFunctions.PostgreSQL;

public class PostgreSqlDbReplicaStore(string connectionString, string tablePrefix) : IReplicaStore
{
    private string? _initializeSql;
    public async Task Initialize()
    {
        _initializeSql ??= $@"
            CREATE TABLE IF NOT EXISTS {tablePrefix}_replicas (
                id CHAR(32) PRIMARY KEY,
                heartbeat INT
            );";
        await using var conn = await CreateConnection();
        var command = new NpgsqlCommand(_initializeSql, conn);
        await command.ExecuteNonQueryAsync();
    }

    private string? _insertSql;
    public async Task Insert(ReplicaId replicaId)
    {
        _insertSql ??= $@"
            INSERT INTO {tablePrefix}_replicas
                (id, heartbeat)
            VALUES
                ($1, 0)";
        
        await using var conn = await CreateConnection();
        await using var command = new NpgsqlCommand(_insertSql, conn)
        {
            Parameters =
            {
                new() {Value = replicaId.AsGuid.ToString("N")}
            }
        };

        await command.ExecuteNonQueryAsync();
    }

    private string? _deleteSql;
    public async Task Delete(ReplicaId replicaId)
    {
        _deleteSql ??= $"DELETE FROM {tablePrefix}_replicas WHERE id = $1";
        
        await using var conn = await CreateConnection();
        await using var command = new NpgsqlCommand(_deleteSql, conn)
        {
            Parameters =
            {
                new() {Value = replicaId.AsGuid.ToString("N")}
            }
        };

        await command.ExecuteNonQueryAsync();
    }

    private string? _updateHeartbeatSql;
    public async Task UpdateHeartbeat(ReplicaId replicaId)
    {
        _updateHeartbeatSql ??= $@"
            UPDATE {tablePrefix}_replicas
            SET heartbeat = heartbeat + 1
            WHERE id = $1";
        
        await using var conn = await CreateConnection();
        await using var command = new NpgsqlCommand(_updateHeartbeatSql, conn)
        {
            Parameters =
            {
                new() {Value = replicaId.AsGuid.ToString("N")}
            }
        };

        await command.ExecuteNonQueryAsync();
    }

    private string? _getAllSql;
    public async Task<IReadOnlyList<StoredReplica>> GetAll()
    {
        _getAllSql ??= $"SELECT id, heartbeat FROM {tablePrefix}_replicas";
        
        await using var conn = await CreateConnection();
        await using var command = new NpgsqlCommand(_getAllSql, conn);
        var storedReplicas = new List<StoredReplica>();

        await using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            var id = Guid.Parse(reader.GetString(0));
            var heartbeat = reader.GetInt32(1);
            storedReplicas.Add(new StoredReplica(id.ToReplicaId(), heartbeat));
        }

        return storedReplicas;
    }

    private string? _truncateSql;
    public async Task Truncate()
    {
        _truncateSql ??= $"TRUNCATE TABLE {tablePrefix}_replicas";

        await using var conn = await CreateConnection();
        await using var command = new NpgsqlCommand(_truncateSql, conn);

        await command.ExecuteNonQueryAsync();
    }
    
    private async Task<NpgsqlConnection> CreateConnection()
    {
        var conn = new NpgsqlConnection(connectionString);
        await conn.OpenAsync();
        return conn;
    }
}