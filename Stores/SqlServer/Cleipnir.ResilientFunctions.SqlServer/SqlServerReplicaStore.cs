using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Storage;
using Microsoft.Data.SqlClient;

namespace Cleipnir.ResilientFunctions.SqlServer;

public class SqlServerReplicaStore(string connectionString, string tablePrefix) : IReplicaStore
{
    private string? _initializeSql;
    public async Task Initialize()
    {
        _initializeSql ??= $@"
            CREATE TABLE {tablePrefix}Replicas (
                Id CHAR(32) PRIMARY KEY,
                Heartbeat INT
            );";
        await using var conn = await CreateConnection();
        var command = new SqlCommand(_initializeSql, conn);
        await command.ExecuteNonQueryAsync();
    }

    private string? _insertSql;
    public async Task Insert(ReplicaId replicaId)
    {
        _insertSql ??= $@"
            INSERT INTO {tablePrefix}Replicas
                (Id, Heartbeat)
            VALUES
                (@Id, 0)";
        
        await using var conn = await CreateConnection();
        await using var command = new SqlCommand(_insertSql, conn)
        {
            Parameters =
            {
                new() {ParameterName = "Id", Value = replicaId.AsGuid.ToString("N")}
            }
        };

        await command.ExecuteNonQueryAsync();
    }

    private string? _deleteSql;
    public async Task Delete(ReplicaId replicaId)
    {
        _deleteSql ??= $"DELETE FROM {tablePrefix}Replicas WHERE Id = @Id";
        
        await using var conn = await CreateConnection();
        await using var command = new SqlCommand(_deleteSql, conn)
        {
            Parameters =
            {
                new() {ParameterName = "Id", Value = replicaId.AsGuid.ToString("N")}
            }
        };

        await command.ExecuteNonQueryAsync();
    }

    private string? _updateHeartbeatSql;
    public async Task UpdateHeartbeat(ReplicaId replicaId)
    {
        _updateHeartbeatSql ??= $@"
            UPDATE {tablePrefix}Replicas
            SET heartbeat = heartbeat + 1
            WHERE id = @Id";
        
        await using var conn = await CreateConnection();
        await using var command = new SqlCommand(_updateHeartbeatSql, conn)
        {
            Parameters =
            {
                new() {ParameterName = "Id", Value = replicaId.AsGuid.ToString("N")}
            }
        };

        await command.ExecuteNonQueryAsync();
    }

    private string? _getAllSql;
    public async Task<IReadOnlyList<StoredReplica>> GetAll()
    {
        _getAllSql ??= $"SELECT Id, Heartbeat FROM {tablePrefix}Replicas";
        
        await using var conn = await CreateConnection();
        await using var command = new SqlCommand(_getAllSql, conn);
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
        _truncateSql ??= $"TRUNCATE TABLE {tablePrefix}Replicas";

        await using var conn = await CreateConnection();
        await using var command = new SqlCommand(_truncateSql, conn);

        await command.ExecuteNonQueryAsync();
    }
    
    private async Task<SqlConnection> CreateConnection()
    {
        var conn = new SqlConnection(connectionString);
        await conn.OpenAsync();
        return conn;
    }
}