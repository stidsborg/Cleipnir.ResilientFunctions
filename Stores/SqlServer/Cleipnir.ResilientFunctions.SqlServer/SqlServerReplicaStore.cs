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
                Id UNIQUEIDENTIFIER PRIMARY KEY,
                Heartbeat BIGINT
            );";
        await using var conn = await CreateConnection();
        var command = new SqlCommand(_initializeSql, conn);
        await command.ExecuteNonQueryAsync();
    }

    private string? _insertSql;
    public async Task Insert(ReplicaId replicaId, long timestamp)
    {
        _insertSql ??= $@"
            INSERT INTO {tablePrefix}Replicas
                (Id, Heartbeat)
            VALUES
                (@Id, @Timestamp)";
        
        await using var conn = await CreateConnection();
        await using var command = new SqlCommand(_insertSql, conn)
        {
            Parameters =
            {
                new() {ParameterName = "Id", Value = replicaId.AsGuid},
                new() {ParameterName = "Timestamp", Value = timestamp}
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
                new() {ParameterName = "Id", Value = replicaId.AsGuid}
            }
        };

        await command.ExecuteNonQueryAsync();
    }

    private string? _updateHeartbeatSql;
    public async Task<bool> UpdateHeartbeat(ReplicaId replicaId, long timeStamp)
    {
        _updateHeartbeatSql ??= $@"
            UPDATE {tablePrefix}Replicas
            SET heartbeat = @Timestamp
            WHERE id = @Id";
        
        await using var conn = await CreateConnection();
        await using var command = new SqlCommand(_updateHeartbeatSql, conn)
        {
            Parameters =
            {
                new() {ParameterName = "Id", Value = replicaId.AsGuid},
                new() {ParameterName = "Timestamp", Value = timeStamp}
            }
        };

        var affectedRows = await command.ExecuteNonQueryAsync();
        return affectedRows > 0;
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
            var id = reader.GetGuid(0);
            var heartbeat = reader.GetInt64(1);
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