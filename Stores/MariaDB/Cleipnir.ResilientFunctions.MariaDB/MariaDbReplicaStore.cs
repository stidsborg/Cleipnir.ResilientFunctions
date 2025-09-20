using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Storage;
using MySqlConnector;

namespace Cleipnir.ResilientFunctions.MariaDb;

public class MariaDbReplicaStore(string connectionString, string tablePrefix) : IReplicaStore
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
        var command = new MySqlCommand(_initializeSql, conn);
        await command.ExecuteNonQueryAsync();
    }
    
    private string? _insertSql;
    public async Task Insert(ReplicaId replicaId, long timeStamp)
    {
        _insertSql ??= $@"
            INSERT INTO {tablePrefix}_replicas
                (id, heartbeat)
            VALUES
                (?, ?)";
        
        await using var conn = await CreateConnection();
        await using var command = new MySqlCommand(_insertSql, conn)
        {
            Parameters =
            {
                new() {Value = replicaId.AsGuid.ToString("N")},
                new() {Value = timeStamp},
            }
        };

        await command.ExecuteNonQueryAsync();
    }

    private string? _deleteSql;
    public async Task Delete(ReplicaId replicaId)
    {
        _deleteSql ??= $"DELETE FROM {tablePrefix}_replicas WHERE id = ?";
        
        await using var conn = await CreateConnection();
        await using var command = new MySqlCommand(_deleteSql, conn)
        {
            Parameters =
            {
                new() {Value = replicaId.AsGuid.ToString("N")}
            }
        };

        await command.ExecuteNonQueryAsync();
    }
    
    private string? _updateHeartbeatSql;
    public async Task<bool> UpdateHeartbeat(ReplicaId replicaId, long timeStamp)
    {
        _updateHeartbeatSql ??= $@"
            UPDATE {tablePrefix}_replicas
            SET heartbeat = ?
            WHERE id = ?";
        
        await using var conn = await CreateConnection();
        await using var command = new MySqlCommand(_updateHeartbeatSql, conn)
        {
            Parameters =
            {
                new() {Value = timeStamp},
                new() {Value = replicaId.AsGuid.ToString("N")}
            }
        };

        var affectedRows = await command.ExecuteNonQueryAsync();
        return affectedRows > 0;
    }

    private string? _getAllSql;
    public async Task<IReadOnlyList<StoredReplica>> GetAll()
    {
        _getAllSql ??= $"SELECT id, heartbeat FROM {tablePrefix}_replicas";
        
        await using var conn = await CreateConnection();
        await using var command = new MySqlCommand(_getAllSql, conn);
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
        await using var command = new MySqlCommand(_truncateSql, conn);

        await command.ExecuteNonQueryAsync();
    }
    
    private async Task<MySqlConnection> CreateConnection()
    {
        var conn = new MySqlConnection(connectionString);
        await conn.OpenAsync();
        return conn;
    }
}