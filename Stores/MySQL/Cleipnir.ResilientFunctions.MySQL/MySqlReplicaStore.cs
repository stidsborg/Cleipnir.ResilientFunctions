using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Storage;
using MySqlConnector;

namespace Cleipnir.ResilientFunctions.MySQL;

public class MySqlReplicaStore : IReplicaStore
{
    private readonly string _connectionString;
    private readonly string _tablePrefix;

    public MySqlReplicaStore(string connectionString, string tablePrefix = "")
    {
        _connectionString = connectionString;
        _tablePrefix = tablePrefix;
    }

    private string? _initialize;
    public async Task Initialize()
    {
        await using var conn = await CreateConnection();
        _initialize ??= @$"
            CREATE TABLE IF NOT EXISTS {_tablePrefix}_replicas (
                replica_id CHAR(32) PRIMARY KEY,
                ttl BIGINT NOT NULL               
            );";
        var command = new MySqlCommand(_initialize, conn);
        await command.ExecuteNonQueryAsync();
    }

    private string? _truncateSql;
    public async Task Truncate()
    {
        await using var conn = await CreateConnection();
        _truncateSql ??= $"TRUNCATE TABLE {_tablePrefix}_replicas";
        var command = new MySqlCommand(_truncateSql, conn);
        await command.ExecuteNonQueryAsync();
    }

    public async Task Insert(Guid replicaId, long ttl)
    {
        await using var conn = await CreateConnection();
        var sql= $"INSERT INTO {_tablePrefix}_replicas VALUES ('{replicaId:N}', {ttl})";

        await using var command = new MySqlCommand(sql, conn);
        await command.ExecuteNonQueryAsync();
    }

    public async Task Update(Guid replicaId, long ttl)
    {
        await using var conn = await CreateConnection();
        var sql= $"UPDATE {_tablePrefix}_replicas SET ttl = {ttl} WHERE replica_id = '{replicaId:N}'";

        await using var command = new MySqlCommand(sql, conn);
        await command.ExecuteNonQueryAsync();
    }

    public async Task Delete(Guid replicaId)
    {
        await using var conn = await CreateConnection();
        var sql= $"DELETE FROM {_tablePrefix}_replicas WHERE replica_id = '{replicaId:N}'";

        await using var command = new MySqlCommand(sql, conn);
        await command.ExecuteNonQueryAsync();
    }

    public async Task Prune(long currentTime)
    {
        await using var conn = await CreateConnection();
        var sql= $"DELETE FROM {_tablePrefix}_replicas WHERE ttl < {currentTime}";

        await using var command = new MySqlCommand(sql, conn);
        await command.ExecuteNonQueryAsync();
    }

    public async Task<int> GetReplicaCount()
    {
        await using var conn = await CreateConnection();
        var sql= $"SELECT COUNT(*) FROM {_tablePrefix}_replicas";

        await using var command = new MySqlCommand(sql, conn);
        return ((int)(long)(await command.ExecuteScalarAsync() ?? 0));
    }
    
    private async Task<MySqlConnection> CreateConnection()
    {
        var conn = new MySqlConnection(_connectionString);
        await conn.OpenAsync();
        return conn;
    }
}