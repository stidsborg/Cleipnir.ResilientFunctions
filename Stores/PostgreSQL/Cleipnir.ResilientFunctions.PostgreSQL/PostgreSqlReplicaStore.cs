using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Storage;
using Npgsql;

namespace Cleipnir.ResilientFunctions.PostgreSQL;

public class PostgreSqlReplcaStore(string connectionString, string tablePrefix = "") : IReplicaStore
{
    private string? _initializeSql;
    public async Task Initialize()
    {
        await using var conn = await CreateConnection();
        _initializeSql ??= @$"
            CREATE TABLE IF NOT EXISTS {tablePrefix}_replicas (
                replica_id CHAR(32) PRIMARY KEY,
                ttl BIGINT               
            );";
        var command = new NpgsqlCommand(_initializeSql, conn);
        await command.ExecuteNonQueryAsync();
    }

    private string? _truncateSql;
    public async Task Truncate()
    {
        await using var conn = await CreateConnection();
        _truncateSql ??= $"TRUNCATE TABLE {tablePrefix}_replicas";
        var command = new NpgsqlCommand(_truncateSql, conn);
        await command.ExecuteNonQueryAsync();
    }

    public async Task Insert(Guid replicaId, long ttl)
    {
        await using var conn = await CreateConnection();
        var sql = $"INSERT INTO {tablePrefix}_replicas VALUES ('{replicaId:N}', {ttl})";

        await using var command = new NpgsqlCommand(sql, conn);
        await command.ExecuteNonQueryAsync();
    }

    public async Task Update(Guid replicaId, long ttl)
    {
        await using var conn = await CreateConnection();
        var sql = $"UPDATE {tablePrefix}_replicas SET ttl = {ttl} WHERE replica_id = '{replicaId:N}'";

        await using var command = new NpgsqlCommand(sql, conn);
        await command.ExecuteNonQueryAsync();
    }

    public async Task Delete(Guid replicaId)
    {
        await using var conn = await CreateConnection();
        var sql = $"DELETE FROM {tablePrefix}_replicas WHERE replica_id = '{replicaId:N}'";

        await using var command = new NpgsqlCommand(sql, conn);
        await command.ExecuteNonQueryAsync();
    }

    public async Task Prune(long currentTime)
    {
        await using var conn = await CreateConnection();
        var sql = $"DELETE FROM {tablePrefix}_replicas WHERE ttl < {currentTime}";

        await using var command = new NpgsqlCommand(sql, conn);
        await command.ExecuteNonQueryAsync();
    }

    public async Task<int> GetReplicaCount()
    {
        await using var conn = await CreateConnection();
        var sql = $"SELECT COUNT(*) FROM {tablePrefix}_replicas";

        await using var command = new NpgsqlCommand(sql, conn);
        return ((int)(long)(await command.ExecuteScalarAsync() ?? 0));
    }

    private async Task<NpgsqlConnection> CreateConnection()
    {
        var conn = new NpgsqlConnection(connectionString);
        await conn.OpenAsync();
        return conn;
    }
}