using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Storage;
using Microsoft.Data.SqlClient;

namespace Cleipnir.ResilientFunctions.SqlServer;

public class SqlServerReplicaStore(string connectionString, string tablePrefix = "") : IReplicaStore
{
    private readonly Func<Task<SqlConnection>> _connFunc = CreateConnection(connectionString);
    
    public async Task Initialize()
    {
        await using var conn = await _connFunc();
        var sql = @$"    
            CREATE TABLE {tablePrefix}_Replicas (
                ReplicaId NCHAR(32) PRIMARY KEY,
                Ttl BIGINT NOT NULL                                                       
            );";

        await using var command = new SqlCommand(sql, conn);
        try
        {
            await command.ExecuteNonQueryAsync();    
        } catch (SqlException exception) when (exception.Number == 2714) {}
    }

    public async Task Truncate()
    {
        await using var conn = await _connFunc();
        var sql = $"TRUNCATE TABLE {tablePrefix}_Replicas;";
        await using var command = new SqlCommand(sql, conn);
        try
        {
            await command.ExecuteNonQueryAsync();    
        } catch (SqlException exception) when (exception.Number == 2714) {}
    }

    public async Task Insert(Guid replicaId, long ttl)
    {
        await using var conn = await _connFunc();
        var sql = $"INSERT INTO {tablePrefix}_Replicas VALUES ('{replicaId:N}', {ttl});";
        await using var command = new SqlCommand(sql, conn);
        await command.ExecuteNonQueryAsync();
    }

    public async Task Update(Guid replicaId, long ttl)
    {
        await using var conn = await _connFunc();
        var sql = $"UPDATE {tablePrefix}_Replicas SET Ttl = {ttl} WHERE ReplicaId = '{replicaId:N}'";
        await using var command = new SqlCommand(sql, conn);
        await command.ExecuteNonQueryAsync();
    }

    public async Task Delete(Guid replicaId)
    {
        await using var conn = await _connFunc();
        var sql = $"DELETE FROM {tablePrefix}_Replicas WHERE ReplicaId = '{replicaId:N}'";
        await using var command = new SqlCommand(sql, conn);
        await command.ExecuteNonQueryAsync();
    }

    public async Task Prune(long currentTime)
    {
        await using var conn = await _connFunc();
        var sql = $"DELETE FROM {tablePrefix}_Replicas WHERE Ttl < '{currentTime}'";
        await using var command = new SqlCommand(sql, conn);
        await command.ExecuteNonQueryAsync();
    }

    public async Task<int> GetReplicaCount()
    {
        await using var conn = await _connFunc();
        var sql = $"SELECT COUNT(*) FROM {tablePrefix}_Replicas";
        await using var command = new SqlCommand(sql, conn);
        return ((int?) await command.ExecuteScalarAsync()) ?? 0;
    }
    
    private static Func<Task<SqlConnection>> CreateConnection(string connectionString)
    {
        return async () =>
        {
            var connection = new SqlConnection(connectionString);
            await connection.OpenAsync();
            return connection;
        };
    }
}