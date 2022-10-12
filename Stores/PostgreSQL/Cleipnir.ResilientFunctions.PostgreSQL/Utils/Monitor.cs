using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Utils.Monitor;
using Npgsql;

namespace Cleipnir.ResilientFunctions.PostgreSQL.Utils;

public class Monitor : IMonitor
{
    private readonly string _connectionString;
    private readonly string _tablePrefix;

    public Monitor(string connectionString, string tablePrefix = "")
    {
        _connectionString = connectionString;
        _tablePrefix = tablePrefix;
    }

    public async Task Initialize()
    {
        var conn = new NpgsqlConnection(_connectionString);
        await conn.OpenAsync();
        var sql = @$"
            CREATE TABLE IF NOT EXISTS {_tablePrefix}monitor (
                groupname VARCHAR(255) PRIMARY KEY NOT NULL,                
                keyid VARCHAR(255) NOT NULL
            );";

        await using var command = new NpgsqlCommand(sql, conn);
        await command.ExecuteNonQueryAsync();
    }

    public async Task DropUnderlyingTable()
    {
        var conn = new NpgsqlConnection(_connectionString);
        await conn.OpenAsync();
        var sql = @$"DROP TABLE IF EXISTS {_tablePrefix}monitor";
        
        await using var command = new NpgsqlCommand(sql, conn);
        await command.ExecuteNonQueryAsync();
    }

    public async Task<IMonitor.ILock?> Acquire(string group, string key)
    {
        var conn = new NpgsqlConnection(_connectionString);
        await conn.OpenAsync();
        {
            var sql = @$"           
            INSERT INTO {_tablePrefix}monitor (groupname, keyid)
            VALUES ($1, $2)
            ON CONFLICT DO NOTHING;";
            await using var command = new NpgsqlCommand(sql, conn)
            {
                Parameters =
                {
                    new() {Value = group},
                    new() {Value = key}
                }
            };

            var affectedRows = await command.ExecuteNonQueryAsync();
            if (affectedRows == 1) return new Lock(this, group, key);
        }
        {
            var sql = @$"
                SELECT COUNT(*) 
                FROM {_tablePrefix}monitor
                WHERE groupname = $1 AND keyid = $2;";
            await using var command = new NpgsqlCommand(sql, conn)
            {
                Parameters =
                {
                    new() {Value = group},
                    new() {Value = key}
                }
            };
            var count = (long) (await command.ExecuteScalarAsync() ?? 0);
            return count == 1
                ? new Lock(this, group, key)
                : null;
        }
    }

    public async Task Release(string group, string key)
    {
        var conn = new NpgsqlConnection(_connectionString);
        await conn.OpenAsync();
        var sql = @$"DELETE FROM {_tablePrefix}monitor WHERE groupname = $1 AND keyid = $2";
        await using var command = new NpgsqlCommand(sql, conn)
        {
            Parameters =
            {
                new() {Value = group},
                new() {Value = key}
            }
        };
        await command.ExecuteNonQueryAsync();
    }

    private class Lock : IMonitor.ILock
    {
        private readonly IMonitor _monitor;
        private readonly string _lockId;
        private readonly string _keyId;

        public Lock(IMonitor monitor, string lockId, string keyId)
        {
            _monitor = monitor;
            _lockId = lockId;
            _keyId = keyId;
        }

        public async ValueTask DisposeAsync() => await _monitor.Release(_lockId, _keyId);
    }
}