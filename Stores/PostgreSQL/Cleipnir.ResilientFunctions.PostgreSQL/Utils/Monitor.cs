using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Utils.Monitor;
using Dapper;
using Npgsql;

namespace Cleipnir.ResilientFunctions.PostgreSQL.Utils;

public class Monitor : IMonitor
{
    private readonly Func<Task<NpgsqlConnection>> _connFunc;
    private readonly string _tablePrefix;

    public Monitor(Func<Task<NpgsqlConnection>> connFunc, string tablePrefix = "")
    {
        _connFunc = connFunc;
        _tablePrefix = tablePrefix;
    }

    public async Task Initialize()
    {
        await using var conn = await _connFunc();
        await conn.ExecuteAsync(@$"
            CREATE TABLE IF NOT EXISTS {_tablePrefix}monitor (
                lockid VARCHAR(255) PRIMARY KEY NOT NULL,                
                keyid VARCHAR(255) NOT NULL
            );"
        );
    }

    public async Task DropUnderlyingTable()
    {
        await using var conn = await _connFunc();
        await conn.ExecuteAsync(@$"DROP TABLE IF EXISTS {_tablePrefix}monitor");
    }

    public async Task<IMonitor.ILock?> Acquire(string lockId, string keyId)
    {
        await using var conn = await _connFunc();
        var affectedRows = await conn.ExecuteAsync(@$"           
                INSERT INTO {_tablePrefix}monitor (lockid, keyid)
                VALUES (@LockId, @KeyId)
                ON CONFLICT DO NOTHING;",
            new {LockId = lockId, KeyId = keyId}
        );

        if (affectedRows == 1) return new Lock(this, lockId, keyId);

        var count = conn.ExecuteScalar<int>(@$"
            SELECT COUNT(*) 
            FROM {_tablePrefix}monitor
            WHERE lockid = @LockId AND keyid = @KeyId;",
            new {LockId = lockId, KeyId = keyId}
        );
        
        return count == 0
            ? null
            : new Lock(this, lockId, keyId);
    }

    public async Task Release(string lockId, string keyId)
    {
        await using var conn = await _connFunc();
        await conn.ExecuteAsync(
            @$"DELETE FROM {_tablePrefix}monitor WHERE lockid = @LockId AND keyid = @KeyId",
            new { LockId = lockId, KeyId = keyId }
        );
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