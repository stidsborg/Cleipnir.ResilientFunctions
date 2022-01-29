using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Utils.Monitor;
using Dapper;
using Microsoft.Data.SqlClient;

namespace Cleipnir.ResilientFunctions.SqlServer;

public class Monitor : IMonitor
{
    private readonly Func<Task<SqlConnection>> _connFunc;
    private readonly string _tablePrefix;

    public Monitor(Func<Task<SqlConnection>> connFunc, string tablePrefix = "")
    {
        _connFunc = connFunc;
        _tablePrefix = tablePrefix;
    }

    public async Task Initialize()
    {
        await using var conn = await _connFunc();
        try
        {
            await conn.ExecuteAsync(@$"
                CREATE TABLE {_tablePrefix}Monitor (
                    [LockId] NVARCHAR(255) PRIMARY KEY NOT NULL,                
                    [KeyId] NVARCHAR(255) NOT NULL
                );"
            );
        }
        catch (SqlException e)
        {
            if (e.Number != SqlError.TABLE_ALREADY_EXISTS)
                throw;
        }
    }

    public async Task DropUnderlyingTable()
    {
        await using var conn = await _connFunc();
        try
        {
            await conn.ExecuteAsync(@$"DROP TABLE {_tablePrefix}Monitor");
        }
        catch (SqlException e)
        {
            if (e.Number != SqlError.TABLE_DOES_NOT_EXIST)
                throw;
        }
    }

    public async Task<IMonitor.ILock?> Acquire(string lockId, string keyId)
    {
        try
        {
            await using var conn = await _connFunc();
            var affectedRows = await conn.ExecuteAsync(@$"
            IF (SELECT COUNT(*) FROM {_tablePrefix}Monitor WHERE [LockId] = @LockId AND [KeyId] = @KeyId) = 0
                INSERT INTO {_tablePrefix}Monitor ([LockId], [KeyId])
                VALUES (@LockId, @KeyId);",
                new {LockId = lockId, keyId = keyId}
            );

            return affectedRows == 0
                ? null
                : new Lock(this, lockId, keyId);
        }
        catch (SqlException sqlException)
        {
            if (sqlException.Number != SqlError.UNIQUENESS_VIOLATION)
                throw;
            
            return null;
        }
    }

    public async Task Release(string lockId, string keyId)
    {
        await using var conn = await _connFunc();
        await conn.ExecuteAsync(
            @$"DELETE FROM {_tablePrefix}Monitor WHERE [LockId] = @LockId AND [KeyId] = @KeyId",
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