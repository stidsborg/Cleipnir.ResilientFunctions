using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Utils.Monitor;
using Microsoft.Data.SqlClient;

namespace Cleipnir.ResilientFunctions.SqlServer.Utils;

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
            var sql = @$"
                CREATE TABLE {_tablePrefix}Monitor (
                    [GroupName] NVARCHAR(255) PRIMARY KEY NOT NULL,                
                    [KeyId] NVARCHAR(255) NOT NULL
                )";

            await using var command = new SqlCommand(sql, conn);
            await command.ExecuteNonQueryAsync();
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
            var sql = $"DROP TABLE {_tablePrefix}Monitor";
            await using var command = new SqlCommand(sql, conn);
            await command.ExecuteNonQueryAsync();
        }
        catch (SqlException e)
        {
            if (e.Number != SqlError.TABLE_DOES_NOT_EXIST)
                throw;
        }
    }

    public async Task<IMonitor.ILock?> Acquire(string group, string key)
    {
        try
        {
            await using var conn = await _connFunc();
            var sql = @$"
                IF (SELECT COUNT(*) FROM {_tablePrefix}Monitor WHERE [GroupName] = @GroupName AND [KeyId] = @KeyId) = 0
                    INSERT INTO {_tablePrefix}Monitor ([GroupName], [KeyId])
                    VALUES (@GroupName, @KeyId);";
            await using var command = new SqlCommand(sql, conn);
            command.Parameters.AddWithValue("@GroupName", group);
            command.Parameters.AddWithValue("@KeyId", key);

            var affectedRows = await command.ExecuteNonQueryAsync();
            return affectedRows == 0
                ? null
                : new Lock(this, group, key);
        }
        catch (SqlException sqlException)
        {
            if (sqlException.Number != SqlError.UNIQUENESS_VIOLATION)
                throw;
            
            return null;
        }
    }

    public async Task Release(string group, string key)
    {
        await using var conn = await _connFunc();
        var sql = $"DELETE FROM {_tablePrefix}Monitor WHERE [GroupName] = @GroupName AND [KeyId] = @KeyId";
        await using var command = new SqlCommand(sql, conn);
        command.Parameters.AddWithValue("@GroupName", group);
        command.Parameters.AddWithValue("@KeyId", key);
        await command.ExecuteNonQueryAsync();
    }

    private class Lock : IMonitor.ILock
    {
        private readonly IMonitor _monitor;
        private readonly string _group;
        private readonly string _key;

        public Lock(IMonitor monitor, string group, string key)
        {
            _monitor = monitor;
            _group = group;
            _key = key;
        }

        public async ValueTask DisposeAsync() => await _monitor.Release(_group, _key);
    }
}