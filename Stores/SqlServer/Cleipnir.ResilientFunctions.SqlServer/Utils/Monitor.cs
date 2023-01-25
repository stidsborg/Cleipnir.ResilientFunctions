using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Utils.Monitor;
using Microsoft.Data.SqlClient;

namespace Cleipnir.ResilientFunctions.SqlServer.Utils;

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
        await using var conn = new SqlConnection(_connectionString);
        await conn.OpenAsync();
        try
        {
            var sql = @$"
                CREATE TABLE {_tablePrefix}RFunctions_Monitor (
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

    public async Task TruncateTable()
    {
        await using var conn = new SqlConnection(_connectionString);
        await conn.OpenAsync();

        var sql = $"TRUNCATE TABLE {_tablePrefix}RFunctions_Monitor";
        await using var command = new SqlCommand(sql, conn);
        await command.ExecuteNonQueryAsync();
    }

    public async Task DropUnderlyingTable()
    {
        await using var conn = new SqlConnection(_connectionString);
        await conn.OpenAsync();

        var sql = $"DROP TABLE IF EXISTS {_tablePrefix}RFunctions_Monitor";
        await using var command = new SqlCommand(sql, conn);
        await command.ExecuteNonQueryAsync();
    }

    public async Task<IMonitor.ILock?> Acquire(string group, string key)
    {
        try
        {
            await using var conn = new SqlConnection(_connectionString);
            await conn.OpenAsync();
            var sql = @$"
                IF (SELECT COUNT(*) FROM {_tablePrefix}RFunctions_Monitor WHERE [GroupName] = @GroupName AND [KeyId] = @KeyId) = 0
                    INSERT INTO {_tablePrefix}RFunctions_Monitor ([GroupName], [KeyId])
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
        await using var conn = new SqlConnection(_connectionString);
        await conn.OpenAsync();
        var sql = $"DELETE FROM {_tablePrefix}RFunctions_Monitor WHERE [GroupName] = @GroupName AND [KeyId] = @KeyId";
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