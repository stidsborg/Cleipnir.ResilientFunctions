using System.Collections.Generic;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Storage;
using Microsoft.Data.SqlClient;

namespace Cleipnir.ResilientFunctions.SqlServer;

public class SqlServerTimeoutStore : ITimeoutStore
{
    private readonly string _connectionString;
    private readonly string _tablePrefix;

    public SqlServerTimeoutStore(string connectionString, string tablePrefix = "")
    {
        _connectionString = connectionString;
        _tablePrefix = tablePrefix;
    }

    private string? _initializeSql;
    public async Task Initialize()
    {
        await using var conn = await CreateConnection();
        
        _initializeSql ??= @$"            
            CREATE TABLE {_tablePrefix}_Timeouts (
                flowType NVARCHAR(255),
                flowInstance NVARCHAR(255),
                TimeoutId NVARCHAR(255),
                Expires BIGINT,          
                PRIMARY KEY (flowType, flowInstance, TimeoutId)
            );";
        var command = new SqlCommand(_initializeSql, conn);
        try
        {
            await command.ExecuteNonQueryAsync();    
        } catch (SqlException exception) when (exception.Number == 2714) {}
    }

    private string? _truncateSql;
    public async Task Truncate()
    {
        await using var conn = await CreateConnection();
        _truncateSql ??= $"TRUNCATE TABLE {_tablePrefix}_Timeouts";
        var command = new SqlCommand(_truncateSql, conn);
        await command.ExecuteNonQueryAsync();
    }

    private string? _dropUnderlyingTableSql;
    public async Task DropUnderlyingTable()
    {
        await using var conn = await CreateConnection();
        _dropUnderlyingTableSql ??= $"DROP TABLE IF EXISTS {_tablePrefix}_Timeouts;";
        var command = new SqlCommand(_dropUnderlyingTableSql, conn);
        await command.ExecuteNonQueryAsync();
    }

    private string? _upsertTimeoutSql;
    public async Task UpsertTimeout(StoredTimeout storedTimeout, bool overwrite)
    {
        var (functionId, timeoutId, expiry) = storedTimeout;
        await using var conn = await CreateConnection();

        _upsertTimeoutSql ??= @$"
            IF EXISTS (
                SELECT * FROM {_tablePrefix}_Timeouts 
                WHERE flowType = @flowType AND flowInstance = @flowInstance AND TimeoutId=@TimeoutId
            )           
            BEGIN
                UPDATE {_tablePrefix}_Timeouts
                SET Expires = @Expiry
                WHERE flowType = @flowType AND flowInstance = @flowInstance AND TimeoutId = @TimeoutId AND @Overwrite = 1
            END
            ELSE
            BEGIN                
                INSERT INTO {_tablePrefix}_Timeouts
                    (flowType, flowInstance, TimeoutId, Expires)
                VALUES 
                    (@flowType, @flowInstance, @TimeoutId, @Expiry);
            END";
        await using var command = new SqlCommand(_upsertTimeoutSql, conn);
        command.Parameters.AddWithValue("@flowType", functionId.Type.Value);
        command.Parameters.AddWithValue("@flowInstance", functionId.Instance.Value);
        command.Parameters.AddWithValue("@TimeoutId", timeoutId);
        command.Parameters.AddWithValue("@Expiry", expiry);
        command.Parameters.AddWithValue("@Overwrite", overwrite ? 1 : 2);
        await command.ExecuteNonQueryAsync();
    }

    private string? _removeTimeoutSql;
    public async Task RemoveTimeout(FlowId flowId, string timeoutId)
    {
        await using var conn = await CreateConnection();

        _removeTimeoutSql ??= @$"    
            DELETE FROM {_tablePrefix}_Timeouts
            WHERE
                flowType = @flowType AND
                flowInstance = @flowInstance AND 
                TimeoutId = @TimeoutId";
        await using var command = new SqlCommand(_removeTimeoutSql, conn);
        command.Parameters.AddWithValue("@flowType", flowId.Type.Value);
        command.Parameters.AddWithValue("@flowInstance", flowId.Instance.Value);
        command.Parameters.AddWithValue("@TimeoutId", timeoutId);
        await command.ExecuteNonQueryAsync();
    }

    private string? _removeSql;
    public async Task Remove(FlowId flowId)
    {
        await using var conn = await CreateConnection();

        _removeSql ??= @$"    
            DELETE FROM {_tablePrefix}_Timeouts
            WHERE flowType = @flowType AND flowInstance = @flowInstance";
        
        await using var command = new SqlCommand(_removeSql, conn);
        command.Parameters.AddWithValue("@flowType", flowId.Type.Value);
        command.Parameters.AddWithValue("@flowInstance", flowId.Instance.Value);
        await command.ExecuteNonQueryAsync();
    }

    private string? _getTimeoutsExpiresBeforeSql;
    public async Task<IEnumerable<StoredTimeout>> GetTimeouts(long expiresBefore)
    {
        await using var conn = await CreateConnection();
        _getTimeoutsExpiresBeforeSql ??= @$"    
            SELECT flowType, flowInstance, TimeoutId, Expires
            FROM {_tablePrefix}_Timeouts
            WHERE Expires <= @ExpiresBefore";
        
        await using var command = new SqlCommand(_getTimeoutsExpiresBeforeSql, conn);
        command.Parameters.AddWithValue("@ExpiresBefore", expiresBefore);
        
        var storedTimeouts = new List<StoredTimeout>();
        await using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            var flowType = reader.GetString(0); 
            var flowInstance = reader.GetString(1);
            var timeoutId = reader.GetString(2);
            var expires = reader.GetInt64(3);
            var functionId = new FlowId(flowType, flowInstance);
            storedTimeouts.Add(new StoredTimeout(functionId, timeoutId, expires));
        }

        return storedTimeouts;
    }

    private string? _getTimeoutsSql;
    public async Task<IEnumerable<StoredTimeout>> GetTimeouts(FlowId flowId)
    {
        var (typeId, instanceId) = flowId;
        await using var conn = await CreateConnection();
        _getTimeoutsSql ??= @$"    
            SELECT TimeoutId, Expires
            FROM {_tablePrefix}_Timeouts
            WHERE flowType = @flowType AND flowInstance = @flowInstance";
        
        await using var command = new SqlCommand(_getTimeoutsSql, conn);
        command.Parameters.AddWithValue("@flowType", typeId.Value);
        command.Parameters.AddWithValue("@flowInstance", instanceId.Value);
        
        var storedTimeouts = new List<StoredTimeout>();
        await using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            var timeoutId = reader.GetString(0);
            var expires = reader.GetInt64(1);
            storedTimeouts.Add(new StoredTimeout(flowId, timeoutId, expires));
        }

        return storedTimeouts;
    }

    private async Task<SqlConnection> CreateConnection()
    {
        var conn = new SqlConnection(_connectionString);
        await conn.OpenAsync();
        return conn;
    }
}