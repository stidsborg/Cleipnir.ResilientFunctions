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

    public async Task Initialize()
    {
        await using var conn = await CreateConnection();
        
        var sql = @$"            
            CREATE TABLE {_tablePrefix}RFunctions_Timeouts (
                FunctionTypeId NVARCHAR(255),
                FunctionInstanceId NVARCHAR(255),
                TimeoutId NVARCHAR(255),
                Expires BIGINT,          
                PRIMARY KEY (FunctionTypeId, FunctionInstanceId, TimeoutId)
            );";
        var command = new SqlCommand(sql, conn);
        try
        {
            await command.ExecuteNonQueryAsync();    
        } catch (SqlException exception) when (exception.Number == 2714) {}
    }
    
    public async Task TruncateTable()
    {
        await using var conn = await CreateConnection();
        
        var sql = $"TRUNCATE TABLE {_tablePrefix}RFunctions_Timeouts";
        var command = new SqlCommand(sql, conn);
        await command.ExecuteNonQueryAsync();
    }
    
    public async Task DropUnderlyingTable()
    {
        await using var conn = await CreateConnection();
        var sql = $"DROP TABLE IF EXISTS {_tablePrefix}RFunctions_Timeouts;";
        var command = new SqlCommand(sql, conn);
        await command.ExecuteNonQueryAsync();
    }

    public async Task UpsertTimeout(StoredTimeout storedTimeout, bool overwrite)
    {
        var (functionId, timeoutId, expiry) = storedTimeout;
        await using var conn = await CreateConnection();

        var sql = @$"
            IF EXISTS (
                SELECT * FROM {_tablePrefix}RFunctions_Timeouts 
                WHERE FunctionTypeId = @FunctionTypeId AND FunctionInstanceId = @FunctionInstanceId AND TimeoutId=@TimeoutId
            )           
            BEGIN
                UPDATE {_tablePrefix}RFunctions_Timeouts
                SET Expires = @Expiry
                WHERE FunctionTypeId = @FunctionTypeId AND FunctionInstanceId = @FunctionInstanceId AND TimeoutId = @TimeoutId AND @Overwrite = 1
            END
            ELSE
            BEGIN                
                INSERT INTO {_tablePrefix}RFunctions_Timeouts
                    (FunctionTypeId, FunctionInstanceId, TimeoutId, Expires)
                VALUES 
                    (@FunctionTypeId, @FunctionInstanceId, @TimeoutId, @Expiry);
            END";
        await using var command = new SqlCommand(sql, conn);
        command.Parameters.AddWithValue("@FunctionTypeId", functionId.TypeId.Value);
        command.Parameters.AddWithValue("@FunctionInstanceId", functionId.InstanceId.Value);
        command.Parameters.AddWithValue("@TimeoutId", timeoutId);
        command.Parameters.AddWithValue("@Expiry", expiry);
        command.Parameters.AddWithValue("@Overwrite", overwrite ? 1 : 2);
        await command.ExecuteNonQueryAsync();
    }

    public async Task RemoveTimeout(FunctionId functionId, string timeoutId)
    {
        await using var conn = await CreateConnection();

        var sql = @$"    
            DELETE FROM {_tablePrefix}RFunctions_Timeouts
            WHERE
                FunctionTypeId = @FunctionTypeId AND
                FunctionInstanceId = @FunctionInstanceId AND 
                TimeoutId = @TimeoutId";
        await using var command = new SqlCommand(sql, conn);
        command.Parameters.AddWithValue("@FunctionTypeId", functionId.TypeId.Value);
        command.Parameters.AddWithValue("@FunctionInstanceId", functionId.InstanceId.Value);
        command.Parameters.AddWithValue("@TimeoutId", timeoutId);
        await command.ExecuteNonQueryAsync();
    }

    public async Task Remove(FunctionId functionId)
    {
        await using var conn = await CreateConnection();

        var sql = @$"    
            DELETE FROM {_tablePrefix}RFunctions_Timeouts
            WHERE FunctionTypeId = @FunctionTypeId AND FunctionInstanceId = @FunctionInstanceId";
        
        await using var command = new SqlCommand(sql, conn);
        command.Parameters.AddWithValue("@FunctionTypeId", functionId.TypeId.Value);
        command.Parameters.AddWithValue("@FunctionInstanceId", functionId.InstanceId.Value);
        await command.ExecuteNonQueryAsync();
    }

    public async Task<IEnumerable<StoredTimeout>> GetTimeouts(string functionTypeId, long expiresBefore)
    {
        await using var conn = await CreateConnection();
        var sql = @$"    
            SELECT FunctionInstanceId, TimeoutId, Expires
            FROM {_tablePrefix}RFunctions_Timeouts
            WHERE FunctionTypeId = @FunctionTypeId AND Expires <= @ExpiresBefore";
        
        await using var command = new SqlCommand(sql, conn);
        command.Parameters.AddWithValue("@FunctionTypeId", functionTypeId);
        command.Parameters.AddWithValue("@ExpiresBefore", expiresBefore);
        
        var storedTimeouts = new List<StoredTimeout>();
        await using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            var functionInstanceId = reader.GetString(0);
            var timeoutId = reader.GetString(1);
            var expires = reader.GetInt64(2);
            var functionId = new FunctionId(functionTypeId, functionInstanceId);
            storedTimeouts.Add(new StoredTimeout(functionId, timeoutId, expires));
        }

        return storedTimeouts;
    }

    public async Task<IEnumerable<StoredTimeout>> GetTimeouts(FunctionId functionId)
    {
        var (typeId, instanceId) = functionId;
        await using var conn = await CreateConnection();
        var sql = @$"    
            SELECT TimeoutId, Expires
            FROM {_tablePrefix}RFunctions_Timeouts
            WHERE FunctionTypeId = @FunctionTypeId AND FunctionInstanceId = @FunctionInstanceId";
        
        await using var command = new SqlCommand(sql, conn);
        command.Parameters.AddWithValue("@FunctionTypeId", typeId.Value);
        command.Parameters.AddWithValue("@FunctionInstanceId", instanceId.Value);
        
        var storedTimeouts = new List<StoredTimeout>();
        await using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            var timeoutId = reader.GetString(0);
            var expires = reader.GetInt64(1);
            storedTimeouts.Add(new StoredTimeout(functionId, timeoutId, expires));
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