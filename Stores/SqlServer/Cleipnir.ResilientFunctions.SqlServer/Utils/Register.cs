using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Utils.Register;
using Microsoft.Data.SqlClient;

namespace Cleipnir.ResilientFunctions.SqlServer.Utils;

public class Register : IRegister
{
    private readonly string _connectionString;
    private readonly string _tablePrefix;

    public Register(string connectionString, string tablePrefix = "")
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
                CREATE TABLE {_tablePrefix}RFunctions_Register (
                    GroupName VARCHAR(255) NOT NULL,
                    KeyId VARCHAR(255) NOT NULL,
                    Value VARCHAR(255) NOT NULL,
                    PRIMARY KEY (GroupName, KeyId)
                );";
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
        await using var conn = new SqlConnection(_connectionString);
        await conn.OpenAsync();

        var sql = $"DROP TABLE IF EXISTS {_tablePrefix}RFunctions_Register";
        await using var command = new SqlCommand(sql, conn);
        await command.ExecuteNonQueryAsync();
    }
    
    public async Task<bool> SetIfEmpty(string group, string key, string value)
    {
        await using var conn = new SqlConnection(_connectionString);
        await conn.OpenAsync();

        var sql = @$" 
            INSERT INTO {_tablePrefix}RFunctions_Register (GroupName, KeyId, Value)            
            SELECT * FROM (VALUES (@Group, @Key, @Value)) AS s(GroupName, KeyId, Value)
            WHERE NOT EXISTS (SELECT * FROM {_tablePrefix}RFunctions_Register WITH (UPDLOCK) WHERE GroupName = @Group AND KeyId = @Key)";
        
        await using var command = new SqlCommand(sql, conn)
        {
            Parameters =
            {
                new() { ParameterName = "@Group", Value = group },
                new() { ParameterName = "@Key", Value = key }, 
                new() { ParameterName = "@Value", Value = value }
            }
        };

        var affectedRows = await command.ExecuteNonQueryAsync();
        return affectedRows > 0;
    }

    public async Task<bool> CompareAndSwap(string group, string key, string newValue, string expectedValue, bool setIfEmpty = true)
    {
        await using var conn = new SqlConnection(_connectionString);
        await conn.OpenAsync();

        if (!setIfEmpty)
        {
            //as setIfEmpty is false then only update if expected value is found
            var sql = @$" 
                UPDATE {_tablePrefix}RFunctions_Register
                SET Value = @NewValue
                WHERE GroupName = @Group AND KeyId = @Key AND Value = @ExpectedValue";
        
            await using var command = new SqlCommand(sql, conn)
            {
                Parameters =
                {
                    new() { ParameterName = "@NewValue", Value = newValue },
                    new() { ParameterName = "@Group", Value = group },
                    new() { ParameterName = "@Key", Value = key }, 
                    new() { ParameterName = "@ExpectedValue", Value = expectedValue },
                }
            };

            var affectedRows = await command.ExecuteNonQueryAsync();
            return affectedRows > 0;
        } else
        {
            //setIfEmpty is true
            var sql = @$"               
                BEGIN TRANSACTION;
                DELETE FROM {_tablePrefix}RFunctions_Register WHERE GroupName = @Group AND KeyId = @Key AND Value = @ExpectedValue;
                INSERT INTO {_tablePrefix}RFunctions_Register (GroupName, KeyId, Value)            
                SELECT * FROM (VALUES (@Group, @Key, @NewValue)) AS s(GroupName, KeyId, Value)
                WHERE NOT EXISTS (SELECT * FROM {_tablePrefix}RFunctions_Register WITH (UPDLOCK) WHERE GroupName = @Group AND KeyId = @Key);
                COMMIT TRANSACTION;";

            await using var command = new SqlCommand(sql, conn)
            {
                Parameters =
                {
                    new() { ParameterName = "@Group", Value = group },
                    new() { ParameterName = "@Key", Value = key },
                    new() { ParameterName = "@ExpectedValue", Value = expectedValue },
                    new() { ParameterName = "@NewValue", Value = newValue },
                }
            };

            var affectedRows = await command.ExecuteNonQueryAsync();
            return affectedRows > 0;
        }
    }

    public async Task<string?> Get(string group, string key)
    {
        await using var conn = new SqlConnection(_connectionString);
        await conn.OpenAsync();
        
        var sql = @$"    
            SELECT Value
            FROM {_tablePrefix}RFunctions_Register
            WHERE GroupName = @Group AND KeyId = @Key";
        await using var command = new SqlCommand(sql, conn)
        {
            Parameters =
            {
                new() { ParameterName = "@Group", Value = group },
                new() { ParameterName = "@Key", Value = key }
            }
        };
        
        await using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
            return reader.GetString(0);

        return default;
    }

    public async Task<bool> Delete(string group, string key, string expectedValue)
    {
        await using var conn = new SqlConnection(_connectionString);
        await conn.OpenAsync();

        var sql = @$" 
            DELETE FROM {_tablePrefix}RFunctions_Register
            WHERE GroupName = @Group AND KeyId = @Key AND value = @Value";

        await using var command = new SqlCommand(sql, conn)
        {
            Parameters =
            {
                new() { ParameterName = "@Group", Value = group },
                new() { ParameterName = "@Key", Value = key },
                new() { ParameterName = "@Value", Value = expectedValue },
            }
        };

        var affectedRows = await command.ExecuteNonQueryAsync();
        return affectedRows > 0;
    }

    public async Task Delete(string group, string key)
    {
        await using var conn = new SqlConnection(_connectionString);
        await conn.OpenAsync();

        var sql = @$" 
            DELETE FROM {_tablePrefix}RFunctions_Register
            WHERE GroupName = @Group AND KeyId = @Key";

        await using var command = new SqlCommand(sql, conn)
        {
            Parameters =
            {
                new() { ParameterName = "@Group", Value = group },
                new() { ParameterName = "@Key", Value = key },
            }
        };

        await command.ExecuteNonQueryAsync();
    }

    public async Task<bool> Exists(string group, string key)
    {
        await using var conn = new SqlConnection(_connectionString);
        await conn.OpenAsync();
        
        var sql = @$"    
            SELECT COUNT(*)
            FROM {_tablePrefix}RFunctions_Register
            WHERE GroupName = @Group AND KeyId = @Key";
        await using var command = new SqlCommand(sql, conn)
        {
            Parameters =
            {
                new() { ParameterName = "@Group", Value = group },
                new() { ParameterName = "@Key", Value = key },
            }
        };

        var count = (int?) await command.ExecuteScalarAsync();
        return count > 0;
    }
}