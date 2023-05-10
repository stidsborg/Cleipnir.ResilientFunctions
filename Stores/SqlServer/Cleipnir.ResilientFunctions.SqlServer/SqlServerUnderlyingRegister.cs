using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Utils;
using Microsoft.Data.SqlClient;

namespace Cleipnir.ResilientFunctions.SqlServer;

public class SqlServerUnderlyingRegister : IUnderlyingRegister
{
    private readonly string _connectionString;
    private readonly string _tablePrefix;

    public SqlServerUnderlyingRegister(string connectionString, string tablePrefix = "")
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
                    RegisterType INT NOT NULL,
                    [Group] VARCHAR(255) NOT NULL,
                    Name VARCHAR(255) NOT NULL,
                    Value VARCHAR(255) NOT NULL,
                    PRIMARY KEY (RegisterType, [Group], Name)
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
    
    public async Task<bool> SetIfEmpty(RegisterType registerType, string group, string name, string value)
    {
        await using var conn = new SqlConnection(_connectionString);
        await conn.OpenAsync();

        var sql = @$" 
            INSERT INTO {_tablePrefix}RFunctions_Register 
                (RegisterType, [Group], Name, Value)
            VALUES 
                (@RegisterType, @Group, @Name, @Value)";
        
        await using var command = new SqlCommand(sql, conn)
        {
            Parameters =
            {
                new() { ParameterName = "@RegisterType", Value = (int) registerType },
                new() { ParameterName = "@Group", Value = group },
                new() { ParameterName = "@Name", Value = name }, 
                new() { ParameterName = "@Value", Value = value }
            }
        };

        try
        {
            await command.ExecuteNonQueryAsync();
            return true;
        } catch (SqlException sqlException) when (sqlException.Number == SqlError.UNIQUENESS_VIOLATION)
        {
            return false;
        }
    }

    public async Task<bool> CompareAndSwap(RegisterType registerType, string group, string name, string newValue, string expectedValue, bool setIfEmpty = true)
    {
        await using var conn = new SqlConnection(_connectionString);
        await conn.OpenAsync();

        if (!setIfEmpty)
        {
            //as setIfEmpty is false then only update if expected value is found
            var sql = @$" 
                UPDATE {_tablePrefix}RFunctions_Register
                SET Value = @NewValue
                WHERE RegisterType = @RegisterType AND [Group] = @Group AND Name = @Name AND Value = @ExpectedValue";
        
            await using var command = new SqlCommand(sql, conn)
            {
                Parameters =
                {
                    new() { ParameterName = "@RegisterType", Value =  (int) registerType },
                    new() { ParameterName = "@Group", Value = group },
                    new() { ParameterName = "@Name", Value = name }, 
                    new() { ParameterName = "@NewValue", Value = newValue },
                    new() { ParameterName = "@ExpectedValue", Value = expectedValue }
                }
            };

            var affectedRows = await command.ExecuteNonQueryAsync();
            return affectedRows > 0;
        } else
        {
            //setIfEmpty is true
            var sql = @$"               
                BEGIN TRANSACTION;
                DELETE FROM {_tablePrefix}RFunctions_Register WHERE RegisterType = @RegisterType AND [Group] = @Group AND Name = @Name AND Value = @ExpectedValue;
                INSERT INTO {_tablePrefix}RFunctions_Register (RegisterType, [Group], Name, Value)
                VALUES (@RegisterType, @Group, @Name, @NewValue);
                COMMIT TRANSACTION;";

            await using var command = new SqlCommand(sql, conn)
            {
                Parameters =
                {
                    new() { ParameterName = "@RegisterType", Value = (int) registerType },
                    new() { ParameterName = "@Group", Value = group },
                    new() { ParameterName = "@Name", Value = name },
                    new() { ParameterName = "@ExpectedValue", Value = expectedValue },
                    new() { ParameterName = "@NewValue", Value = newValue },
                }
            };

            try
            {
                var affectedRows = await command.ExecuteNonQueryAsync();
                return affectedRows > 0;
            } catch (SqlException sqlException) when (sqlException.Number == SqlError.UNIQUENESS_VIOLATION)
            {
                return false;
            }
        }
    }

    public async Task<string?> Get(RegisterType registerType, string group, string name)
    {
        await using var conn = new SqlConnection(_connectionString);
        await conn.OpenAsync();
        
        var sql = @$"    
            SELECT Value
            FROM {_tablePrefix}RFunctions_Register
            WHERE RegisterType = @RegisterType AND [Group] = @Group AND Name = @Name";
        await using var command = new SqlCommand(sql, conn)
        {
            Parameters =
            {
                new() { ParameterName = "@RegisterType", Value = (int) registerType },
                new() { ParameterName = "@Group", Value = group },
                new() { ParameterName = "@Name", Value = name }
            }
        };
        
        await using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
            return reader.GetString(0);

        return default;
    }

    public async Task<bool> Delete(RegisterType registerType, string group, string name, string expectedValue)
    {
        await using var conn = new SqlConnection(_connectionString);
        await conn.OpenAsync();

        var sql = @$" 
            DELETE FROM {_tablePrefix}RFunctions_Register
            WHERE RegisterType = @RegisterType AND [Group] = @Group AND Name = @Name AND Value = @Value";

        await using var command = new SqlCommand(sql, conn)
        {
            Parameters =
            {
                new() { ParameterName = "@RegisterType", Value = (int) registerType },
                new() { ParameterName = "@Group", Value = group },
                new() { ParameterName = "@Name", Value = name },
                new() { ParameterName = "@Value", Value = expectedValue },
            }
        };

        var affectedRows = await command.ExecuteNonQueryAsync();
        return affectedRows > 0;
    }

    public async Task Delete(RegisterType registerType, string group, string name)
    {
        await using var conn = new SqlConnection(_connectionString);
        await conn.OpenAsync();

        var sql = @$" 
            DELETE FROM {_tablePrefix}RFunctions_Register
            WHERE RegisterType = @RegisterType AND [Group] = @Group AND Name = @Name";

        await using var command = new SqlCommand(sql, conn)
        {
            Parameters =
            {
                new() { ParameterName = "@RegisterType", Value = (int) registerType },
                new() { ParameterName = "@Group", Value = group },
                new() { ParameterName = "@Name", Value = name }
            }
        };

        await command.ExecuteNonQueryAsync();
    }

    public async Task<bool> Exists(RegisterType registerType, string group, string name)
    {
        await using var conn = new SqlConnection(_connectionString);
        await conn.OpenAsync();
        
        var sql = @$"    
            SELECT COUNT(*)
            FROM {_tablePrefix}RFunctions_Register
            WHERE RegisterType = @RegisterType AND [Group] = @Group AND Name = @Name";
        await using var command = new SqlCommand(sql, conn)
        {
            Parameters =
            {
                new() { ParameterName = "@RegisterType", Value = (int) registerType },
                new() { ParameterName = "@Group", Value = group },
                new() { ParameterName = "@Name", Value = name }
            }
        };

        var count = (int?) await command.ExecuteScalarAsync();
        return count > 0;
    }

    public async Task TruncateTable()
    {
        await using var conn = new SqlConnection(_connectionString);
        await conn.OpenAsync();

        var sql = $"TRUNCATE TABLE {_tablePrefix}RFunctions_Register";
        await using var command = new SqlCommand(sql, conn);
        await command.ExecuteNonQueryAsync();
    }
}