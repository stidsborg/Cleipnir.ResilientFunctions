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

    private string? _initializeSql;
    public async Task Initialize()
    {
        await using var conn = new SqlConnection(_connectionString);
        await conn.OpenAsync();
        try
        {
            _initializeSql ??= @$"            
                CREATE TABLE {_tablePrefix}_Register (
                    RegisterType INT NOT NULL,
                    [Group] VARCHAR(255) NOT NULL,
                    Name VARCHAR(255) NOT NULL,
                    Value VARCHAR(255) NOT NULL,
                    PRIMARY KEY (RegisterType, [Group], Name)
                );";
            await using var command = new SqlCommand(_initializeSql, conn);
            await command.ExecuteNonQueryAsync();
        }
        catch (SqlException e)
        {
            if (e.Number != SqlError.TABLE_ALREADY_EXISTS)
                throw;
        }
    }

    private string? _setIfEmptySql;
    public async Task<bool> SetIfEmpty(RegisterType registerType, string group, string name, string value)
    {
        await using var conn = new SqlConnection(_connectionString);
        await conn.OpenAsync();

        _setIfEmptySql ??= @$" 
            INSERT INTO {_tablePrefix}_Register 
                (RegisterType, [Group], Name, Value)
            VALUES 
                (@RegisterType, @Group, @Name, @Value)";
        
        await using var command = new SqlCommand(_setIfEmptySql, conn)
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

    private string? _compareAndSwapNonEmptySql;
    private string? _compareAndSwapEmptySql;
    public async Task<bool> CompareAndSwap(RegisterType registerType, string group, string name, string newValue, string expectedValue, bool setIfEmpty = true)
    {
        await using var conn = new SqlConnection(_connectionString);
        await conn.OpenAsync();

        if (!setIfEmpty)
        {
            //as setIfEmpty is false then only update if expected value is found
            _compareAndSwapNonEmptySql ??= @$" 
                UPDATE {_tablePrefix}_Register
                SET Value = @NewValue
                WHERE RegisterType = @RegisterType AND [Group] = @Group AND Name = @Name AND Value = @ExpectedValue";
        
            await using var command = new SqlCommand(_compareAndSwapNonEmptySql, conn)
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
            _compareAndSwapEmptySql ??= @$"               
                BEGIN TRANSACTION;
                DELETE FROM {_tablePrefix}_Register WHERE RegisterType = @RegisterType AND [Group] = @Group AND Name = @Name AND Value = @ExpectedValue;
                INSERT INTO {_tablePrefix}_Register (RegisterType, [Group], Name, Value)
                VALUES (@RegisterType, @Group, @Name, @NewValue);
                COMMIT TRANSACTION;";

            await using var command = new SqlCommand(_compareAndSwapEmptySql, conn)
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

    private string? _getSql;
    public async Task<string?> Get(RegisterType registerType, string group, string name)
    {
        await using var conn = new SqlConnection(_connectionString);
        await conn.OpenAsync();
        
        _getSql ??= @$"    
            SELECT Value
            FROM {_tablePrefix}_Register
            WHERE RegisterType = @RegisterType AND [Group] = @Group AND Name = @Name";
        await using var command = new SqlCommand(_getSql, conn)
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

    private string? _deleteExpectedValueSql;
    public async Task<bool> Delete(RegisterType registerType, string group, string name, string expectedValue)
    {
        await using var conn = new SqlConnection(_connectionString);
        await conn.OpenAsync();

        _deleteExpectedValueSql ??= @$" 
            DELETE FROM {_tablePrefix}_Register
            WHERE RegisterType = @RegisterType AND [Group] = @Group AND Name = @Name AND Value = @Value";

        await using var command = new SqlCommand(_deleteExpectedValueSql, conn)
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

    private string? _deleteSql;
    public async Task Delete(RegisterType registerType, string group, string name)
    {
        await using var conn = new SqlConnection(_connectionString);
        await conn.OpenAsync();

        _deleteSql ??= @$" 
            DELETE FROM {_tablePrefix}_Register
            WHERE RegisterType = @RegisterType AND [Group] = @Group AND Name = @Name";

        await using var command = new SqlCommand(_deleteSql, conn)
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

    private string? _existsSql;
    public async Task<bool> Exists(RegisterType registerType, string group, string name)
    {
        await using var conn = new SqlConnection(_connectionString);
        await conn.OpenAsync();
        
        _existsSql ??= @$"    
            SELECT COUNT(*)
            FROM {_tablePrefix}_Register
            WHERE RegisterType = @RegisterType AND [Group] = @Group AND Name = @Name";
        await using var command = new SqlCommand(_existsSql, conn)
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

    private string? _truncateTableSql;
    public async Task TruncateTable()
    {
        await using var conn = new SqlConnection(_connectionString);
        await conn.OpenAsync();

        _truncateTableSql ??= $"TRUNCATE TABLE {_tablePrefix}_Register";
        await using var command = new SqlCommand(_truncateTableSql, conn);
        await command.ExecuteNonQueryAsync();
    }
}