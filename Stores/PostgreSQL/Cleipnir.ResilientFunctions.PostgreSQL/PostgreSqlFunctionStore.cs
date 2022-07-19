using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Storage;
using Npgsql;

namespace Cleipnir.ResilientFunctions.PostgreSQL;

public class PostgreSqlFunctionStore : IFunctionStore
{
    private readonly string _connectionString;
    private readonly string _tablePrefix;

    public PostgreSqlFunctionStore(string connectionString, string tablePrefix = "")
    {
        _connectionString = connectionString;
        _tablePrefix = tablePrefix;
    } 

    private async Task<NpgsqlConnection> CreateConnection()
    {
        var conn = new NpgsqlConnection(_connectionString);
        await conn.OpenAsync();
        return conn;
    }

    public async Task Initialize()
    {
        await using var conn = await CreateConnection();
        var sql = $@"
            CREATE TABLE IF NOT EXISTS {_tablePrefix}rfunctions (
                function_type_id VARCHAR(200) NOT NULL,
                function_instance_id VARCHAR(200) NOT NULL,
                param_json TEXT NULL,
                param_type VARCHAR(255) NULL,
                scrapbook_json TEXT NULL,
                scrapbook_type VARCHAR(255) NULL,
                status INT NOT NULL,
                result_json TEXT NULL,
                result_type VARCHAR(255) NULL,
                error_json TEXT NULL,
                postponed_until BIGINT NULL,
                epoch INT NOT NULL,
                sign_of_life INT NOT NULL,
                crashed_check_frequency BIGINT NOT NULL,
                PRIMARY KEY (function_type_id, function_instance_id)
            );
            CREATE INDEX IF NOT EXISTS idx_{_tablePrefix}rfunctions_executing
            ON {_tablePrefix}rfunctions(function_type_id, function_instance_id)
            INCLUDE (epoch, sign_of_life)
            WHERE status = {(int) Status.Executing};

            CREATE INDEX IF NOT EXISTS idx_{_tablePrefix}rfunctions_postponed
            ON {_tablePrefix}rfunctions(function_type_id, postponed_until, function_instance_id)
            INCLUDE (epoch)
            WHERE status = {(int) Status.Postponed};

            ALTER TABLE {_tablePrefix}rfunctions ALTER COLUMN status SET DEFAULT {(int) Status.Executing};
            ALTER TABLE {_tablePrefix}rfunctions ALTER COLUMN epoch SET DEFAULT 0;
            ALTER TABLE {_tablePrefix}rfunctions ALTER COLUMN sign_of_life SET DEFAULT 0;";

        await using var command = new NpgsqlCommand(sql, conn);
        await command.ExecuteNonQueryAsync();
    }

    public async Task DropIfExists()
    {
        await using var conn = await CreateConnection();
        var sql = $"DROP TABLE IF EXISTS {_tablePrefix}rfunctions";
        await using var command = new NpgsqlCommand(sql, conn);
        await command.ExecuteNonQueryAsync();
    }

    public async Task TruncateTable()
    {
        await using var conn = await CreateConnection();
        var sql = $"TRUNCATE TABLE {_tablePrefix}rfunctions";
        await using var command = new NpgsqlCommand(sql, conn);
        await command.ExecuteNonQueryAsync();
    }
    
    public async Task<bool> CreateFunction(
        FunctionId functionId, 
        StoredParameter param, 
        string? scrapbookType,
        long crashedCheckFrequency)
    {
        await using var conn = await CreateConnection();
        var sql = @$"
            INSERT INTO {_tablePrefix}rfunctions
                (function_type_id, function_instance_id, param_json, param_type, scrapbook_type, crashed_check_frequency)
            VALUES
                ($1, $2, $3, $4, $5, $6)
            ON CONFLICT DO NOTHING;";
        await using var command = new NpgsqlCommand(sql, conn)
        {
            Parameters =
            {
                new() {Value = functionId.TypeId.Value},
                new() {Value = functionId.InstanceId.Value},
                new() {Value = param.ParamJson},
                new() {Value =  param.ParamType},
                new() {Value = scrapbookType ?? (object) DBNull.Value},
                new (){Value = crashedCheckFrequency}
            }
        };

        var affectedRows = await command.ExecuteNonQueryAsync();
        return affectedRows == 1;
    }

    public async Task<bool> TryToBecomeLeader(
        FunctionId functionId, 
        Status newStatus, 
        int expectedEpoch, 
        int newEpoch,
        long crashedCheckFrequency
    )
    {
        await using var conn = await CreateConnection();
        var sql = @$"
            UPDATE {_tablePrefix}RFunctions
            SET epoch = $1, status = $2, crashed_check_frequency = $3
            WHERE function_type_id = $4 AND function_instance_id = $5 AND epoch = $6";

        await using var command = new NpgsqlCommand(sql, conn)
        {
            Parameters =
            {
                new() {Value = newEpoch},
                new() {Value = (int) newStatus},
                new() {Value = crashedCheckFrequency},
                new() {Value = functionId.TypeId.Value},
                new() {Value = functionId.InstanceId.Value},
                new() {Value = expectedEpoch},
            }
        };
        
        var affectedRows = await command.ExecuteNonQueryAsync();
        return affectedRows == 1;
    }

    public async Task<bool> UpdateSignOfLife(FunctionId functionId, int expectedEpoch, int newSignOfLife)
    {
        await using var conn = await CreateConnection();
        var sql = $@"
            UPDATE {_tablePrefix}rfunctions
            SET sign_of_life = $1
            WHERE function_type_id = $2 AND function_instance_id = $3 AND epoch = $4";
        await using var command = new NpgsqlCommand(sql, conn)
        {
            Parameters =
            {
                new() {Value = newSignOfLife},
                new() {Value = functionId.TypeId.Value},
                new() {Value = functionId.InstanceId.Value},
                new() {Value = expectedEpoch},
            }
        };

        var affectedRows = await command.ExecuteNonQueryAsync();
        return affectedRows == 1;
    }

    public async Task<IEnumerable<StoredExecutingFunction>> GetExecutingFunctions(FunctionTypeId functionTypeId)
    {
        await using var conn = await CreateConnection();
        var sql = @$"
            SELECT function_instance_id, epoch, sign_of_life, crashed_check_frequency 
            FROM {_tablePrefix}rfunctions
            WHERE function_type_id = $1 AND status = {(int) Status.Executing}";
        await using var command = new NpgsqlCommand(sql, conn)
        {
            Parameters = { new() {Value = functionTypeId.Value} }
        };

        await using var reader = await command.ExecuteReaderAsync();

        var functions = new List<StoredExecutingFunction>();
        while (await reader.ReadAsync())
        {
            var functionInstanceId = reader.GetString(0);
            var epoch = reader.GetInt32(1);
            var signOfLife = reader.GetInt32(2);
            var crashedCheckFrequency = reader.GetInt64(3);
            functions.Add(new StoredExecutingFunction(functionInstanceId, epoch, signOfLife, crashedCheckFrequency));
        }

        return functions;
    }

    public async Task<IEnumerable<StoredPostponedFunction>> GetPostponedFunctions(FunctionTypeId functionTypeId, long expiresBefore)
    {
        await using var conn = await CreateConnection();
        var sql = @$"
            SELECT function_instance_id, epoch, postponed_until
            FROM {_tablePrefix}rfunctions
            WHERE function_type_id = $1 AND status = {(int) Status.Postponed} AND postponed_until <= $2";
        await using var command = new NpgsqlCommand(sql, conn)
        {
            Parameters =
            {
                new() {Value = functionTypeId.Value},
                new() {Value = expiresBefore}
            }
        };
        
        await using var reader = await command.ExecuteReaderAsync();
        var functions = new List<StoredPostponedFunction>();
        while (await reader.ReadAsync())
        {
            var functionInstanceId = reader.GetString(0);
            var epoch = reader.GetInt32(1);
            var postponedUntil = reader.GetInt64(2);
            functions.Add(new StoredPostponedFunction(functionInstanceId, epoch, postponedUntil));
        }

        return functions;
    }
    
    public async Task<bool> SetFunctionState(
        FunctionId functionId, 
        Status status, 
        string? scrapbookJson, 
        StoredResult? result,
        string? errorJson, 
        long? postponedUntil, 
        int expectedEpoch)
    {
        await using var conn = await CreateConnection();
        var sql = $@"
            UPDATE {_tablePrefix}rfunctions
            SET status = $1, scrapbook_json = $2, 
                result_json = $3, result_type = $4, 
                error_json = $5, postponed_until = $6
            WHERE 
                function_type_id = $7 AND 
                function_instance_id = $8 AND 
                epoch = $9";
        await using var command = new NpgsqlCommand(sql, conn)
        {
            Parameters =
            {
                new() {Value = (int) status},
                new() {Value = scrapbookJson ?? (object) DBNull.Value},
                new() {Value = result?.ResultJson ?? (object) DBNull.Value},
                new() {Value = result?.ResultType ?? (object) DBNull.Value},
                new() {Value = errorJson ?? (object) DBNull.Value},
                new() {Value = postponedUntil ?? (object) DBNull.Value},
                new() {Value = functionId.TypeId.Value},
                new() {Value = functionId.InstanceId.Value},
                new() {Value = expectedEpoch},
            }
        };

        var affectedRows = await command.ExecuteNonQueryAsync();
        return affectedRows == 1;
    }

    public async Task<StoredFunction?> GetFunction(FunctionId functionId)
    {
        await using var conn = await CreateConnection();
        var sql = $@"
            SELECT               
                param_json, 
                param_type,
                scrapbook_json, 
                scrapbook_type,
                status,
                result_json, 
                result_type,
                error_json,
                postponed_until,
                epoch, 
                sign_of_life
            FROM {_tablePrefix}rfunctions
            WHERE function_type_id = $1 AND function_instance_id = $2;";
        await using var command = new NpgsqlCommand(sql, conn)
        {
            Parameters = { 
                new() {Value = functionId.TypeId.Value},
                new() {Value = functionId.InstanceId.Value}
            }
        };
        
        await using var reader = await command.ExecuteReaderAsync();
        
        while (await reader.ReadAsync())
        {
            var hasScrapbookJson = !await reader.IsDBNullAsync(2);
            var hasScrapbookType = !await reader.IsDBNullAsync(3);
            var hasResult = !await reader.IsDBNullAsync(6);
            var hasError = !await reader.IsDBNullAsync(7);
            var postponedUntil = !await reader.IsDBNullAsync(8);
            return new StoredFunction(
                functionId,
                new StoredParameter(reader.GetString(0), reader.GetString(1)),
                Scrapbook: hasScrapbookType ? new StoredScrapbook(
                    hasScrapbookJson ? reader.GetString(2) : null,  
                    reader.GetString(3)) : null,
                Status: (Status) reader.GetInt32(4),
                Result: hasResult ? new StoredResult(reader.GetString(5), reader.GetString(6)) : null,
                hasError ? reader.GetString(7) : null,
                postponedUntil ? reader.GetInt64(8) : null,
                Epoch: reader.GetInt32(9),
                SignOfLife: reader.GetInt32(10)
            );
        }

        return null;
    }
}