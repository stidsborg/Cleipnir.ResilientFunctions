using System;
using System.Collections.Generic;
using System.Text.Json;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Messaging;
using Cleipnir.ResilientFunctions.Storage;
using Npgsql;

namespace Cleipnir.ResilientFunctions.PostgreSQL;

public class PostgreSqlFunctionStore : IFunctionStore
{
    private readonly string _connectionString;
    private readonly string _tablePrefix;

    private readonly PostgreSqlEventStore _eventStore;
    public IEventStore EventStore => _eventStore;

    private readonly PostgreSqlTimeoutStore _timeoutStore;
    public ITimeoutStore TimeoutStore => _timeoutStore;

    public PostgreSqlFunctionStore(string connectionString, string tablePrefix = "")
    {
        _connectionString = connectionString;
        _tablePrefix = tablePrefix;
        _eventStore = new PostgreSqlEventStore(connectionString, tablePrefix);
        _timeoutStore = new PostgreSqlTimeoutStore(connectionString, tablePrefix);
    } 

    private async Task<NpgsqlConnection> CreateConnection()
    {
        var conn = new NpgsqlConnection(_connectionString);
        await conn.OpenAsync();
        return conn;
    }

    public async Task Initialize()
    {
        await _eventStore.Initialize();
        await _timeoutStore.Initialize();
        await using var conn = await CreateConnection();
        var sql = $@"
            CREATE TABLE IF NOT EXISTS {_tablePrefix}rfunctions (
                function_type_id VARCHAR(200) NOT NULL,
                function_instance_id VARCHAR(200) NOT NULL,
                param_json TEXT NOT NULL,
                param_type VARCHAR(255) NOT NULL,
                scrapbook_json TEXT NOT NULL,
                scrapbook_type VARCHAR(255) NOT NULL,
                status INT NOT NULL DEFAULT {(int) Status.Executing},
                result_json TEXT NULL,
                result_type VARCHAR(255) NULL,
                exception_json TEXT NULL,
                postponed_until BIGINT NULL,
                suspend_until_eventsource_count INT NULL,
                epoch INT NOT NULL DEFAULT 0,
                sign_of_life INT NOT NULL DEFAULT 0,
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

            CREATE INDEX IF NOT EXISTS idx_{_tablePrefix}rfunctions_suspended
            ON {_tablePrefix}rfunctions(function_type_id, function_instance_id)
            INCLUDE (epoch)
            WHERE status = {(int) Status.Suspended};
            ";

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
        await _eventStore.TruncateTable();
        await _timeoutStore.TruncateTable();
        
        await using var conn = await CreateConnection();
        var sql = $"TRUNCATE TABLE {_tablePrefix}rfunctions";
        await using var command = new NpgsqlCommand(sql, conn);
        await command.ExecuteNonQueryAsync();
    }
    
    public async Task<bool> CreateFunction(FunctionId functionId, StoredParameter param, StoredScrapbook storedScrapbook, long crashedCheckFrequency)
    {
        await using var conn = await CreateConnection();
        var sql = @$"
            INSERT INTO {_tablePrefix}rfunctions
                (function_type_id, function_instance_id, param_json, param_type, scrapbook_json, scrapbook_type, crashed_check_frequency)
            VALUES
                ($1, $2, $3, $4, $5, $6, $7)
            ON CONFLICT DO NOTHING;";
        await using var command = new NpgsqlCommand(sql, conn)
        {
            Parameters =
            {
                new() {Value = functionId.TypeId.Value},
                new() {Value = functionId.InstanceId.Value},
                new() {Value = param.ParamJson},
                new() {Value =  param.ParamType},
                new() {Value = storedScrapbook.ScrapbookJson},
                new() {Value = storedScrapbook.ScrapbookType},
                new() {Value = crashedCheckFrequency}
            }
        };

        var affectedRows = await command.ExecuteNonQueryAsync();
        return affectedRows == 1;
    }

    public async Task<bool> IncrementEpoch(FunctionId functionId, int expectedEpoch)
    {
        await using var conn = await CreateConnection();

        var sql = @$"
            UPDATE {_tablePrefix}rfunctions
            SET epoch = epoch + 1
            WHERE function_type_id = $1 AND function_instance_id = $2 AND epoch = $3";

        await using var command = new NpgsqlCommand(sql, conn)
        {
            Parameters =
            {
                new() { Value = functionId.TypeId.Value },
                new() { Value = functionId.InstanceId.Value },
                new() { Value = expectedEpoch },
            }
        };

        var affectedRows = await command.ExecuteNonQueryAsync();
        return affectedRows == 1;
    }

    public async Task<bool> RestartExecution(FunctionId functionId, Tuple<StoredParameter, StoredScrapbook>? paramAndScrapbook, int expectedEpoch, long crashedCheckFrequency)
    {
        if (paramAndScrapbook == null)
        {
            await using var conn = await CreateConnection();

            var sql = @$"
            UPDATE {_tablePrefix}rfunctions
            SET epoch = epoch + 1, status = {(int) Status.Executing}, crashed_check_frequency = $1
            WHERE function_type_id = $2 AND function_instance_id = $3 AND epoch = $4";

            await using var command = new NpgsqlCommand(sql, conn)
            {
                Parameters =
                {
                    new() { Value = crashedCheckFrequency },
                    new() { Value = functionId.TypeId.Value },
                    new() { Value = functionId.InstanceId.Value },
                    new() { Value = expectedEpoch },
                }
            };

            var affectedRows = await command.ExecuteNonQueryAsync();
            return affectedRows == 1;
        }
        else
        {
            await using var conn = await CreateConnection();

            var sql = @$"
            UPDATE {_tablePrefix}rfunctions
            SET param_json = $1, param_type = $2,
                scrapbook_json = $3, scrapbook_type = $4,
                epoch = epoch + 1, status = {(int) Status.Executing},
                crashed_check_frequency = $5
            WHERE function_type_id = $6 AND function_instance_id = $7 AND epoch = $8";

            var (param, scrapbook) = paramAndScrapbook;
            await using var command = new NpgsqlCommand(sql, conn)
            {
                Parameters =
                {
                    new() { Value = param.ParamJson },
                    new() { Value = param.ParamType },
                    new() { Value = scrapbook.ScrapbookJson },
                    new() { Value = scrapbook.ScrapbookType },
                    new() { Value = crashedCheckFrequency },
                    new() { Value = functionId.TypeId.Value },
                    new() { Value = functionId.InstanceId.Value },
                    new() { Value = expectedEpoch },
                }
            };

            var affectedRows = await command.ExecuteNonQueryAsync();
            return affectedRows == 1;
        }
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
            Parameters =
            {
                new() {Value = functionTypeId.Value}
            }
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

    public async Task<IEnumerable<StoredEligibleSuspendedFunction>> GetEligibleSuspendedFunctions(FunctionTypeId functionTypeId)
    {
        await using var conn = await CreateConnection();
        var sql = @$"
            SELECT rf.function_instance_id, rf.epoch
            FROM {_tablePrefix}rfunctions AS rf
            INNER JOIN (
                SELECT events.function_instance_id, MAX(Position) + 1 AS events_count
                FROM {_tablePrefix}rfunctions_events AS events
                WHERE events.function_type_id = $1
                GROUP BY events.function_instance_id
            ) AS events 
                ON rf.function_instance_id = events.function_instance_id
            WHERE rf.function_type_id = $1 AND 
                  rf.status = {(int) Status.Suspended} AND
                  rf.suspend_until_eventsource_count <= events.events_count";
        await using var command = new NpgsqlCommand(sql, conn)
        {
            Parameters =
            {
                new() {Value = functionTypeId.Value},
            }
        };
        
        await using var reader = await command.ExecuteReaderAsync();
        var functions = new List<StoredEligibleSuspendedFunction>();
        while (await reader.ReadAsync())
        {
            var functionInstanceId = reader.GetString(0);
            var epoch = reader.GetInt32(1);
            functions.Add(new StoredEligibleSuspendedFunction(functionInstanceId, epoch));
        }

        return functions;
    }

    public async Task<Epoch?> IsFunctionSuspendedAndEligibleForReInvocation(FunctionId functionId)
    {
        await using var conn = await CreateConnection();
        var sql = @$"
            SELECT rf.epoch
            FROM {_tablePrefix}rfunctions AS rf
            INNER JOIN (
                SELECT MAX(Position) + 1 AS events_count
                FROM {_tablePrefix}rfunctions_events AS events
                WHERE events.function_type_id = $1 AND 
                      events.function_instance_id = $2
            ) AS events ON 1=1
            WHERE rf.function_type_id = $1 AND 
                  rf.function_instance_id = $2 AND
                  rf.status = {(int) Status.Suspended} AND
                  rf.suspend_until_eventsource_count <= events.events_count";
        await using var command = new NpgsqlCommand(sql, conn)
        {
            Parameters =
            {
                new() {Value = functionId.TypeId.Value},
                new() {Value = functionId.InstanceId.Value},
            }
        };
        
        await using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
            return new Epoch(reader.GetInt32(0));

        return null;
    }

    public async Task<bool> SetFunctionState(
        FunctionId functionId, Status status, 
        StoredParameter storedParameter, StoredScrapbook storedScrapbook, StoredResult storedResult, 
        StoredException? storedException, 
        long? postponeUntil, int expectedEpoch)
    {
        await using var conn = await CreateConnection();
        var sql = $@"
            UPDATE {_tablePrefix}rfunctions
            SET status = $1,
                param_json = $2, param_type = $3,
                scrapbook_json = $4, scrapbook_type = $5, 
                result_json = $6, result_type = $7, 
                exception_json = $8, postponed_until = $9,
                epoch = epoch + 1
            WHERE 
                function_type_id = $10 AND 
                function_instance_id = $11 AND 
                epoch = $12";
        await using var command = new NpgsqlCommand(sql, conn)
        {
            Parameters =
            {
                new() {Value = (int) status},
                new() {Value = storedParameter.ParamJson},
                new() {Value = storedParameter.ParamType},
                new() {Value = storedScrapbook.ScrapbookJson},
                new() {Value = storedScrapbook.ScrapbookType},
                new() {Value = storedResult.ResultJson ?? (object) DBNull.Value},
                new() {Value = storedResult.ResultType ?? (object) DBNull.Value},
                new() {Value = storedException == null ? DBNull.Value : JsonSerializer.Serialize(storedException)},
                new() {Value = postponeUntil ?? (object) DBNull.Value},
                new() {Value = functionId.TypeId.Value},
                new() {Value = functionId.InstanceId.Value},
                new() {Value = expectedEpoch},
            }
        };

        var affectedRows = await command.ExecuteNonQueryAsync();
        return affectedRows == 1;
    }

    public async Task<bool> SaveScrapbookForExecutingFunction(FunctionId functionId, string scrapbookJson, int expectedEpoch)
    {
        await using var conn = await CreateConnection();
        var sql = $@"
            UPDATE {_tablePrefix}rfunctions
            SET scrapbook_json = $1
            WHERE 
                function_type_id = $2 AND 
                function_instance_id = $3 AND 
                epoch = $4";
        await using var command = new NpgsqlCommand(sql, conn)
        {
            Parameters =
            {
                new() {Value = scrapbookJson},
                new() {Value = functionId.TypeId.Value},
                new() {Value = functionId.InstanceId.Value},
                new() {Value = expectedEpoch},
            }
        };

        var affectedRows = await command.ExecuteNonQueryAsync();
        return affectedRows == 1;
    }

    public async Task<bool> SetParameters(
        FunctionId functionId,
        StoredParameter storedParameter, StoredScrapbook storedScrapbook,
        int expectedEpoch)
    {
        await using var conn = await CreateConnection();
        var sql = $@"
            UPDATE {_tablePrefix}rfunctions
            SET param_json = $1, param_type = $2, scrapbook_json = $3, scrapbook_type = $4, epoch = epoch + 1
            WHERE function_type_id = $5 AND function_instance_id = $6 AND epoch = $7";
        
        var command = new NpgsqlCommand(sql, conn)
        {
            Parameters =
            {
                new() { Value = storedParameter.ParamJson },
                new() { Value = storedParameter.ParamType },
                new() { Value = storedScrapbook.ScrapbookJson },
                new() { Value = storedScrapbook.ScrapbookType },
                new() { Value = functionId.TypeId.Value },
                new() { Value = functionId.InstanceId.Value },
                new() { Value = expectedEpoch },
            }
        };

        await using var _ = command;
        var affectedRows = await command.ExecuteNonQueryAsync();

        return affectedRows == 1;
    }

    public async Task<bool> SucceedFunction(FunctionId functionId, StoredResult result, string scrapbookJson, int expectedEpoch)
    {
        await using var conn = await CreateConnection();
        var sql = $@"
            UPDATE {_tablePrefix}rfunctions
            SET status = {(int) Status.Succeeded}, result_json = $1, result_type = $2, scrapbook_json = $3
            WHERE 
                function_type_id = $4 AND 
                function_instance_id = $5 AND 
                epoch = $6";
        await using var command = new NpgsqlCommand(sql, conn)
        {
            Parameters =
            {
                new() {Value = result?.ResultJson ?? (object) DBNull.Value},
                new() {Value = result?.ResultType ?? (object) DBNull.Value},
                new() { Value = scrapbookJson },
                new() { Value = functionId.TypeId.Value },
                new() { Value = functionId.InstanceId.Value },
                new() { Value = expectedEpoch },
            }
        };
        
        var affectedRows = await command.ExecuteNonQueryAsync();
        return affectedRows == 1;
    }

    public async Task<bool> PostponeFunction(FunctionId functionId, long postponeUntil, string scrapbookJson, int expectedEpoch)
    {
        await using var conn = await CreateConnection();
        var sql = $@"
            UPDATE {_tablePrefix}rfunctions
            SET status = {(int) Status.Postponed}, postponed_until = $1, scrapbook_json = $2
            WHERE 
                function_type_id = $3 AND 
                function_instance_id = $4 AND 
                epoch = $5";
        await using var command = new NpgsqlCommand(sql, conn)
        {
            Parameters =
            {
                new() { Value = postponeUntil },
                new() { Value = scrapbookJson },
                new() { Value = functionId.TypeId.Value },
                new() { Value = functionId.InstanceId.Value },
                new() { Value = expectedEpoch },
            }
        };
        
        var affectedRows = await command.ExecuteNonQueryAsync();
        return affectedRows == 1;
    }
    
    public async Task<bool> SuspendFunction(FunctionId functionId, int suspendUntilEventSourceCountAtLeast, string scrapbookJson, int expectedEpoch)
    {
        await using var conn = await CreateConnection();
        var sql = $@"
            UPDATE {_tablePrefix}rfunctions
            SET status = {(int) Status.Suspended}, suspend_until_eventsource_count = $1, scrapbook_json = $2
            WHERE 
                function_type_id = $3 AND 
                function_instance_id = $4 AND 
                epoch = $5";
        await using var command = new NpgsqlCommand(sql, conn)
        {
            Parameters =
            {
                new() { Value = suspendUntilEventSourceCountAtLeast },
                new() { Value = scrapbookJson },
                new() { Value = functionId.TypeId.Value },
                new() { Value = functionId.InstanceId.Value },
                new() { Value = expectedEpoch },
            }
        };
        
        var affectedRows = await command.ExecuteNonQueryAsync();
        return affectedRows == 1;
    }

    public async Task<bool> FailFunction(FunctionId functionId, StoredException storedException, string scrapbookJson, int expectedEpoch)
    {
        await using var conn = await CreateConnection();
        var sql = $@"
            UPDATE {_tablePrefix}rfunctions
            SET status = {(int) Status.Failed}, exception_json = $1, scrapbook_json = $2
            WHERE 
                function_type_id = $3 AND 
                function_instance_id = $4 AND 
                epoch = $5";
        await using var command = new NpgsqlCommand(sql, conn)
        {
            Parameters =
            {
                new() { Value = JsonSerializer.Serialize(storedException) },
                new() { Value = scrapbookJson },
                new() { Value = functionId.TypeId.Value },
                new() { Value = functionId.InstanceId.Value },
                new() { Value = expectedEpoch },
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
                exception_json,
                postponed_until,
                suspend_until_eventsource_count,
                epoch, 
                sign_of_life,
                crashed_check_frequency
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
            var hasResult = !await reader.IsDBNullAsync(6);
            var hasException = !await reader.IsDBNullAsync(7);
            var postponedUntil = !await reader.IsDBNullAsync(8);
            var suspendedUntil = !await reader.IsDBNullAsync(9);
            return new StoredFunction(
                functionId,
                new StoredParameter(reader.GetString(0), reader.GetString(1)),
                Scrapbook: new StoredScrapbook(reader.GetString(2),reader.GetString(3)),
                Status: (Status) reader.GetInt32(4),
                Result: new StoredResult(
                    hasResult ? reader.GetString(5) : null, 
                    hasResult ? reader.GetString(6) : null
                ),
                Exception: !hasException ? null : JsonSerializer.Deserialize<StoredException>(reader.GetString(7)),
                postponedUntil ? reader.GetInt64(8) : null,
                suspendedUntil ? reader.GetInt32(9) : null,
                Epoch: reader.GetInt32(10),
                SignOfLife: reader.GetInt32(11),
                CrashedCheckFrequency: reader.GetInt64(12)
            );
        }

        return null;
    }

    public async Task<StoredFunctionStatus?> GetFunctionStatus(FunctionId functionId)
    {
        await using var conn = await CreateConnection();
        var sql = $@"
            SELECT status, epoch 
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
            return new StoredFunctionStatus(
                functionId,
                (Status)reader.GetInt32(1),
                Epoch: reader.GetInt32(2)
            );
            
        }

        return null;
    }

    public async Task<bool> DeleteFunction(FunctionId functionId, int? expectedEpoch = null)
    {
        await using var conn = await CreateConnection();
        var sql = @$"
            DELETE FROM {_tablePrefix}rfunctions
            WHERE function_type_id = $1
            AND function_instance_id = $2 ";
        
        if (expectedEpoch != null)
            sql += "AND epoch = $3";

        await using var command = new NpgsqlCommand(sql, conn)
        {
            Parameters =
            {
                new() {Value = functionId.TypeId.Value},
                new() {Value = functionId.InstanceId.Value},
            }
        };
        if (expectedEpoch != null)
            command.Parameters.Add(new() { Value = expectedEpoch.Value });

        var affectedRows = await command.ExecuteNonQueryAsync();
        return affectedRows == 1;
    }
}