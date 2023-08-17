﻿using System.Data;
using System.Text.Json;
using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Messaging;
using Cleipnir.ResilientFunctions.Storage;
using MySqlConnector;
using static Cleipnir.ResilientFunctions.MySQL.DatabaseHelper;

namespace Cleipnir.ResilientFunctions.MySQL;

public class MySqlFunctionStore : IFunctionStore
{
    private readonly string _connectionString;
    private readonly string _tablePrefix;

    private readonly MySqlEventStore _eventStore;
    public IEventStore EventStore => _eventStore;
    private readonly MySqlTimeoutStore _timeoutStore;
    public ITimeoutStore TimeoutStore => _timeoutStore;
    public Utilities Utilities { get; }
    private readonly MySqlUnderlyingRegister _mySqlUnderlyingRegister;

    public MySqlFunctionStore(string connectionString, string tablePrefix = "")
    {
        _connectionString = connectionString;
        _tablePrefix = tablePrefix;
        _eventStore = new MySqlEventStore(connectionString, tablePrefix);
        _timeoutStore = new MySqlTimeoutStore(connectionString, tablePrefix);
        _mySqlUnderlyingRegister = new(connectionString, _tablePrefix);
        Utilities = new Utilities(_mySqlUnderlyingRegister);
    }

    public async Task Initialize()
    {
        await _mySqlUnderlyingRegister.Initialize();
        await EventStore.Initialize();
        await TimeoutStore.Initialize();
        await using var conn = await CreateOpenConnection(_connectionString);
        var sql = $@"
            CREATE TABLE IF NOT EXISTS {_tablePrefix}rfunctions (
                function_type_id VARCHAR(200) NOT NULL,
                function_instance_id VARCHAR(200) NOT NULL,
                param_json TEXT NOT NULL,
                param_type VARCHAR(255) NOT NULL,
                scrapbook_json TEXT NOT NULL,
                scrapbook_type VARCHAR(255) NOT NULL,
                status INT NOT NULL,
                result_json TEXT NULL,
                result_type VARCHAR(255) NULL,
                exception_json TEXT NULL,
                postponed_until BIGINT NULL,
                epoch INT NOT NULL,
                lease_expiration BIGINT NOT NULL,
                PRIMARY KEY (function_type_id, function_instance_id),
                INDEX (function_type_id, status, function_instance_id)   
            );";

        await using var command = new MySqlCommand(sql, conn);
        await command.ExecuteNonQueryAsync();
    }

    public async Task DropIfExists()
    {
        await _eventStore.DropUnderlyingTable();
        await _mySqlUnderlyingRegister.DropUnderlyingTable();

        await using var conn = await CreateOpenConnection(_connectionString);
        var sql = $"DROP TABLE IF EXISTS {_tablePrefix}rfunctions";
        await using var command = new MySqlCommand(sql, conn);
        await command.ExecuteNonQueryAsync();
    }

    public async Task TruncateTables()
    {
        await _eventStore.TruncateTable();
        await _timeoutStore.TruncateTable();
        
        await using var conn = await CreateOpenConnection(_connectionString);
        var sql = $"TRUNCATE TABLE {_tablePrefix}rfunctions";
        await using var command = new MySqlCommand(sql, conn);
        await command.ExecuteNonQueryAsync();
    }
    
    public async Task<bool> CreateFunction(
        FunctionId functionId, 
        StoredParameter param, 
        StoredScrapbook storedScrapbook, 
        IEnumerable<StoredEvent>? storedEvents,
        long leaseExpiration,
        long? postponeUntil)
    {
        await using var conn = await CreateOpenConnection(_connectionString);
        MySqlTransaction? transaction = null;
        if (storedEvents != null)
        {
            transaction = await conn.BeginTransactionAsync();
            await _eventStore.AppendEvents(functionId, storedEvents, existingCount: 0, conn, transaction);
        }

        var status = postponeUntil == null ? Status.Executing : Status.Postponed;
        var sql = @$"
            INSERT IGNORE INTO {_tablePrefix}rfunctions
                (function_type_id, function_instance_id, param_json, param_type, scrapbook_json, scrapbook_type, status, epoch, lease_expiration, postponed_until)
            VALUES
                (?, ?, ?, ?, ?, ?, {(int) status}, 0, ?, ?)";
        await using var command = new MySqlCommand(sql, conn, transaction)
        {
            Parameters =
            {
                new() {Value = functionId.TypeId.Value},
                new() {Value = functionId.InstanceId.Value},
                new() {Value = param.ParamJson},
                new() {Value = param.ParamType},
                new() {Value = storedScrapbook.ScrapbookJson},
                new() {Value = storedScrapbook.ScrapbookType},
                new() {Value = leaseExpiration},
                new() {Value = postponeUntil}
            }
        };

        var affectedRows = await command.ExecuteNonQueryAsync();
        await (transaction?.CommitAsync() ?? Task.CompletedTask);
        return affectedRows == 1;
    }

    public async Task<bool> IncrementAlreadyPostponedFunctionEpoch(FunctionId functionId, int expectedEpoch)
    {
        await using var conn = await CreateOpenConnection(_connectionString);
        var sql = @$"
            UPDATE {_tablePrefix}rfunctions
            SET epoch = epoch + 1
            WHERE function_type_id = ? AND function_instance_id = ? AND epoch = ?";

        await using var command = new MySqlCommand(sql, conn)
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

    public async Task<bool> RestartExecution(FunctionId functionId, int expectedEpoch, long leaseExpiration)
    {
        await using var conn = await CreateOpenConnection(_connectionString);
        var sql = @$"
            UPDATE {_tablePrefix}rfunctions
            SET epoch = epoch + 1, status = {(int)Status.Executing}, lease_expiration = ?
            WHERE function_type_id = ? AND function_instance_id = ? AND epoch = ?";

        await using var command = new MySqlCommand(sql, conn)
        {
            Parameters =
            {
                new() { Value = leaseExpiration },
                new() { Value = functionId.TypeId.Value },
                new() { Value = functionId.InstanceId.Value },
                new() { Value = expectedEpoch },
            }
        };

        var affectedRows = await command.ExecuteNonQueryAsync();
        return affectedRows == 1;
    }

    public async Task<bool> RenewLease(FunctionId functionId, int expectedEpoch, long leaseExpiration)
    {
        await using var conn = await CreateOpenConnection(_connectionString);
        var sql = $@"
            UPDATE {_tablePrefix}rfunctions
            SET lease_expiration = ?
            WHERE function_type_id = ? AND function_instance_id = ? AND epoch = ?";
        await using var command = new MySqlCommand(sql, conn)
        {
            Parameters =
            {
                new() {Value = leaseExpiration},
                new() {Value = functionId.TypeId.Value},
                new() {Value = functionId.InstanceId.Value},
                new() {Value = expectedEpoch},
            }
        };

        var affectedRows = await command.ExecuteNonQueryAsync();
        return affectedRows == 1;
    }

    public async Task<IEnumerable<StoredExecutingFunction>> GetCrashedFunctions(FunctionTypeId functionTypeId, long leaseExpiresBefore)
    {
        await using var conn = await CreateOpenConnection(_connectionString);
        var sql = @$"
            SELECT function_instance_id, epoch, lease_expiration 
            FROM {_tablePrefix}rfunctions
            WHERE function_type_id = ? AND lease_expiration < ? AND status = {(int) Status.Executing}";
        await using var command = new MySqlCommand(sql, conn)
        {
            Parameters =
            {
                new() {Value = functionTypeId.Value},
                new() { Value = leaseExpiresBefore }
            }
        };

        await using var reader = await command.ExecuteReaderAsync();

        var functions = new List<StoredExecutingFunction>();
        while (await reader.ReadAsync())
        {
            var functionInstanceId = reader.GetString(0);
            var epoch = reader.GetInt32(1);
            var expiration = reader.GetInt64(2);
            functions.Add(new StoredExecutingFunction(functionInstanceId, epoch, expiration));
        }

        return functions;
    }

    public async Task<IEnumerable<StoredPostponedFunction>> GetPostponedFunctions(FunctionTypeId functionTypeId, long expiresBefore)
    {
        await using var conn = await CreateOpenConnection(_connectionString);
        var sql = @$"
            SELECT function_instance_id, epoch, postponed_until
            FROM {_tablePrefix}rfunctions
            WHERE function_type_id = ? AND status = {(int) Status.Postponed} AND postponed_until <= ?";
        await using var command = new MySqlCommand(sql, conn)
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
        FunctionId functionId, Status status, 
        StoredParameter storedParameter, StoredScrapbook storedScrapbook, StoredResult storedResult, 
        StoredException? storedException, 
        long? postponeUntil,
        ReplaceEvents? events,
        int expectedEpoch)
    {
        await using var conn = await CreateOpenConnection(_connectionString);
        await using var transaction = events != null
            ? await conn.BeginTransactionAsync(IsolationLevel.RepeatableRead)
            : default;    
        
        var sql = $@"
            UPDATE {_tablePrefix}rfunctions
            SET status = ?, 
                param_json = ?, param_type = ?, 
                scrapbook_json = ?, scrapbook_type = ?, 
                result_json = ?, result_type = ?, 
                exception_json = ?, postponed_until = ?,
                epoch = epoch + 1
            WHERE 
                function_type_id = ? AND 
                function_instance_id = ? AND 
                epoch = ?";
        await using var command = new MySqlCommand(sql, conn, transaction)
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
                new() {Value = storedException != null ? JsonSerializer.Serialize(storedException) : DBNull.Value},
                new() {Value = postponeUntil ?? (object) DBNull.Value},
                new() {Value = functionId.TypeId.Value},
                new() {Value = functionId.InstanceId.Value},
                new() {Value = expectedEpoch},
            }
        };

        var affectedRows = await command.ExecuteNonQueryAsync();
        if (affectedRows == 0 || transaction == null) 
            return affectedRows == 1;

        var (storedEvents, existingCount) = events!;
        affectedRows = await _eventStore.Truncate(functionId, conn, transaction);
        if (affectedRows != existingCount)
            return false;
        await _eventStore.AppendEvents(functionId, storedEvents, await _eventStore.GetNumberOfEvents(functionId), conn, transaction);

        await transaction.CommitAsync();
        return true;
    }

    public async Task<bool> SaveScrapbookForExecutingFunction( 
        FunctionId functionId,
        string scrapbookJson,
        int expectedEpoch,
        ComplimentaryState.SaveScrapbookForExecutingFunction _)
    {
        await using var conn = await CreateOpenConnection(_connectionString);
        var sql = $@"
            UPDATE {_tablePrefix}rfunctions
            SET scrapbook_json = ?
            WHERE 
                function_type_id = ? AND 
                function_instance_id = ? AND 
                epoch = ?";
        await using var command = new MySqlCommand(sql, conn)
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
        ReplaceEvents? events,
        bool suspended,
        int expectedEpoch)
    {
        await using var conn = await CreateOpenConnection(_connectionString);
        await using var transaction = events != null
            ? await conn.BeginTransactionAsync()
            : null;
        
        var sql = $@"
            UPDATE {_tablePrefix}rfunctions
            SET param_json = ?, param_type = ?, scrapbook_json = ?, scrapbook_type = ?, epoch = epoch + 1
            WHERE 
                function_type_id = ? AND 
                function_instance_id = ? AND 
                epoch = ?";

        var command = transaction == null
            ? new MySqlCommand(sql, conn)
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
            }
            : new MySqlCommand(sql, conn, transaction)
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
        if (affectedRows == 0 || transaction == null)
            return affectedRows == 1;

        var (storedEvents, existingCount) = events!;
        affectedRows = await _eventStore.Truncate(functionId, conn, transaction);
        if (affectedRows != existingCount)
            return false;
        
        await _eventStore.AppendEvents(functionId, storedEvents, existingCount, conn, transaction);

        await transaction.CommitAsync();
        return true;
    }

    public async Task<bool> SucceedFunction(FunctionId functionId, StoredResult result, string scrapbookJson, int expectedEpoch, ComplimentaryState.SetResult _)
    {
        await using var conn = await CreateOpenConnection(_connectionString);
        var sql = $@"
            UPDATE {_tablePrefix}rfunctions
            SET status = {(int) Status.Succeeded}, result_json = ?, result_type = ?, scrapbook_json = ?
            WHERE 
                function_type_id = ? AND 
                function_instance_id = ? AND 
                epoch = ?";
        
        await using var command = new MySqlCommand(sql, conn)
        {
            Parameters =
            {
                new() {Value = result?.ResultJson ?? (object) DBNull.Value},
                new() {Value = result?.ResultType ?? (object) DBNull.Value},
                new() {Value = scrapbookJson},
                new() {Value = functionId.TypeId.Value},
                new() {Value = functionId.InstanceId.Value},
                new() {Value = expectedEpoch},
            }
        };
        
        var affectedRows = await command.ExecuteNonQueryAsync();
        return affectedRows == 1;
    }

    public async Task<bool> PostponeFunction(FunctionId functionId, long postponeUntil, string scrapbookJson, int expectedEpoch, ComplimentaryState.SetResult _)
    {
        await using var conn = await CreateOpenConnection(_connectionString);
        var sql = $@"
            UPDATE {_tablePrefix}rfunctions
            SET status = {(int) Status.Postponed}, postponed_until = ?, scrapbook_json = ?
            WHERE 
                function_type_id = ? AND 
                function_instance_id = ? AND 
                epoch = ?";
        
        await using var command = new MySqlCommand(sql, conn)
        {
            Parameters =
            {
                new() {Value = postponeUntil},
                new() {Value = scrapbookJson},
                new() {Value = functionId.TypeId.Value},
                new() {Value = functionId.InstanceId.Value},
                new() {Value = expectedEpoch},
            }
        };
        
        var affectedRows = await command.ExecuteNonQueryAsync();
        return affectedRows == 1;
    }
    
    public async Task<SuspensionResult> SuspendFunction(FunctionId functionId, int expectedEventCount, string scrapbookJson, int expectedEpoch, ComplimentaryState.SetResult _)
    {
        await using var conn = await CreateOpenConnection(_connectionString);
        await using var transaction = await conn.BeginTransactionAsync(IsolationLevel.Serializable);
        var sql = $@"
            UPDATE {_tablePrefix}rfunctions
            SET status = {(int) Status.Suspended}, scrapbook_json = ?
            WHERE 
                function_type_id = ? AND 
                function_instance_id = ? AND 
                epoch = ? AND
                (SELECT COALESCE(MAX(position), -1) + 1 FROM {_tablePrefix}rfunctions_events WHERE function_type_id = ? AND function_instance_id = ?) = ?";
        
        await using var command = new MySqlCommand(sql, conn, transaction)
        {
            Parameters =
            {
                new() {Value = scrapbookJson},
                new() {Value = functionId.TypeId.Value},
                new() {Value = functionId.InstanceId.Value},
                new() {Value = expectedEpoch},
                new() {Value = functionId.TypeId.Value},
                new() {Value = functionId.InstanceId.Value},
                new() {Value = expectedEventCount},
            }
        };
        
        var affectedRows = await command.ExecuteNonQueryAsync();
        await transaction.CommitAsync();
        
        if (affectedRows == 1)
            return SuspensionResult.Success;

        var sf = await GetFunction(functionId);
        return sf?.Epoch != expectedEpoch
            ? SuspensionResult.ConcurrentStateModification
            : SuspensionResult.EventCountMismatch;
    }

    public async Task<StatusAndEpoch?> GetFunctionStatus(FunctionId functionId)
    {
        await using var conn = await CreateOpenConnection(_connectionString);
        var sql = $@"
            SELECT status, epoch
            FROM {_tablePrefix}rfunctions
            WHERE function_type_id = ? AND function_instance_id = ?;";
        await using var command = new MySqlCommand(sql, conn)
        {
            Parameters = { 
                new() {Value = functionId.TypeId.Value},
                new() {Value = functionId.InstanceId.Value}
            }
        };
        
        await using var reader = await command.ExecuteReaderAsync();
        
        while (await reader.ReadAsync())
        {
            return new StatusAndEpoch(
                Status: (Status) reader.GetInt32(0),
                Epoch: reader.GetInt32(1)
            );
        }

        return null;
    }

    public async Task<bool> FailFunction(FunctionId functionId, StoredException storedException, string scrapbookJson, int expectedEpoch, ComplimentaryState.SetResult _)
    {
        await using var conn = await CreateOpenConnection(_connectionString);
        var sql = $@"
            UPDATE {_tablePrefix}rfunctions
            SET status = {(int) Status.Failed}, exception_json = ?, scrapbook_json = ?
            WHERE 
                function_type_id = ? AND 
                function_instance_id = ? AND 
                epoch = ?";
        
        await using var command = new MySqlCommand(sql, conn)
        {
            Parameters =
            {
                new() {Value = JsonSerializer.Serialize(storedException)},
                new() {Value = scrapbookJson},
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
        await using var conn = await CreateOpenConnection(_connectionString);
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
                epoch, 
                lease_expiration
            FROM {_tablePrefix}rfunctions
            WHERE function_type_id = ? AND function_instance_id = ?;";
        await using var command = new MySqlCommand(sql, conn)
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
            var hasError = !await reader.IsDBNullAsync(7);
            var storedException = hasError
                ? JsonSerializer.Deserialize<StoredException>(reader.GetString(7))
                : null;
            var postponedUntil = !await reader.IsDBNullAsync(8);
            return new StoredFunction(
                functionId,
                new StoredParameter(reader.GetString(0), reader.GetString(1)),
                Scrapbook: new StoredScrapbook(reader.GetString(2), reader.GetString(3)),
                Status: (Status) reader.GetInt32(4),
                Result: new StoredResult(
                    hasResult ? reader.GetString(5) : null, 
                    hasResult ? reader.GetString(6) : null
                ),
                storedException,
                postponedUntil ? reader.GetInt64(8) : null,
                Epoch: reader.GetInt32(9),
                LeaseExpiration: reader.GetInt64(10)
            );
        }

        return null;
    }

    public async Task<bool> DeleteFunction(FunctionId functionId, int? expectedEpoch = null)
    {
        await using var conn = await CreateOpenConnection(_connectionString);
        
        var sql = $@"
            START TRANSACTION;
            DELETE FROM {_tablePrefix}rfunctions_events
            WHERE 
                function_type_id = ? AND 
                function_instance_id = ?;
            DELETE FROM {_tablePrefix}rfunctions
            WHERE 
                function_type_id = ? AND 
                function_instance_id = ?";
        
        if (expectedEpoch != null)
            sql += " AND epoch = ? ";

        sql += "; COMMIT";
        
        await using var command = new MySqlCommand(sql, conn)
        {
            Parameters =
            {
                new() {Value = functionId.TypeId.Value},
                new() {Value = functionId.InstanceId.Value},
                new() {Value = functionId.TypeId.Value},
                new() {Value = functionId.InstanceId.Value}
            }
        };
        if (expectedEpoch != null)
            command.Parameters.Add(new() { Value = expectedEpoch.Value });

        var affectedRows = await command.ExecuteNonQueryAsync();
        return affectedRows > 0;
    }
}