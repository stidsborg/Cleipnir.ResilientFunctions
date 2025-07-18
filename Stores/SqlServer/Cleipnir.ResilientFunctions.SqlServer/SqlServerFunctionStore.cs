﻿using System;
using System.Collections.Generic;
using System.Data.SqlTypes;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Messaging;
using Cleipnir.ResilientFunctions.Storage;
using Microsoft.Data.SqlClient;

namespace Cleipnir.ResilientFunctions.SqlServer;

public class SqlServerFunctionStore : IFunctionStore
{
    private readonly Func<Task<SqlConnection>> _connFunc;
    private readonly string _tableName;

    private readonly SqlServerTimeoutStore _timeoutStore;
    private readonly SqlServerEffectsStore _effectsStore;
    private readonly SqlServerMessageStore _messageStore;
    private readonly SqlServerCorrelationsStore _correlationStore;
    private readonly SqlServerTypeStore _typeStore;
    private readonly SqlServerMigrator _migrator;
    
    public IEffectsStore EffectsStore => _effectsStore;
    public ITimeoutStore TimeoutStore => _timeoutStore;
    public ICorrelationStore CorrelationStore => _correlationStore;
    public ITypeStore TypeStore => _typeStore;
    public IMessageStore MessageStore => _messageStore;
    public Utilities Utilities { get; }
    public IMigrator Migrator => _migrator;
    private readonly SqlServerSemaphoreStore _semaphoreStore;
    public ISemaphoreStore SemaphoreStore => _semaphoreStore;
    private readonly SqlServerReplicaStore _replicaStore;
    public IReplicaStore ReplicaStore => _replicaStore;

    private readonly SqlServerUnderlyingRegister _underlyingRegister;
    
    private readonly SqlGenerator _sqlGenerator;

    public SqlServerFunctionStore(string connectionString, string tablePrefix = "")
    {
        _tableName = tablePrefix == "" ? "RFunctions" : tablePrefix;
        _sqlGenerator = new SqlGenerator(_tableName);
        
        _connFunc = CreateConnection(connectionString);
        _messageStore = new SqlServerMessageStore(connectionString, _sqlGenerator, _tableName);
        _timeoutStore = new SqlServerTimeoutStore(connectionString, _tableName);
        _underlyingRegister = new SqlServerUnderlyingRegister(connectionString, _tableName);
        _effectsStore = new SqlServerEffectsStore(connectionString, _sqlGenerator, _tableName);
        _correlationStore = new SqlServerCorrelationsStore(connectionString, _tableName);
        _semaphoreStore = new SqlServerSemaphoreStore(connectionString, _tableName);
        _typeStore = new SqlServerTypeStore(connectionString, _tableName);
        _migrator = new SqlServerMigrator(connectionString, _tableName);
        _replicaStore = new SqlServerReplicaStore(connectionString, _tableName);
        Utilities = new Utilities(_underlyingRegister);
    }
    
    private static Func<Task<SqlConnection>> CreateConnection(string connectionString)
    {
        return async () =>
        {
            var connection = new SqlConnection(connectionString);
            await connection.OpenAsync();
            return connection;
        };
    }

    private string? _initializeSql;
    public async Task Initialize()
    {
        var createTables = await _migrator.InitializeAndMigrate();
        if (!createTables)
            return;
        
        await _underlyingRegister.Initialize();
        await _messageStore.Initialize();
        await _effectsStore.Initialize();
        await _timeoutStore.Initialize();
        await _correlationStore.Initialize();
        await _typeStore.Initialize();
        await _semaphoreStore.Initialize();
        await _replicaStore.Initialize();
        await using var conn = await _connFunc();
        _initializeSql ??= @$"    
            CREATE TABLE {_tableName} (
                FlowType INT NOT NULL,
                FlowInstance UNIQUEIDENTIFIER NOT NULL,
                Status INT NOT NULL,
                Epoch INT NOT NULL,               
                Expires BIGINT NOT NULL,
                Interrupted BIT NOT NULL DEFAULT 0,
                ParamJson VARBINARY(MAX) NULL,                                        
                ResultJson VARBINARY(MAX) NULL,
                ExceptionJson NVARCHAR(MAX) NULL,
                HumanInstanceId NVARCHAR(MAX) NOT NULL,                                                                        
                Timestamp BIGINT NOT NULL,
                Parent NVARCHAR(MAX) NULL,
                Owner UNIQUEIDENTIFIER NULL,
                PRIMARY KEY (FlowType, FlowInstance)
            );
            CREATE INDEX {_tableName}_idx_Executing
                ON {_tableName} (Expires, FlowType, FlowInstance)
                INCLUDE (Epoch)
                WHERE Status = {(int)Status.Executing};    
            CREATE INDEX {_tableName}_idx_Owners
                ON {_tableName} (Owner, FlowType, FlowInstance)                
                WHERE Status = {(int)Status.Executing};  
            CREATE INDEX {_tableName}_idx_Postponed
                ON {_tableName} (Expires, FlowType, FlowInstance)
                INCLUDE (Epoch)
                WHERE Status = {(int)Status.Postponed};
            CREATE INDEX {_tableName}_idx_Succeeded
                ON {_tableName} (FlowType, FlowInstance)
                WHERE Status = {(int)Status.Succeeded};";

        await using var command = new SqlCommand(_initializeSql, conn);
        try
        {
            await command.ExecuteNonQueryAsync();    
        } catch (SqlException exception) when (exception.Number == 2714) {}
    }

    private string? _truncateSql;
    public async Task TruncateTables()
    {
        await _underlyingRegister.TruncateTable();
        await _messageStore.TruncateTable();
        await _timeoutStore.Truncate();
        await _effectsStore.Truncate();
        await _correlationStore.Truncate();
        await _typeStore.Truncate();
        await _semaphoreStore.Truncate();
        await _replicaStore.Truncate();
        
        await using var conn = await _connFunc();
        _truncateSql ??= $"TRUNCATE TABLE {_tableName}";
        await using var command = new SqlCommand(_truncateSql, conn);
        await command.ExecuteNonQueryAsync();
    }
    
    public async Task<bool> CreateFunction(
        StoredId storedId, 
        FlowInstance humanInstanceId,
        byte[]? param, 
        long leaseExpiration,
        long? postponeUntil,
        long timestamp,
        StoredId? parent,
        ReplicaId? owner,
        IReadOnlyList<StoredEffect>? effects = null, 
        IReadOnlyList<StoredMessage>? messages = null
    )
    {
        await using var conn = await _connFunc();
        
        try
        {
            var storeCommand = _sqlGenerator
                .CreateFunction(
                    storedId,
                    humanInstanceId,
                    param,
                    leaseExpiration,
                    postponeUntil,
                    timestamp,
                    parent,
                    owner,
                    paramPrefix: null
                );

            if (messages?.Any() ?? false)
            {
                var messagesCommand = _sqlGenerator.AppendMessages(
                    messages.Select((msg, position) => new StoredIdAndMessageWithPosition(storedId, msg, position)).ToList(),
                    interrupt: false,
                    prefix: "Message"
                );
                storeCommand = storeCommand.Merge(messagesCommand);
            }

            if (effects?.Any() ?? false)
            {
                var effectsCommand = _sqlGenerator.UpdateEffects(
                    effects.Select(e => new StoredEffectChange(storedId, e.StoredEffectId, CrudOperation.Insert, e)).ToList(),
                    paramPrefix: "Effect"
                );
                storeCommand = storeCommand.Merge(effectsCommand);
            }
            
            await using var command = storeCommand.ToSqlCommand(conn);
            await command.ExecuteNonQueryAsync();
        }
        catch (SqlException sqlException) when (sqlException.Number == SqlError.UNIQUENESS_VIOLATION)
        {
            return false;
        }

        return true;
    }

    private string? _bulkScheduleFunctionsSql;
    public async Task BulkScheduleFunctions(IEnumerable<IdWithParam> functionsWithParam, StoredId? parent)
    {
        _bulkScheduleFunctionsSql ??= @$"
            MERGE INTO {_tableName}
            USING (VALUES @VALUES) 
            AS source (
                FlowType, 
                FlowInstance, 
                ParamJson, 
                Status,
                Epoch, 
                Expires,
                Timestamp,
                HumanInstanceId,
                Parent
            )
            ON {_tableName}.FlowType = source.FlowType AND {_tableName}.flowInstance = source.flowInstance         
            WHEN NOT MATCHED THEN
              INSERT (FlowType, FlowInstance, ParamJson, Status, Epoch, Expires, Timestamp, HumanInstanceId, Parent)
              VALUES (source.FlowType, source.flowInstance, source.ParamJson, source.Status, source.Epoch, source.Expires, source.Timestamp, source.HumanInstanceId, source.Parent);";

        var parentStr = parent == null ? "NULL" : $"'{parent}'";
        var valueSql = $"(@FlowType, @FlowInstance, @ParamJson, {(int)Status.Postponed}, 0, 0, 0, @HumanInstanceId, {parentStr})";
        var chunk = functionsWithParam
            .Select(
                (fp, i) =>
                {
                    var sql = valueSql
                        .Replace("@FlowType", $"@FlowType{i}")
                        .Replace("@FlowInstance", $"@FlowInstance{i}")
                        .Replace("@ParamJson", $"@ParamJson{i}")
                        .Replace("@HumanInstanceId", $"@HumanInstanceId{i}");

                    return new { Id = i, Sql = sql, FunctionId = fp.StoredId, Param = fp.Param, HumanInstanceId = fp.HumanInstanceId };
                }).Chunk(100);

        await using var conn = await _connFunc();
        foreach (var idAndSqls in chunk)
        {
            var valuesSql = string.Join($",{Environment.NewLine}", idAndSqls.Select(a => a.Sql));
            var sql = _bulkScheduleFunctionsSql.Replace("@VALUES", valuesSql);
            
            await using var command = new SqlCommand(sql, conn);
            foreach (var idAndSql in idAndSqls)
            {
                command.Parameters.AddWithValue($"@FlowType{idAndSql.Id}", idAndSql.FunctionId.Type.Value);
                command.Parameters.AddWithValue($"@FlowInstance{idAndSql.Id}", idAndSql.FunctionId.Instance.Value);
                command.Parameters.AddWithValue($"@ParamJson{idAndSql.Id}", idAndSql.Param == null ? SqlBinary.Null : idAndSql.Param);
                command.Parameters.AddWithValue($"@HumanInstanceId{idAndSql.Id}", idAndSql.HumanInstanceId);
            }
            
            await command.ExecuteNonQueryAsync();
        }
    }
    
    public async Task<StoredFlowWithEffectsAndMessages?> RestartExecution(StoredId storedId, int expectedEpoch, long leaseExpiration, ReplicaId replicaId)
    {
        var restartCommand = _sqlGenerator.RestartExecution(storedId, expectedEpoch, leaseExpiration, replicaId);
        var effectsCommand = _sqlGenerator.GetEffects(storedId, paramPrefix: "Effect");
        var messagesCommand = _sqlGenerator.GetMessages(storedId, skip: 0, paramPrefix: "Message");

        await using var conn = await _connFunc();
        await using var command = StoreCommand
            .Merge(restartCommand, effectsCommand, messagesCommand)
            .ToSqlCommand(conn);
        
        await using var reader = await command.ExecuteReaderAsync();
        var sf = _sqlGenerator.ReadToStoredFlow(storedId, reader);
        if (sf?.Epoch != expectedEpoch + 1)
            return null;
    
        await reader.NextResultAsync();
        var effects = await _sqlGenerator.ReadEffects(reader);

        await reader.NextResultAsync();
        var messages = await _sqlGenerator.ReadMessages(reader);

        return new StoredFlowWithEffectsAndMessages(sf, effects, messages);
    }
    
    public async Task<int> RenewLeases(IReadOnlyList<LeaseUpdate> leaseUpdates, long leaseExpiration)
    {
        await using var conn = await _connFunc();

        var predicates = leaseUpdates
            .Select(u =>
                $"(FlowType = {u.StoredId.Type.Value} AND FlowInstance = '{u.StoredId.Instance.Value}' AND Epoch = {u.ExpectedEpoch})"
            ).StringJoin(" OR " + Environment.NewLine);

        var sql = @$"
            UPDATE {_tableName}
            SET Expires = {leaseExpiration}
            WHERE {predicates}";
        
        await using var command = new SqlCommand(sql, conn);
        return await command.ExecuteNonQueryAsync();
    }

    private string? _getExpiredFunctionsSql;
    public async Task<IReadOnlyList<IdAndEpoch>> GetExpiredFunctions(long expiresBefore)
    {
        await using var conn = await _connFunc();
        _getExpiredFunctionsSql ??= @$"
            SELECT FlowType, FlowInstance, Epoch
            FROM {_tableName} WITH (NOLOCK) 
            WHERE Expires <= @Expires AND (Status = { (int)Status.Executing } OR Status = { (int)Status.Postponed})";

        await using var command = new SqlCommand(_getExpiredFunctionsSql, conn);
        command.Parameters.AddWithValue("@Expires", expiresBefore);

        await using var reader = await command.ExecuteReaderAsync();
        var rows = new List<IdAndEpoch>(); 
        while (reader.HasRows)
        {
            while (reader.Read())
            {
                var flowType = reader.GetInt32(0);
                var flowInstance = reader.GetGuid(1);
                var flowId = new StoredId(new StoredType(flowType), flowInstance.ToStoredInstance());
                var epoch = reader.GetInt32(2);
                rows.Add(new IdAndEpoch(flowId, epoch));    
            }

            reader.NextResult();
        }

        return rows;
    }

    private string? _getSucceededFunctionsSql;
    public async Task<IReadOnlyList<StoredInstance>> GetSucceededFunctions(StoredType storedType, long completedBefore)
    {
        await using var conn = await _connFunc();
        _getSucceededFunctionsSql ??= @$"
            SELECT FlowInstance
            FROM {_tableName} 
            WHERE FlowType = @FlowType 
              AND Status = {(int) Status.Succeeded} 
              AND Timestamp <= @CompletedBefore";

        await using var command = new SqlCommand(_getSucceededFunctionsSql, conn);
        command.Parameters.AddWithValue("@FlowType", storedType.Value);
        command.Parameters.AddWithValue("@CompletedBefore", completedBefore);

        await using var reader = await command.ExecuteReaderAsync();
        var storedInstances = new List<StoredInstance>(); 
        while (reader.HasRows)
        {
            while (reader.Read())
            {
                var storedInstance = reader.GetGuid(0).ToStoredInstance();
                storedInstances.Add(storedInstance);    
            }

            reader.NextResult();
        }

        return storedInstances;
    }

    private string? _setFunctionStateSql;
    public async Task<bool> SetFunctionState(
        StoredId storedId, Status status, 
        byte[]? param, byte[]? result, 
        StoredException? storedException, 
        long expires,
        int expectedEpoch)
    {
        await using var conn = await _connFunc();
    
        _setFunctionStateSql ??= @$"
            UPDATE {_tableName}
            SET
                Status = @Status,
                ParamJson = @ParamJson,             
                ResultJson = @ResultJson,
                ExceptionJson = @ExceptionJson,
                Expires = @Expires,
                Epoch = Epoch + 1
            WHERE FlowType = @FlowType
            AND FlowInstance = @FlowInstance
            AND Epoch = @ExpectedEpoch";
        
        await using var command = new SqlCommand(_setFunctionStateSql, conn);
        command.Parameters.AddWithValue("@Status", (int) status);
        command.Parameters.AddWithValue("@ParamJson", param == null ? SqlBinary.Null : param);
        command.Parameters.AddWithValue("@ResultJson", result == null ? SqlBinary.Null : result);
        var exceptionJson = storedException == null ? null : JsonSerializer.Serialize(storedException);
        command.Parameters.AddWithValue("@ExceptionJson", exceptionJson ?? (object) DBNull.Value);
        command.Parameters.AddWithValue("@Expires", expires);
        command.Parameters.AddWithValue("@FlowInstance", storedId.Instance.Value);
        command.Parameters.AddWithValue("@FlowType", storedId.Type.Value);
        command.Parameters.AddWithValue("@ExpectedEpoch", expectedEpoch);

        var affectedRows = await command.ExecuteNonQueryAsync();
        return affectedRows > 0;
    }
    
    public async Task<bool> SucceedFunction(
        StoredId storedId, 
        byte[]? result, 
        long timestamp,
        int expectedEpoch, 
        IReadOnlyList<StoredEffect>? effects,
        IReadOnlyList<StoredMessage>? messages,
        ComplimentaryState complimentaryState)
    {
        await using var conn = await _connFunc();
        await using var command = _sqlGenerator
            .SucceedFunction(
                storedId,
                result,
                timestamp,
                expectedEpoch,
                paramPrefix: ""
            ).ToSqlCommand(conn);
        
        var affectedRows = await command.ExecuteNonQueryAsync();
        return affectedRows > 0;
    }
    
    public async Task<bool> PostponeFunction(
        StoredId storedId, 
        long postponeUntil, 
        long timestamp,
        bool ignoreInterrupted,
        int expectedEpoch, 
        IReadOnlyList<StoredEffect>? effects,
        IReadOnlyList<StoredMessage>? messages,
        ComplimentaryState complimentaryState)
    {
        await using var conn = await _connFunc();
        await using var command = _sqlGenerator.PostponeFunction(
            storedId,
            postponeUntil,
            timestamp,
            ignoreInterrupted,
            expectedEpoch,
            paramPrefix: ""
        ).ToSqlCommand(conn);

        var affectedRows = await command.ExecuteNonQueryAsync();
        return affectedRows > 0;
    }
    
    public async Task<bool> FailFunction(
        StoredId storedId, 
        StoredException storedException, 
        long timestamp,
        int expectedEpoch, 
        IReadOnlyList<StoredEffect>? effects,
        IReadOnlyList<StoredMessage>? messages,
        ComplimentaryState complimentaryState)
    {
        await using var conn = await _connFunc();
        await using var command = _sqlGenerator
            .FailFunction(
                storedId,
                storedException,
                timestamp,
                expectedEpoch,
                paramPrefix: ""
            ).ToSqlCommand(conn);
        

        var affectedRows = await command.ExecuteNonQueryAsync();
        return affectedRows > 0;
    }
    
    public async Task<bool> SuspendFunction(
        StoredId storedId, 
        long timestamp,
        int expectedEpoch,
        IReadOnlyList<StoredEffect>? effects,
        IReadOnlyList<StoredMessage>? messages,
        ComplimentaryState complimentaryState)
    {
        await using var conn = await _connFunc();
        await using var command = _sqlGenerator
            .SuspendFunction(
                storedId,
                timestamp,
                expectedEpoch,
                paramPrefix: ""
            ).ToSqlCommand(conn); 

        var affectedRows = await command.ExecuteNonQueryAsync();
        return affectedRows == 1;
    }

    private string? _getReplicasSql;
    public async Task<IReadOnlyList<ReplicaId>> GetOwnerReplicas()
    {
        await using var conn = await _connFunc();
        _getReplicasSql ??= @$"
            SELECT DISTINCT(Owner)
            FROM {_tableName}
            WHERE Status = {(int) Status.Executing} AND Owner IS NOT NULL";
        
        await using var command = new SqlCommand(_getReplicasSql, conn);
        
        await using var reader = await command.ExecuteReaderAsync();
        var replicas = new List<ReplicaId>();
        while (reader.Read())
            replicas.Add(reader.GetGuid(0).ToReplicaId());
        
        return replicas;
    }

    private string? _rescheduleFunctionsSql;
    public async Task RescheduleCrashedFunctions(ReplicaId replicaId)
    {
        await using var conn = await _connFunc();
        _rescheduleFunctionsSql ??= @$"
                UPDATE {_tableName}
                SET 
                    Owner = NULL,
                    Status = {(int) Status.Postponed},
                    Expires = 0,
                    Epoch = Epoch + 1
                WHERE Owner = @Owner";
        
        await using var command = new SqlCommand(_rescheduleFunctionsSql, conn);
        command.Parameters.AddWithValue("@Owner", replicaId.AsGuid);
        
        await command.ExecuteNonQueryAsync();
    }

    private string? _interruptSql;
    private string? _interruptIfExecutingSql;
    public async Task<bool> Interrupt(StoredId storedId, bool onlyIfExecuting)
    {
        await using var conn = await _connFunc();
        _interruptSql ??= @$"
                UPDATE {_tableName}
                SET 
                    Interrupted = 1,
                    Status = 
                        CASE 
                            WHEN Status = {(int) Status.Suspended} THEN {(int) Status.Postponed}
                            ELSE Status
                        END,
                    Expires = 
                        CASE
                            WHEN Status = {(int) Status.Postponed} THEN 0
                            WHEN Status = {(int) Status.Suspended} THEN 0
                            ELSE Expires
                        END
                WHERE FlowType = @FlowType AND FlowInstance = @FlowInstance";
        _interruptIfExecutingSql ??= _interruptSql + $" AND Status = {(int) Status.Executing}";

        var sql = onlyIfExecuting
            ? _interruptIfExecutingSql
            : _interruptSql;
        
        await using var command = new SqlCommand(sql, conn);
        command.Parameters.AddWithValue("@FlowType", storedId.Type.Value);
        command.Parameters.AddWithValue("@FlowInstance", storedId.Instance.Value);

        var affectedRows = await command.ExecuteNonQueryAsync();
        return affectedRows == 1;
    }
    
    public async Task Interrupt(IReadOnlyList<StoredId> storedIds)
    {
        if (storedIds.Count == 0)
            return;
        
        await using var conn = await _connFunc();
        await using var cmd = _sqlGenerator.Interrupt(storedIds).ToSqlCommand(conn);
        await cmd.ExecuteNonQueryAsync();
    }

    private string? _setParametersSql;
    public async Task<bool> SetParameters(
        StoredId storedId,
        byte[]? param, byte[]? result,
        int expectedEpoch)
    {
        await using var conn = await _connFunc();
        
        _setParametersSql ??= @$"
            UPDATE {_tableName}
            SET ParamJson = @ParamJson,  
                ResultJson = @ResultJson,
                Epoch = Epoch + 1
            WHERE FlowType = @FlowType AND FlowInstance = @FlowInstance AND Epoch = @ExpectedEpoch";

        await using var command = new SqlCommand(_setParametersSql, conn);
        command.Parameters.AddWithValue("@ParamJson", param == null ? SqlBinary.Null : param);
        command.Parameters.AddWithValue("@ResultJson", result == null ? SqlBinary.Null : result);
        command.Parameters.AddWithValue("@FlowInstance", storedId.Instance.Value);
        command.Parameters.AddWithValue("@FlowType", storedId.Type.Value);
        command.Parameters.AddWithValue("@ExpectedEpoch", expectedEpoch);

        var affectedRows = await command.ExecuteNonQueryAsync();
        return affectedRows > 0;
    }
    
    private string? _interruptedSql;
    public async Task<bool?> Interrupted(StoredId storedId)
    {
        await using var conn = await _connFunc();
        _interruptedSql ??= @$"
                SELECT Interrupted 
                FROM {_tableName}            
                WHERE FlowType = @FlowType AND FlowInstance = @FlowInstance;";

        await using var command = new SqlCommand(_interruptedSql, conn);
        command.Parameters.AddWithValue("@FlowType", storedId.Type.Value);
        command.Parameters.AddWithValue("@FlowInstance", storedId.Instance.Value);

        var interrupted = await command.ExecuteScalarAsync();
        return (bool?) interrupted;
    }

    private string? _getFunctionStatusSql;
    public async Task<StatusAndEpoch?> GetFunctionStatus(StoredId storedId)
    {
        await using var conn = await _connFunc();
        _getFunctionStatusSql ??= @$"
            SELECT Status, Epoch
            FROM {_tableName}
            WHERE FlowType = @FlowType AND FlowInstance = @FlowInstance";
        
        await using var command = new SqlCommand(_getFunctionStatusSql, conn);
        command.Parameters.AddWithValue("@FlowType", storedId.Type.Value);
        command.Parameters.AddWithValue("@FlowInstance", storedId.Instance.Value);
        
        await using var reader = await command.ExecuteReaderAsync();
        while (reader.HasRows)
            while (reader.Read())
            {
                var status = (Status) reader.GetInt32(0);
                var epoch = reader.GetInt32(1);

                return new StatusAndEpoch(status, epoch);
            }

        return null;
    }

    public async Task<IReadOnlyList<StatusAndEpochWithId>> GetFunctionsStatus(IEnumerable<StoredId> storedIds)
    {
        var predicates = storedIds
            .Select(s => new { Type = s.Type.Value, Instance = s.Instance.Value })
            .GroupBy(id => id.Type, id => id.Instance)
            .Select(g => $"(FlowType = {g.Key} AND FlowInstance IN ({string.Join(",", g.Select(instance => $"'{instance}'"))}))")
            .StringJoin(" OR " + Environment.NewLine);

        var sql = @$"
            SELECT FlowType, FlowInstance, Status, Epoch, Expires
            FROM {_tableName}
            WHERE {predicates}";
        
        await using var conn = await _connFunc();
        
        await using var command = new SqlCommand(sql, conn);
        await using var reader = await command.ExecuteReaderAsync();
        var toReturn = new List<StatusAndEpochWithId>();
        
        while (reader.Read())
        {
            var type = reader.GetInt32(0).ToStoredType();
            var instance = reader.GetGuid(1).ToStoredInstance();
            var status = (Status) reader.GetInt32(2);
            var epoch = reader.GetInt32(3);
            var expires = reader.GetInt64(4);

            var storedId = new StoredId(type, instance);
            toReturn.Add(new StatusAndEpochWithId(storedId, status, epoch, expires));
        }

        return toReturn;
    }

    private string? _getFunctionSql;
    public async Task<StoredFlow?> GetFunction(StoredId storedId)
    {
        await using var conn = await _connFunc();
        _getFunctionSql ??= @$"
            SELECT  ParamJson, 
                    Status,
                    ResultJson, 
                    ExceptionJson,
                    Expires,
                    Epoch, 
                    Interrupted,
                    Timestamp,
                    HumanInstanceId,
                    Parent,
                    Owner
            FROM {_tableName}
            WHERE FlowType = @FlowType
            AND flowInstance = @FlowInstance";
        
        await using var command = new SqlCommand(_getFunctionSql, conn);
        command.Parameters.AddWithValue("@FlowType", storedId.Type.Value);
        command.Parameters.AddWithValue("@FlowInstance", storedId.Instance.Value);
        
        await using var reader = await command.ExecuteReaderAsync();
        return ReadToStoredFlow(storedId, reader);
    }

    private string? _getInstancesWithStatusSql;
    public async Task<IReadOnlyList<StoredInstance>> GetInstances(StoredType storedType, Status status)
    {
        await using var conn = await _connFunc();
        _getInstancesWithStatusSql ??= @$"
            SELECT FlowInstance
            FROM {_tableName} 
            WHERE FlowType = @FlowType AND Status = @Status";

        await using var command = new SqlCommand(_getInstancesWithStatusSql, conn);
        command.Parameters.AddWithValue("@FlowType", storedType.Value);
        command.Parameters.AddWithValue("@Status", (int) status);

        await using var reader = await command.ExecuteReaderAsync();
        var instances = new List<StoredInstance>(); 
        while (reader.Read())
        {
            var flowInstance = reader.GetGuid(0).ToStoredInstance();
            instances.Add(flowInstance);    
        }

        return instances;
    }

    private string? _getInstancesSql;
    public async Task<IReadOnlyList<StoredInstance>> GetInstances(StoredType storedType)
    {
        await using var conn = await _connFunc();
        _getInstancesSql ??= @$"
            SELECT FlowInstance
            FROM {_tableName} 
            WHERE FlowType = @FlowType";

        await using var command = new SqlCommand(_getInstancesSql, conn);
        command.Parameters.AddWithValue("@FlowType", storedType.Value);

        await using var reader = await command.ExecuteReaderAsync();
        var instances = new List<StoredInstance>();
        while (reader.Read())
        {
            var flowInstance = reader.GetGuid(0);
            instances.Add(flowInstance.ToStoredInstance());
        }

        return instances;
    }

    private StoredFlow? ReadToStoredFlow(StoredId storedId, SqlDataReader reader)
    {
        while (reader.HasRows)
        {
            while (reader.Read())
            {
                var parameter = reader.IsDBNull(0) ? null : (byte[]) reader.GetValue(0);
                var status = (Status) reader.GetInt32(1);
                var result = reader.IsDBNull(2) ? null : (byte[]) reader.GetValue(2);
                var exceptionJson = reader.IsDBNull(3) ? null : reader.GetString(3);
                var storedException = exceptionJson == null
                    ? null
                    : JsonSerializer.Deserialize<StoredException>(exceptionJson);
                var expires = reader.GetInt64(4);
                var epoch = reader.GetInt32(5);
                var interrupted = reader.GetBoolean(6);
                var timestamp = reader.GetInt64(7);
                var humanInstanceId = reader.GetString(8);
                var parentId = reader.IsDBNull(9) ? null : StoredId.Deserialize(reader.GetString(9));
                var ownerId = reader.IsDBNull(10) ? null : reader.GetGuid(10).ToReplicaId();

                return new StoredFlow(
                    storedId,
                    humanInstanceId,
                    parameter,
                    status,
                    result,
                    storedException,
                    epoch,
                    expires,
                    timestamp,
                    interrupted,
                    parentId,
                    ownerId
                );
            }
        }

        return default;
    }
    
    public async Task<bool> DeleteFunction(StoredId storedId)
    {
        await _messageStore.Truncate(storedId);
        await _effectsStore.Remove(storedId);
        await _timeoutStore.Remove(storedId);
        await _correlationStore.RemoveCorrelations(storedId);

        return await DeleteStoredFunction(storedId);
    }

    private string? _deleteFunctionSql;
    private async Task<bool> DeleteStoredFunction(StoredId storedId)
    {
        await using var conn = await _connFunc();
        _deleteFunctionSql ??= @$"
            DELETE FROM {_tableName}
            WHERE FlowType = @FlowType
            AND FlowInstance = @FlowInstance ";
        
        await using var command = new SqlCommand(_deleteFunctionSql, conn);
        command.Parameters.AddWithValue("@FlowInstance", storedId.Instance.Value);
        command.Parameters.AddWithValue("@FlowType", storedId.Type.Value);
        
        return await command.ExecuteNonQueryAsync() == 1;
    }
}