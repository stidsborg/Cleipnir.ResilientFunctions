using System;
using System.Collections.Generic;
using System.Data.SqlTypes;
using System.Linq;
using System.Runtime.Serialization;
using System.Text.Json;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Messaging;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Storage.Session;
using Cleipnir.ResilientFunctions.Storage.Utils;
using Microsoft.Data.SqlClient;

namespace Cleipnir.ResilientFunctions.SqlServer;

public class SqlServerFunctionStore : IFunctionStore
{
    private readonly Func<Task<SqlConnection>> _connFunc;
    private readonly string _tableName;
    private readonly string _connectionString;
    
    private readonly SqlServerEffectsStore _effectsStore;
    private readonly SqlServerMessageStore _messageStore;
    private readonly SqlServerCorrelationsStore _correlationStore;
    private readonly SqlServerTypeStore _typeStore;
    
    public IEffectsStore EffectsStore => _effectsStore;
    public ICorrelationStore CorrelationStore => _correlationStore;
    public ITypeStore TypeStore => _typeStore;
    public IMessageStore MessageStore => _messageStore;
    public Utilities Utilities { get; }
    private readonly SqlServerSemaphoreStore _semaphoreStore;
    public ISemaphoreStore SemaphoreStore => _semaphoreStore;
    private readonly SqlServerReplicaStore _replicaStore;
    public IReplicaStore ReplicaStore => _replicaStore;

    private readonly SqlServerUnderlyingRegister _underlyingRegister;
    
    private readonly SqlGenerator _sqlGenerator;

    public SqlServerFunctionStore(string connectionString, string tablePrefix = "")
    {
        _tableName = tablePrefix == "" ? "RFunctions" : tablePrefix;
        _connectionString = connectionString;
        _sqlGenerator = new SqlGenerator(_tableName);
        
        _connFunc = CreateConnection(connectionString);
        _messageStore = new SqlServerMessageStore(connectionString, _sqlGenerator, _tableName);
        _underlyingRegister = new SqlServerUnderlyingRegister(connectionString, _tableName);
        _effectsStore = new SqlServerEffectsStore(connectionString, _tableName);
        _correlationStore = new SqlServerCorrelationsStore(connectionString, _tableName);
        _semaphoreStore = new SqlServerSemaphoreStore(connectionString, _tableName);
        _typeStore = new SqlServerTypeStore(connectionString, _tableName);
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
        if (await DoTablesAlreadyExist())
            return;
        
        await _underlyingRegister.Initialize();
        await _messageStore.Initialize();
        await _effectsStore.Initialize();
        await _correlationStore.Initialize();
        await _typeStore.Initialize();
        await _semaphoreStore.Initialize();
        await _replicaStore.Initialize();
        await using var conn = await _connFunc();
        _initializeSql ??= @$"
            CREATE TABLE {_tableName} (
                Id UNIQUEIDENTIFIER PRIMARY KEY,
                Status INT NOT NULL,
                Expires BIGINT NOT NULL,
                Interrupted BIT NOT NULL DEFAULT 0,
                ParamJson VARBINARY(MAX) NULL,
                ResultJson VARBINARY(MAX) NULL,
                ExceptionJson NVARCHAR(MAX) NULL,
                HumanInstanceId NVARCHAR(MAX) NOT NULL,
                Timestamp BIGINT NOT NULL,
                Parent UNIQUEIDENTIFIER NULL,
                Owner UNIQUEIDENTIFIER NULL,
                Effects VARBINARY(MAX) NULL
            );
            CREATE INDEX {_tableName}_idx_Executing
                ON {_tableName} (Expires, Id)
                INCLUDE (Owner)
                WHERE Status = {(int)Status.Executing};    
            CREATE INDEX {_tableName}_idx_Owners
                ON {_tableName} (Owner, Id)                
                WHERE Status = {(int)Status.Executing};  
            CREATE INDEX {_tableName}_idx_Postponed
                ON {_tableName} (Expires, Id)
                INCLUDE (Owner)
                WHERE Status = {(int)Status.Postponed};
            CREATE INDEX {_tableName}_idx_Succeeded
                ON {_tableName} (Id)
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
    
    public async Task<IStorageSession?> CreateFunction(
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
            var session = new SnapshotStorageSession(owner ?? ReplicaId.Empty) { RowExists = true };

            // Serialize effects if present
            byte[]? effectsBytes = null;
            if (effects?.Any() ?? false)
            {
                foreach (var effect in effects)
                    session.Effects[effect.EffectId] = effect;
                effectsBytes = session.Serialize();
            }

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
                    paramPrefix: null,
                    effects: effectsBytes
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

            await using var command = storeCommand.ToSqlCommand(conn);
            if (messages?.Any() != true)
            {
                await command.ExecuteNonQueryAsync();
                return owner == null ? null : session;
            }

            await using var transaction = conn.BeginTransaction();
            command.Transaction = transaction;
            await command.ExecuteNonQueryAsync();
            await transaction.CommitAsync();
            return owner == null ? null : session;
        }
        catch (SqlException sqlException) when (sqlException.Number == SqlError.UNIQUENESS_VIOLATION)
        {
            return null;
        }
    }

    private string? _bulkScheduleFunctionsSql;
    public async Task BulkScheduleFunctions(IEnumerable<IdWithParam> functionsWithParam, StoredId? parent)
    {
        _bulkScheduleFunctionsSql ??= @$"
            MERGE INTO {_tableName}
            USING (VALUES @VALUES) 
            AS source (
                Id, 
                ParamJson, 
                Status,
                Owner,
                Expires,
                Timestamp,
                HumanInstanceId,
                Parent
            )
            ON {_tableName}.Id = source.Id         
            WHEN NOT MATCHED THEN
              INSERT (Id, ParamJson, Status, Owner, Expires, Timestamp, HumanInstanceId, Parent)
              VALUES (source.Id, source.ParamJson, source.Status, source.Owner, source.Expires, source.Timestamp, source.HumanInstanceId, source.Parent);";

        var parentStr = parent == null ? "NULL" : $"'{parent.AsGuid}'";
        var valueSql = $"(@Id, @ParamJson, {(int)Status.Postponed}, NULL, 0, 0, @HumanInstanceId, {parentStr})";
        var chunk = functionsWithParam
            .Select(
                (fp, i) =>
                {
                    var sql = valueSql
                        .Replace("@Id", $"@Id{i}")
                        .Replace("@ParamJson", $"@ParamJson{i}")
                        .Replace("@HumanInstanceId", $"@HumanInstanceId{i}");

                    return new { Id = i, Sql = sql, StoredId = fp.StoredId, Param = fp.Param, HumanInstanceId = fp.HumanInstanceId };
                }).Chunk(100);

        await using var conn = await _connFunc();
        foreach (var idAndSqls in chunk)
        {
            var valuesSql = string.Join($",{Environment.NewLine}", idAndSqls.Select(a => a.Sql));
            var sql = _bulkScheduleFunctionsSql.Replace("@VALUES", valuesSql);
            
            await using var command = new SqlCommand(sql, conn);
            foreach (var idAndSql in idAndSqls)
            {
                command.Parameters.AddWithValue($"@Id{idAndSql.Id}", idAndSql.StoredId.AsGuid);
                command.Parameters.AddWithValue($"@ParamJson{idAndSql.Id}", idAndSql.Param ?? SqlBinary.Null);
                command.Parameters.AddWithValue($"@HumanInstanceId{idAndSql.Id}", idAndSql.HumanInstanceId);
            }
            
            await command.ExecuteNonQueryAsync();
        }
    }
    
    public async Task<StoredFlowWithEffectsAndMessages?> RestartExecution(StoredId storedId, ReplicaId replicaId)
    {
        var restartCommand = _sqlGenerator.RestartExecution(storedId, replicaId);
        var messagesCommand = _sqlGenerator.GetMessages(storedId, skip: 0, paramPrefix: "Message");

        await using var conn = await _connFunc();
        await using var command = StoreCommand
            .Merge(restartCommand, messagesCommand)
            .ToSqlCommand(conn);

        await using var reader = await command.ExecuteReaderAsync();
        var (sf, effectsBytes) = _sqlGenerator.ReadToStoredFlowWithEffects(storedId, reader);
        if (sf?.OwnerId != replicaId)
            return null;

        var session = new SnapshotStorageSession(replicaId);
        var effects = new List<StoredEffect>();
        if (effectsBytes != null)
        {
            var effectsBytesArray = BinaryPacker.Split(effectsBytes);
            foreach (var effectBytes in effectsBytesArray)
            {
                if (effectBytes == null)
                    throw new SerializationException("Unable to deserialize effect");

                var storedEffect = StoredEffect.Deserialize(effectBytes);
                effects.Add(storedEffect);
                session.Effects[storedEffect.EffectId] = storedEffect;
            }

            session.RowExists = true;
            session.Version = 0;
        }

        await reader.NextResultAsync();
        var messages = await _sqlGenerator.ReadMessages(reader);
        var storedMessages = messages.Select(m => SqlServerMessageStore.ConvertToStoredMessage(m.content) with { Position = m.position }).ToList();

        return new StoredFlowWithEffectsAndMessages(sf, effects, storedMessages, session);
    }

    public async Task<Dictionary<StoredId, StoredFlowWithEffectsAndMessages>> RestartExecutions(
        IReadOnlyList<StoredId> storedIds,
        ReplicaId owner)
    {
        if (storedIds.Count == 0)
            return new Dictionary<StoredId, StoredFlowWithEffectsAndMessages>();

        // Execute 2 queries in parallel (restart includes effects inline)
        var restartTask = RestartFlowsAsync(storedIds, owner);
        var messagesTask = FetchMessagesAsync(storedIds);

        await Task.WhenAll(restartTask, messagesTask);

        var restartedFlows = await restartTask;
        var messagesMap = await messagesTask;

        // Build result dictionary - only for successfully restarted flows
        var result = new Dictionary<StoredId, StoredFlowWithEffectsAndMessages>();
        foreach (var (flow, effectsBytes, session) in restartedFlows)
        {
            var effects = new List<StoredEffect>();
            if (effectsBytes != null)
            {
                var effectsBytesArray = BinaryPacker.Split(effectsBytes);
                foreach (var effectBytes in effectsBytesArray)
                {
                    if (effectBytes != null)
                    {
                        var storedEffect = StoredEffect.Deserialize(effectBytes);
                        effects.Add(storedEffect);
                        session.Effects[storedEffect.EffectId] = storedEffect;
                    }
                }
                session.RowExists = true;
                session.Version = 0;
            }

            var messages = messagesMap.TryGetValue(flow.StoredId, out var msgs)
                ? msgs
                : new List<StoredMessage>();

            result[flow.StoredId] = new StoredFlowWithEffectsAndMessages(
                flow, effects, messages, session
            );
        }

        return result;
    }

    private async Task<List<(StoredFlow flow, byte[]? effectsBytes, SnapshotStorageSession session)>> RestartFlowsAsync(
        IReadOnlyList<StoredId> storedIds,
        ReplicaId owner)
    {
        await using var conn = await _connFunc();
        var storeCommand = _sqlGenerator.RestartExecutions(storedIds, owner);

        await using var command = storeCommand.ToSqlCommand(conn);
        await using var reader = await command.ExecuteReaderAsync();

        var flows = new List<(StoredFlow flow, byte[]? effectsBytes, SnapshotStorageSession session)>();
        while (await reader.ReadAsync())
        {
            var storedId = reader.GetGuid(0).ToStoredId();
            var (flow, effectsBytes) = _sqlGenerator.ReadToStoredFlowWithEffects(storedId, reader);
            if (flow != null && flow.OwnerId == owner)
            {
                var session = new SnapshotStorageSession(owner);
                flows.Add((flow, effectsBytes, session));
            }
        }
        return flows;
    }

    private async Task<Dictionary<StoredId, List<StoredMessage>>> FetchMessagesAsync(
        IReadOnlyList<StoredId> storedIds)
    {
        await using var conn = await _connFunc();
        var storeCommand = _sqlGenerator.GetMessages(storedIds);

        await using var command = storeCommand.ToSqlCommand(conn);
        await using var reader = await command.ExecuteReaderAsync();

        var messagesDict = await _sqlGenerator.ReadStoredIdsMessages(reader);
        return messagesDict.ToDictionary(
            kv => kv.Key,
            kv => kv.Value.Select(m => SqlServerMessageStore.ConvertToStoredMessage(m.content) with { Position = m.position }).ToList()
        );
    }

    private string? _getExpiredFunctionsSql;
    public async Task<IReadOnlyList<StoredId>> GetExpiredFunctions(long expiresBefore)
    {
        await using var conn = await _connFunc();
        _getExpiredFunctionsSql ??= @$"
            SELECT Id
            FROM {_tableName} WITH (NOLOCK) 
            WHERE Expires <= @Expires AND Status = { (int)Status.Postponed}";

        await using var command = new SqlCommand(_getExpiredFunctionsSql, conn);
        command.Parameters.AddWithValue("@Expires", expiresBefore);

        await using var reader = await command.ExecuteReaderAsync();
        var rows = new List<StoredId>(); 
        while (reader.HasRows)
        {
            while (reader.Read())
            {
                var storedId = new StoredId(reader.GetGuid(0));
                rows.Add(storedId);
            }

            reader.NextResult();
        }

        return rows;
    }

    private string? _getSucceededFunctionsSql;
    public async Task<IReadOnlyList<StoredId>> GetSucceededFunctions(long completedBefore)
    {
        await using var conn = await _connFunc();
        _getSucceededFunctionsSql ??= @$"
            SELECT Id
            FROM {_tableName} 
            WHERE Status = {(int) Status.Succeeded} AND Timestamp <= @CompletedBefore";

        await using var command = new SqlCommand(_getSucceededFunctionsSql, conn);
        command.Parameters.AddWithValue("@CompletedBefore", completedBefore);

        await using var reader = await command.ExecuteReaderAsync();
        var ids = new List<StoredId>(); 
        while (reader.HasRows)
        {
            while (reader.Read())
            {
                var storedInstance = reader.GetGuid(0).ToStoredId();
                ids.Add(storedInstance);    
            }

            reader.NextResult();
        }

        return ids;
    }

    public async Task<IReadOnlyList<StoredId>> GetInterruptedFunctions(IEnumerable<StoredId> ids)
    {
        var inSql = ids.Select(id => $"'{id.AsGuid}'").StringJoin(", ");
        if (string.IsNullOrEmpty(inSql))
            return [];

        var sql = @$"
            SELECT Id
            FROM {_tableName} WITH (NOLOCK)
            WHERE Interrupted = 1 AND Id IN ({inSql})";

        await using var conn = await _connFunc();
        await using var command = new SqlCommand(sql, conn);

        await using var reader = await command.ExecuteReaderAsync();
        var interruptedIds = new List<StoredId>();
        while (reader.HasRows)
        {
            while (reader.Read())
            {
                var storedId = reader.GetGuid(0).ToStoredId();
                interruptedIds.Add(storedId);
            }

            reader.NextResult();
        }

        return interruptedIds;
    }

    private string? _setFunctionStateSql;
    public async Task<bool> SetFunctionState(
        StoredId storedId, Status status, 
        byte[]? param, byte[]? result, 
        StoredException? storedException, 
        long expires,
        ReplicaId? expectedReplica)
    {
        await using var conn = await _connFunc();

        _setFunctionStateSql ??= @$"
            UPDATE {_tableName}
            SET
                Status = @Status,
                ParamJson = @ParamJson,             
                ResultJson = @ResultJson,
                ExceptionJson = @ExceptionJson,
                Expires = @Expires
            WHERE Id = @Id";
        
        var sql = expectedReplica == null
            ? _setFunctionStateSql + " AND Owner IS NULL"
            : _setFunctionStateSql + $" AND Owner = {expectedReplica}";
        
        await using var command = new SqlCommand(sql, conn);
        command.Parameters.AddWithValue("@Status", (int) status);
        command.Parameters.AddWithValue("@ParamJson", param == null ? SqlBinary.Null : param);
        command.Parameters.AddWithValue("@ResultJson", result == null ? SqlBinary.Null : result);
        var exceptionJson = storedException == null ? null : JsonSerializer.Serialize(storedException);
        command.Parameters.AddWithValue("@ExceptionJson", exceptionJson ?? (object) DBNull.Value);
        command.Parameters.AddWithValue("@Expires", expires);
        command.Parameters.AddWithValue("@Id", storedId.AsGuid);

        var affectedRows = await command.ExecuteNonQueryAsync();
        return affectedRows > 0;
    }

    public async Task<bool> SucceedFunction(
        StoredId storedId,
        byte[]? result,
        long timestamp,
        ReplicaId expectedReplica,
        IReadOnlyList<StoredEffect>? effects,
        IReadOnlyList<StoredMessage>? messages,
        IStorageSession? storageSession)
    {
        byte[]? effectsBytes = null;
        if (storageSession is SnapshotStorageSession session && session.Effects.Count > 0)
            effectsBytes = session.Serialize();

        await using var conn = await _connFunc();
        await using var command = _sqlGenerator
            .SucceedFunction(
                storedId,
                result,
                timestamp,
                expectedReplica,
                paramPrefix: "",
                effects: effectsBytes
            ).ToSqlCommand(conn);

        var affectedRows = await command.ExecuteNonQueryAsync();
        return affectedRows > 0;
    }
    
    public async Task<bool> PostponeFunction(
        StoredId storedId,
        long postponeUntil,
        long timestamp,
        ReplicaId expectedReplica,
        IReadOnlyList<StoredEffect>? effects,
        IReadOnlyList<StoredMessage>? messages,
        IStorageSession? storageSession)
    {
        byte[]? effectsBytes = null;
        if (storageSession is SnapshotStorageSession session && session.Effects.Count > 0)
            effectsBytes = session.Serialize();

        await using var conn = await _connFunc();
        await using var command = _sqlGenerator.PostponeFunction(
            storedId,
            postponeUntil,
            timestamp,
            expectedReplica,
            paramPrefix: "",
            effects: effectsBytes
        ).ToSqlCommand(conn);

        var affectedRows = await command.ExecuteNonQueryAsync();
        return affectedRows > 0;
    }
    
    public async Task<bool> FailFunction(
        StoredId storedId,
        StoredException storedException,
        long timestamp,
        ReplicaId expectedReplica,
        IReadOnlyList<StoredEffect>? effects,
        IReadOnlyList<StoredMessage>? messages,
        IStorageSession? storageSession)
    {
        byte[]? effectsBytes = null;
        if (storageSession is SnapshotStorageSession session && session.Effects.Count > 0)
            effectsBytes = session.Serialize();

        await using var conn = await _connFunc();
        await using var command = _sqlGenerator
            .FailFunction(
                storedId,
                storedException,
                timestamp,
                expectedReplica,
                paramPrefix: "",
                effects: effectsBytes
            ).ToSqlCommand(conn);


        var affectedRows = await command.ExecuteNonQueryAsync();
        return affectedRows > 0;
    }
    
    public async Task<bool> SuspendFunction(
        StoredId storedId,
        long timestamp,
        ReplicaId expectedReplica,
        IReadOnlyList<StoredEffect>? effects,
        IReadOnlyList<StoredMessage>? messages,
        IStorageSession? storageSession)
    {
        byte[]? effectsBytes = null;
        if (storageSession is SnapshotStorageSession session && session.Effects.Count > 0)
            effectsBytes = session.Serialize();

        await using var conn = await _connFunc();
        await using var command = _sqlGenerator
            .SuspendFunction(
                storedId,
                timestamp,
                expectedReplica,
                paramPrefix: "",
                effects: effectsBytes
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
                    Expires = 0
                WHERE Owner = @Owner";
        
        await using var command = new SqlCommand(_rescheduleFunctionsSql, conn);
        command.Parameters.AddWithValue("@Owner", replicaId.AsGuid);
        
        await command.ExecuteNonQueryAsync();
    }

    private string? _interruptSql;
    public async Task<bool> Interrupt(StoredId storedId)
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
                WHERE Id = @Id";
        
        await using var command = new SqlCommand(_interruptSql, conn);
        command.Parameters.AddWithValue("@Id", storedId.AsGuid);

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
        ReplicaId? expectedReplica)
    {
        await using var conn = await _connFunc();
        
        _setParametersSql ??= @$"
            UPDATE {_tableName}
            SET ParamJson = @ParamJson,  
                ResultJson = @ResultJson
            WHERE Id = @Id";
        
        var sql = expectedReplica == null
            ? _setParametersSql + " AND Owner IS NULL;"
            : _setParametersSql + $" AND Owner = '{expectedReplica.AsGuid}'";

        await using var command = new SqlCommand(sql, conn);
        command.Parameters.AddWithValue("@ParamJson", param == null ? SqlBinary.Null : param);
        command.Parameters.AddWithValue("@ResultJson", result == null ? SqlBinary.Null : result);
        command.Parameters.AddWithValue("@Id", storedId.AsGuid);

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
                WHERE Id = @Id;";

        await using var command = new SqlCommand(_interruptedSql, conn);
        command.Parameters.AddWithValue("@Id", storedId.AsGuid);

        var interrupted = await command.ExecuteScalarAsync();
        return (bool?) interrupted;
    }

    private string? _getFunctionStatusSql;
    public async Task<Status?> GetFunctionStatus(StoredId storedId)
    {
        await using var conn = await _connFunc();
        _getFunctionStatusSql ??= @$"
            SELECT Status
            FROM {_tableName}
            WHERE Id = @Id";
        
        await using var command = new SqlCommand(_getFunctionStatusSql, conn);
        command.Parameters.AddWithValue("@Id", storedId.AsGuid);
        
        await using var reader = await command.ExecuteReaderAsync();
        while (reader.HasRows)
            while (reader.Read())
            {
                var status = (Status) reader.GetInt32(0);
                return status;
            }

        return null;
    }

    public async Task<IReadOnlyList<StatusAndId>> GetFunctionsStatus(IEnumerable<StoredId> storedIds)
    {
        var sql = @$"
            SELECT Id, Status, Expires
            FROM {_tableName}
            WHERE Id IN ({storedIds.Select(id => $"'{id.AsGuid}'").StringJoin(", ")})";
        
        await using var conn = await _connFunc();
        
        await using var command = new SqlCommand(sql, conn);
        await using var reader = await command.ExecuteReaderAsync();
        var toReturn = new List<StatusAndId>();
        
        while (reader.Read())
        {
            var id = reader.GetGuid(0);
            var status = (Status) reader.GetInt32(1);
            var expires = reader.GetInt64(2);

            var storedId = new StoredId(id);
            toReturn.Add(new StatusAndId(storedId, status, expires));
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
                    Interrupted,
                    Timestamp,
                    HumanInstanceId,
                    Parent,
                    Owner
            FROM {_tableName}
            WHERE Id = @Id";
        
        await using var command = new SqlCommand(_getFunctionSql, conn);
        command.Parameters.AddWithValue("@Id", storedId.AsGuid);
        
        await using var reader = await command.ExecuteReaderAsync();
        return ReadToStoredFlow(storedId, reader);
    }

    private string? _getInstancesWithStatusSql;
    public async Task<IReadOnlyList<StoredId>> GetInstances(StoredType storedType, Status status)
    {
        await using var conn = await _connFunc();
        _getInstancesWithStatusSql ??= @$"
            SELECT FlowInstance
            FROM {_tableName} 
            WHERE FlowType = @FlowType AND Status = @Status";

        await using var command = new SqlCommand(_getInstancesWithStatusSql, conn);
        command.Parameters.AddWithValue("@FlowType", storedType.Value.ToInt());
        command.Parameters.AddWithValue("@Status", (int) status);

        await using var reader = await command.ExecuteReaderAsync();
        var ids = new List<StoredId>(); 
        while (reader.Read())
        {
            var id = reader.GetGuid(0).ToStoredId();
            ids.Add(id);    
        }

        return ids;
    }

    private string? _getInstancesSql;
    public async Task<IReadOnlyList<StoredId>> GetInstances(StoredType storedType)
    {
        await using var conn = await _connFunc();
        _getInstancesSql ??= @$"
            SELECT FlowInstance
            FROM {_tableName} 
            WHERE FlowType = @FlowType";

        await using var command = new SqlCommand(_getInstancesSql, conn);
        command.Parameters.AddWithValue("@FlowType", storedType.Value.ToInt());

        await using var reader = await command.ExecuteReaderAsync();
        var ids = new List<StoredId>();
        while (reader.Read())
        {
            var flowInstance = reader.GetGuid(0);
            ids.Add(flowInstance.ToStoredId());
        }

        return ids;
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
                var interrupted = reader.GetBoolean(5);
                var timestamp = reader.GetInt64(6);
                var humanInstanceId = reader.GetString(7);
                var parentId = reader.IsDBNull(8) ? null : reader.GetGuid(8).ToStoredId();
                var ownerId = reader.IsDBNull(9) ? null : reader.GetGuid(9).ToReplicaId();

                return new StoredFlow(
                    storedId,
                    humanInstanceId,
                    parameter,
                    status,
                    storedException,
                    expires,
                    timestamp,
                    interrupted,
                    parentId,
                    ownerId,
                    storedId.Type
                );
            }
        }

        return default;
    }
    
    public async Task<bool> DeleteFunction(StoredId storedId)
    {
        await _messageStore.Truncate(storedId);
        await _effectsStore.Remove(storedId);
        await _correlationStore.RemoveCorrelations(storedId);

        return await DeleteStoredFunction(storedId);
    }

    public IFunctionStore WithPrefix(string prefix)
        => new SqlServerFunctionStore(_connectionString, prefix);

    public async Task<IReadOnlyDictionary<StoredId, byte[]?>> GetResults(IEnumerable<StoredId> storedIds)
    {
        var inSql = storedIds.Select(id => $"'{id.AsGuid}'").StringJoin(", ");
        if (inSql == "")
            return new Dictionary<StoredId, byte[]?>();
        
        var sql = @$"
            SELECT Id, ResultJson
            FROM {_tableName}
            WHERE Id IN ({inSql})";

        await using var conn = await _connFunc();
        await using var command = new SqlCommand(sql, conn);

        await using var reader = await command.ExecuteReaderAsync();
        var results = new Dictionary<StoredId, byte[]?>();
        while (reader.Read())
        {
            var storedId = new StoredId(reader.GetGuid(0));
            var result = reader.IsDBNull(1) ? null : (byte[])reader.GetValue(1);
            results[storedId] = result;
        }

        return results;
    }

    private string? _deleteFunctionSql;
    private async Task<bool> DeleteStoredFunction(StoredId storedId)
    {
        await using var conn = await _connFunc();
        _deleteFunctionSql ??= @$"
            DELETE FROM {_tableName}
            WHERE Id = @Id;";
        
        await using var command = new SqlCommand(_deleteFunctionSql, conn);
        command.Parameters.AddWithValue("@Id", storedId.AsGuid);
        
        return await command.ExecuteNonQueryAsync() == 1;
    }
    
    private async Task<bool> DoTablesAlreadyExist()
    {
        await using var conn = await _connFunc();
        
        var sql = $"SELECT TOP(1) 1 FROM {_tableName};";

        await using var command = new SqlCommand(sql, conn);
        try
        {
            await command.ExecuteScalarAsync();
            return true;    
        } catch (SqlException e)
        {
            const int invalidObjectName = 208;
            if (e.Number == invalidObjectName)
                return false;

            throw;
        }
    }
}