using System.Text;
using System.Text.Json;
using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Messaging;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Storage.Session;
using MySqlConnector;
using static Cleipnir.ResilientFunctions.MariaDb.DatabaseHelper;

namespace Cleipnir.ResilientFunctions.MariaDb;

public class MariaDbFunctionStore : IFunctionStore
{
    private readonly string _connectionString;
    private readonly string _tablePrefix;

    private readonly MariaDbMessageStore _messageStore;
    public IMessageStore MessageStore => _messageStore;
    
    private readonly MariaDbEffectsStore _effectsStore;
    public IEffectsStore EffectsStore => _effectsStore;
    
    private readonly MariaDbTypeStore _typeStore;
    public ITypeStore TypeStore => _typeStore;
    
    private readonly MariaDbCorrelationStore _correlationStore;
    public ICorrelationStore CorrelationStore => _correlationStore;
    
    private readonly MariaDbSemaphoreStore _semaphoreStore;
    public ISemaphoreStore SemaphoreStore => _semaphoreStore;

    private readonly MariaDbReplicaStore _replicaStore;
    public IReplicaStore ReplicaStore => _replicaStore;

    public Utilities Utilities { get; }
    private readonly MariaDbUnderlyingRegister _mariaDbUnderlyingRegister;

    private readonly SqlGenerator _sqlGenerator;

    public MariaDbFunctionStore(string connectionString, string tablePrefix = "")
    {
        tablePrefix = tablePrefix == "" ? "rfunctions" : tablePrefix;
        
        _connectionString = connectionString;
        _tablePrefix = tablePrefix;
        _sqlGenerator = new SqlGenerator(tablePrefix);
        
        _messageStore = new MariaDbMessageStore(connectionString, _sqlGenerator, tablePrefix);
        _effectsStore = new MariaDbEffectsStore(connectionString, _sqlGenerator, tablePrefix);
        _correlationStore = new MariaDbCorrelationStore(connectionString, tablePrefix);
        _semaphoreStore = new MariaDbSemaphoreStore(connectionString, tablePrefix);
        _mariaDbUnderlyingRegister = new MariaDbUnderlyingRegister(connectionString, tablePrefix);
        _typeStore = new MariaDbTypeStore(connectionString, tablePrefix);
        _replicaStore = new MariaDbReplicaStore(connectionString, tablePrefix);
        
        Utilities = new Utilities(_mariaDbUnderlyingRegister);
    }

    private string? _initializeSql;
    public async Task Initialize()
    {
        if (await DoTablesAlreadyExist())
            return;
        
        await _mariaDbUnderlyingRegister.Initialize();
        await MessageStore.Initialize();
        await EffectsStore.Initialize();
        await CorrelationStore.Initialize();
        await _semaphoreStore.Initialize();
        await _typeStore.Initialize();
        await _replicaStore.Initialize();
        await using var conn = await CreateOpenConnection(_connectionString);
        _initializeSql ??= $@"
            CREATE TABLE IF NOT EXISTS {_tablePrefix} (
                id CHAR(32) PRIMARY KEY,
                status INT NOT NULL,
                expires BIGINT NOT NULL,
                interrupted BOOLEAN NOT NULL,                
                param_json LONGBLOB NULL,                                    
                result_json LONGBLOB NULL,
                exception_json TEXT NULL,                
                timestamp BIGINT NOT NULL,
                human_instance_id TEXT NOT NULL,
                parent TEXT NULL,
                owner CHAR(32) NULL,
                INDEX (expires, id, status)   
            );";

        await using var command = new MySqlCommand(_initializeSql, conn);
        await command.ExecuteNonQueryAsync();
    }

    private string? _truncateTablesSql;
    public async Task TruncateTables()
    {
        await _messageStore.TruncateTable();
        await _mariaDbUnderlyingRegister.TruncateTable();
        await _effectsStore.Truncate();
        await _correlationStore.Truncate();
        await _semaphoreStore.Truncate();
        await _typeStore.Truncate();
        await _replicaStore.Truncate();
        
        await using var conn = await CreateOpenConnection(_connectionString);
        _truncateTablesSql ??= $"TRUNCATE TABLE {_tablePrefix}";
        await using var command = new MySqlCommand(_truncateTablesSql, conn);
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
        IReadOnlyList<StoredMessage>? messages = null)
    {
        if (effects == null && messages == null)
        {
            await using var conn = await CreateOpenConnection(_connectionString);
            await using var command = _sqlGenerator
                .CreateFunction(storedId, humanInstanceId, param, leaseExpiration, postponeUntil, timestamp, parent, owner, ignoreDuplicate: true)
                .ToSqlCommand(conn);
            var affectedRows = await command.ExecuteNonQueryAsync();
            return affectedRows == 1 ? new PositionsStorageSession() : null;
        }
        else
        {
            var storeCommand = _sqlGenerator.CreateFunction(storedId, humanInstanceId, param, leaseExpiration, postponeUntil, timestamp, parent, owner, ignoreDuplicate: false);
            if (messages?.Any() ?? false)
            {
                var messagesCommand = _sqlGenerator.AppendMessages(
                    messages.Select((msg, position) => new StoredIdAndMessageWithPosition(storedId, msg, position)).ToList()
                );
                storeCommand = storeCommand.Merge(messagesCommand);
            }

            var session = new PositionsStorageSession();
            if (effects?.Any() ?? false)
            {
                var effectsCommand = _sqlGenerator.UpdateEffects(
                    storedId,
                    changes: effects.Select(e => new StoredEffectChange(storedId, e.EffectId, CrudOperation.Insert, e)).ToList(),
                    session
                );
                storeCommand = storeCommand.Merge(effectsCommand);
            }

            await using var conn = await CreateOpenConnection(_connectionString);
            await using var command = storeCommand.ToSqlCommand(conn);

            try
            {
                await command.ExecuteNonQueryAsync();
                return session;
            }
            catch (MySqlException ex) when (ex.Number == 1062)
            {
                return null;
            }
        }
    }

    public async Task BulkScheduleFunctions(IEnumerable<IdWithParam> functionsWithParam, StoredId? parent)
    {
        var insertSql = @$"
            INSERT IGNORE INTO {_tablePrefix}
              (id, param_json, status, expires, timestamp, human_instance_id, parent, owner)
            VALUES                      
                    ";
        
        var now = DateTime.UtcNow.Ticks;
        var parentStr = parent == null ? "NULL" : $"'{parent}'"; 
     
        var rows = new List<string>();
        foreach (var (storedId, humanInstanceId, param) in functionsWithParam)
        {
            var id = storedId.AsGuid;
            var row = $"('{id:N}', {(param == null ? "NULL" : $"x'{Convert.ToHexString(param)}'")}, {(int) Status.Postponed}, 0, {now}, '{humanInstanceId.EscapeString()}', {parentStr}, NULL)"; 
            rows.Add(row);
        }
        var rowsSql = string.Join(", " + Environment.NewLine, rows);
        var strBuilder = new StringBuilder(rowsSql.Length + 2);
        strBuilder.Append(insertSql);
        strBuilder.Append(rowsSql);
        strBuilder.Append(";");
        var sql = strBuilder.ToString();

        await using var conn = await CreateOpenConnection(_connectionString);
        await using var cmd = new MySqlCommand(sql, conn);
        cmd.ExecuteNonQuery();
    }
    
    public async Task<StoredFlowWithEffectsAndMessages?> RestartExecution(StoredId storedId, ReplicaId replicaId)
    {
        var restartCommand = _sqlGenerator.RestartExecution(storedId, replicaId);
        var effectsCommand = _sqlGenerator.GetEffects(storedId);
        var messagesCommand = _sqlGenerator.GetMessages(storedId, skip: 0);

        await using var conn = await CreateOpenConnection(_connectionString);
        await using var command = StoreCommand
            .Merge(restartCommand, effectsCommand, messagesCommand)
            .ToSqlCommand(conn);

        var reader = await command.ExecuteReaderAsync();
        if (reader.RecordsAffected != 1)
            return null;

        var sf = await _sqlGenerator.ReadToStoredFunction(storedId, reader);
        if (sf?.OwnerId != replicaId)
            return null;
        await reader.NextResultAsync();

        var effectsWithPositions = await _sqlGenerator.ReadEffectsWithPositions(reader);
        var effects = effectsWithPositions.Select(e => e.Effect).ToList();
        await reader.NextResultAsync();

        var messages = await _sqlGenerator.ReadMessages(reader);

        var session = new PositionsStorageSession();
        foreach (var (effect, position) in effectsWithPositions.OrderBy(e => e.Position))
        {
            session.MaxPosition = position;
            session.Positions[effect.EffectId.Serialize()] = position;
        }

        return new StoredFlowWithEffectsAndMessages(sf, effects, messages, session);
    }
    
    private string? _getExpiredFunctionsSql;
    public async Task<IReadOnlyList<StoredId>> GetExpiredFunctions(long expiredBefore)
    {
        await using var conn = await CreateOpenConnection(_connectionString);
        _getExpiredFunctionsSql ??= @$"
            SELECT id
            FROM {_tablePrefix}
            WHERE expires <= ? AND status = {(int) Status.Postponed}";
        await using var command = new MySqlCommand(_getExpiredFunctionsSql, conn)
        {
            Parameters =
            {
                new() {Value = expiredBefore}
            }
        };
        
        await using var reader = await command.ExecuteReaderAsync();
        var ids = new List<StoredId>();
        while (await reader.ReadAsync())
        {
            var guid = reader.GetString(0).ToGuid();
            var id = new StoredId(guid);
            ids.Add(id);
        }
        
        return ids;
    }

    private string? _getSucceededFunctionsSql;
    public async Task<IReadOnlyList<StoredId>> GetSucceededFunctions(long completedBefore)
    {
        await using var conn = await CreateOpenConnection(_connectionString);
        _getSucceededFunctionsSql ??= @$"
            SELECT id
            FROM {_tablePrefix}
            WHERE status = {(int) Status.Succeeded} AND timestamp <= ?";
        await using var command = new MySqlCommand(_getSucceededFunctionsSql, conn)
        {
            Parameters =
            {
                new() {Value = completedBefore}
            }
        };
        
        await using var reader = await command.ExecuteReaderAsync();
        var ids = new List<StoredId>();
        while (await reader.ReadAsync())
        {
            var instance = reader.GetString(0).ToGuid().ToStoredId();
            ids.Add(instance);
        }
        
        return ids;
    }

    public async Task<IReadOnlyList<StoredId>> GetInterruptedFunctions(IEnumerable<StoredId> ids)
    {
        var inSql = ids.Select(id => $"'{id.AsGuid:N}'").StringJoin(", ");
        if (string.IsNullOrEmpty(inSql))
            return [];
        
        var sql = @$"
            SELECT id
            FROM {_tablePrefix}
            WHERE interrupted = TRUE AND id IN ({inSql})";

        await using var conn = await CreateOpenConnection(_connectionString);
        await using var command = new MySqlCommand(sql, conn);

        await using var reader = await command.ExecuteReaderAsync();
        var interruptedIds = new List<StoredId>();
        while (await reader.ReadAsync())
        {
            var storedId = reader.GetString(0).ToGuid().ToStoredId();
            interruptedIds.Add(storedId);
        }

        return interruptedIds;
    }

    private string? _setFunctionStateSql;
    public async Task<bool> SetFunctionState(
        StoredId storedId, Status status, 
        byte[]? storedParameter, byte[]? storedResult, 
        StoredException? storedException, 
        long expires,
        ReplicaId? expectedReplica)
    {
        await using var conn = await CreateOpenConnection(_connectionString);

        _setFunctionStateSql ??= $@"
            UPDATE {_tablePrefix}
            SET status = ?, 
                param_json = ?,  
                result_json = ?,  
                exception_json = ?, expires = ?
            WHERE id = ?";
        
        var sql = expectedReplica == null
             ? _setFunctionStateSql + " AND owner IS NULL" 
             :  _setFunctionStateSql + $" AND owner = {expectedReplica}";
        await using var command = new MySqlCommand(sql, conn)
        {
            Parameters =
            {
                new() {Value = (int) status},
                new() {Value = storedParameter ?? (object) DBNull.Value},
                new() {Value = storedResult ?? (object) DBNull.Value},
                new() {Value = storedException != null ? JsonSerializer.Serialize(storedException) : DBNull.Value},
                new() {Value = expires},
                new() {Value = storedId.AsGuid.ToString("N")},
            }
        };

        var affectedRows = await command.ExecuteNonQueryAsync();
        return affectedRows == 1;
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
        await using var conn = await CreateOpenConnection(_connectionString);
        await using var command = _sqlGenerator
            .SucceedFunction(storedId, result, timestamp, expectedReplica.AsGuid)
            .ToSqlCommand(conn);

        var affectedRows = await command.ExecuteNonQueryAsync();
        return affectedRows == 1;
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
        await using var conn = await CreateOpenConnection(_connectionString);
        await using var command = _sqlGenerator
            .PostponeFunction(storedId, postponeUntil, timestamp, expectedReplica)
            .ToSqlCommand(conn);

        var affectedRows = await command.ExecuteNonQueryAsync();
        return affectedRows == 1;
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
        await using var conn = await CreateOpenConnection(_connectionString);
        await using var command = _sqlGenerator
            .FailFunction(storedId, storedException, timestamp, expectedReplica)
            .ToSqlCommand(conn);

        var affectedRows = await command.ExecuteNonQueryAsync();
        return affectedRows == 1;
    }
    
    public async Task<bool> SuspendFunction(
        StoredId storedId,
        long timestamp,
        ReplicaId expectedReplica,
        IReadOnlyList<StoredEffect>? effects,
        IReadOnlyList<StoredMessage>? messages,
        IStorageSession? storageSession)
    {
        await using var conn = await CreateOpenConnection(_connectionString);
        await using var command = _sqlGenerator
            .SuspendFunction(storedId, timestamp, expectedReplica)
            .ToSqlCommand(conn);

        var affectedRows = await command.ExecuteNonQueryAsync();
        return affectedRows == 1;
    }

    private string? _getReplicasSql;
    public async Task<IReadOnlyList<ReplicaId>> GetOwnerReplicas()
    {
        await using var conn = await CreateOpenConnection(_connectionString);
        _getReplicasSql ??= @$"
            SELECT DISTINCT(Owner)
            FROM {_tablePrefix}
            WHERE Status = {(int) Status.Executing} AND Owner IS NOT NULL";
        
        await using var command = new MySqlCommand(_getReplicasSql, conn);
        
        await using var reader = await command.ExecuteReaderAsync();
        var replicas = new List<ReplicaId>();
        while (await reader.ReadAsync())
            replicas.Add(reader.GetString(0).ToGuid().ToReplicaId());
        
        return replicas;
    }

    private string? _rescheduleFunctionsSql;
    public async Task RescheduleCrashedFunctions(ReplicaId replicaId)
    {
        await using var conn = await CreateOpenConnection(_connectionString);
        
        _rescheduleFunctionsSql ??= $@"
            UPDATE {_tablePrefix}
            SET 
                status = {(int) Status.Postponed},
                expires = 0,
                owner = NULL
            WHERE 
                owner = ?";
        
        await using var command = new MySqlCommand(_rescheduleFunctionsSql, conn)
        {
            Parameters =
            {
                new() { Value = replicaId.AsGuid.ToString("N") },
            }
        };
        
        await command.ExecuteNonQueryAsync();
    }

    private string? _interruptSql;
    public async Task<bool> Interrupt(StoredId storedId)
    {
        await using var conn = await CreateOpenConnection(_connectionString);
        
        _interruptSql ??= $@"
            UPDATE {_tablePrefix}
            SET 
                interrupted = TRUE,
                status = 
                    CASE 
                        WHEN status = {(int) Status.Suspended} THEN {(int) Status.Postponed}
                        ELSE status
                    END,
                expires = 
                    CASE
                        WHEN status = {(int) Status.Postponed} THEN 0
                        WHEN status = {(int) Status.Suspended} THEN 0
                        ELSE expires
                    END
            WHERE id = ?";

        await using var command = new MySqlCommand(_interruptSql, conn)
        {
            Parameters =
            {
                new() { Value = storedId.AsGuid.ToString("N") },
            }
        };
        
        var affectedRows = await command.ExecuteNonQueryAsync();
        return affectedRows == 1;
    }
    
    public async Task Interrupt(IReadOnlyList<StoredId> storedIds)
    {
        if (storedIds.Count == 0)
            return;
        
        await using var conn = await CreateOpenConnection(_connectionString);
        await using var cmd = _sqlGenerator.Interrupt(storedIds).ToSqlCommand(conn);
        await cmd.ExecuteNonQueryAsync();
    }

    private string? _setParametersSql;
    public async Task<bool> SetParameters(
        StoredId storedId,
        byte[]? storedParameter, byte[]? storedResult,
        ReplicaId? expectedReplica)
    {
        await using var conn = await CreateOpenConnection(_connectionString);

        _setParametersSql ??= $@"
            UPDATE {_tablePrefix}
            SET param_json = ?,  
                result_json = ?
            WHERE 
                id = ?";

        var sql = expectedReplica == null
            ? _setParametersSql + " AND owner IS NULL"
            : _setParametersSql + $" AND owner = '{expectedReplica.AsGuid:N}'";

        await using var command = new MySqlCommand(sql, conn)
        {
            Parameters =
            {
                new() { Value = storedParameter ?? (object) DBNull.Value },
                new() { Value = storedResult ?? (object) DBNull.Value },
                new() { Value = storedId.AsGuid.ToString("N") }
            }
        };
            
        var affectedRows = await command.ExecuteNonQueryAsync();
        return affectedRows == 1;
    }

    private string? _getInterruptCountSql;
    public async Task<bool?> Interrupted(StoredId storedId)
    {
        await using var conn = await CreateOpenConnection(_connectionString);
        
        _getInterruptCountSql ??= $@"
            SELECT interrupted 
            FROM {_tablePrefix}
            WHERE id = ?;";

        await using var command = new MySqlCommand(_getInterruptCountSql, conn)
        {
            Parameters =
            {
                new() { Value = storedId.AsGuid.ToString("N") },
            }
        };
        
        return (bool?) await command.ExecuteScalarAsync();
    }

    private string? _getFunctionStatusSql;
    public async Task<Status?> GetFunctionStatus(StoredId storedId)
    {
        await using var conn = await CreateOpenConnection(_connectionString);
        _getFunctionStatusSql ??= $@"
            SELECT status
            FROM {_tablePrefix}
            WHERE id = ?;";
        await using var command = new MySqlCommand(_getFunctionStatusSql, conn)
        {
            Parameters = { 
                new() {Value = storedId.AsGuid.ToString("N")}
            }
        };
        
        await using var reader = await command.ExecuteReaderAsync();
        
        while (await reader.ReadAsync())
        {
            return (Status) reader.GetInt32(0);
        }

        return null;
    }

    public async Task<IReadOnlyList<StatusAndId>> GetFunctionsStatus(IEnumerable<StoredId> storedIds)
    {
        var sql = @$"
            SELECT id, status, expires
            FROM {_tablePrefix}
            WHERE Id in ({storedIds.Select(id => $"'{id.AsGuid:N}'").StringJoin(", ")})";

        await using var conn = await CreateOpenConnection(_connectionString);
        await using var command = new MySqlCommand(sql, conn);
        
        var toReturn = new List<StatusAndId>();
        await using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            var guid = reader.GetGuid(0);
            var status = (Status) reader.GetInt32(1);
            var expires = reader.GetInt64(2);

            var storedId = new StoredId(guid);
            toReturn.Add(new StatusAndId(storedId, status, expires));
        }

        return toReturn;
    }

    private string? _getFunctionSql;
    public async Task<StoredFlow?> GetFunction(StoredId storedId)
    {
        await using var conn = await CreateOpenConnection(_connectionString);
        _getFunctionSql ??= $@"
            SELECT               
                param_json,             
                status,
                result_json,             
                exception_json,               
                expires,
                interrupted,
                timestamp,
                human_instance_id,
                parent,
                owner
            FROM {_tablePrefix}
            WHERE id = ?;";
        await using var command = new MySqlCommand(_getFunctionSql, conn)
        {
            Parameters = { 
                new() {Value = storedId.AsGuid.ToString("N")}
            }
        };
        
        await using var reader = await command.ExecuteReaderAsync();
        return await ReadToStoredFunction(storedId, reader);
    }

    private string? _getInstancesWithStatusSql;
    public async Task<IReadOnlyList<StoredId>> GetInstances(StoredType storedType, Status status)
    {
        await using var conn = await CreateOpenConnection(_connectionString);
        _getInstancesWithStatusSql ??= @$"
            SELECT instance
            FROM {_tablePrefix}
            WHERE type = ? AND status = ?";
        await using var command = new MySqlCommand(_getInstancesWithStatusSql, conn)
        {
            Parameters =
            {
                new() {Value = storedType.Value},
                new() {Value = (int) status}
            }
        };
        
        await using var reader = await command.ExecuteReaderAsync();
        var ids = new List<StoredId>();
        while (await reader.ReadAsync())
        {
            var flowInstance = reader.GetString(0).ToGuid().ToStoredId();
            ids.Add(flowInstance);
        }
        
        return ids;
    }

    private string? _getInstancesSql;
    public async Task<IReadOnlyList<StoredId>> GetInstances(StoredType storedType)
    {
        await using var conn = await CreateOpenConnection(_connectionString);
        _getInstancesSql ??= @$"
            SELECT instance
            FROM {_tablePrefix}
            WHERE type = ?";
        await using var command = new MySqlCommand(_getInstancesSql, conn)
        {
            Parameters =
            {
                new() {Value = storedType.Value}
            }
        };
        
        await using var reader = await command.ExecuteReaderAsync();
        var ids = new List<StoredId>();
        while (await reader.ReadAsync())
        {
            var id = reader.GetString(0).ToGuid().ToStoredId();
            ids.Add(id);
        }
        
        return ids;
    }
    
    private async Task<StoredFlow?> ReadToStoredFunction(StoredId storedId, MySqlDataReader reader)
    {
        const int paramIndex = 0;
        const int statusIndex = 1;
        const int resultIndex = 2;
        const int exceptionIndex = 3;
        const int expiresIndex = 4;
        const int interruptedIndex = 5;
        const int timestampIndex = 6;
        const int humanInstanceIdIndex = 7;
        const int parentIndex = 8;
        const int ownerIndex = 9;
        
        while (await reader.ReadAsync())
        {
            var hasParam = !await reader.IsDBNullAsync(paramIndex);
            var hasResult = !await reader.IsDBNullAsync(resultIndex);
            var hasError = !await reader.IsDBNullAsync(exceptionIndex);
            var hasParent = !await reader.IsDBNullAsync(parentIndex);
            var hasOwner = !await reader.IsDBNullAsync(ownerIndex);
            var storedException = hasError
                ? JsonSerializer.Deserialize<StoredException>(reader.GetString(exceptionIndex))
                : null;
            return new StoredFlow(
                storedId,
                InstanceId: reader.GetString(humanInstanceIdIndex),
                hasParam ? (byte[]) reader.GetValue(paramIndex) : null,
                Status: (Status) reader.GetInt32(statusIndex),
                Result: hasResult ? (byte[]) reader.GetValue(resultIndex) : null, 
                storedException, 
                Expires: reader.GetInt64(expiresIndex),
                Interrupted: reader.GetBoolean(interruptedIndex),
                Timestamp: reader.GetInt64(timestampIndex),
                ParentId: hasParent ? StoredId.Deserialize(reader.GetString(parentIndex)) : null,
                OwnerId: hasOwner ? reader.GetString(ownerIndex).ParseToReplicaId() : null,
                StoredType: storedId.Type
            );
        }

        return null;
    }
    
    public async Task<bool> DeleteFunction(StoredId storedId)
    {
        await _messageStore.Truncate(storedId);
        await _effectsStore.Remove(storedId);
        await _correlationStore.RemoveCorrelations(storedId);

        return await DeleteStoredFunction(storedId);
    }

    public IFunctionStore WithPrefix(string prefix)
        => new MariaDbFunctionStore(_connectionString, prefix);

    public async Task<IReadOnlyDictionary<StoredId, byte[]?>> GetResults(IEnumerable<StoredId> storedIds)
    {
        var inSql = storedIds.Select(id => $"'{id.AsGuid:N}'").StringJoin(", ");
        if (inSql == "")
            return new Dictionary<StoredId, byte[]?>();

        var sql = @$"
            SELECT id, result_json
            FROM {_tablePrefix}
            WHERE id IN ({inSql})";

        await using var conn = await CreateOpenConnection(_connectionString);
        await using var command = new MySqlCommand(sql, conn);

        await using var reader = await command.ExecuteReaderAsync();
        var results = new Dictionary<StoredId, byte[]?>();
        while (await reader.ReadAsync())
        {
            var guid = reader.GetString(0).ToGuid();
            var storedId = new StoredId(guid);
            var hasResult = !await reader.IsDBNullAsync(1);
            var result = hasResult ? (byte[])reader.GetValue(1) : null;
            results[storedId] = result;
        }

        return results;
    }

    private string? _deleteFunctionSql;
    private async Task<bool> DeleteStoredFunction(StoredId storedId)
    {
        await using var conn = await CreateOpenConnection(_connectionString);
        
        _deleteFunctionSql ??= $@"            
            DELETE FROM {_tablePrefix}
            WHERE id = ?";
        
        await using var command = new MySqlCommand(_deleteFunctionSql, conn)
        {
            Parameters =
            {
                new() {Value = storedId.AsGuid.ToString("N")}
            }
        };

        return await command.ExecuteNonQueryAsync() == 1;
    }
    
    private async Task<bool> DoTablesAlreadyExist()
    {
        await using var conn = await CreateOpenConnection(_connectionString);
        
        var sql = $"SELECT 1 FROM {_tablePrefix} LIMIT 1;";

        await using var command = new MySqlCommand(sql, conn);
        try
        {
            await command.ExecuteScalarAsync();
            return true;    
        } catch (MySqlException e)
        {
            if (e.ErrorCode == MySqlErrorCode.NoSuchTable)
                return false;

            throw;
        }
    }
}