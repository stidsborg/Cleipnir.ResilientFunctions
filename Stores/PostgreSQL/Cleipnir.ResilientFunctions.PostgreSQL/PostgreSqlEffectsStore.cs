using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Domain.Exceptions;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Storage.Session;
using Cleipnir.ResilientFunctions.Storage.Utils;

namespace Cleipnir.ResilientFunctions.PostgreSQL;

public class PostgreSqlEffectsStore(string connectionString, string tablePrefix = "") : IEffectsStore
{
    private readonly PostgresCommandExecutor _commandExecutor = new(connectionString);
    private readonly string _tableName = tablePrefix == "" ? "rfunctions" : tablePrefix;

    public Task Initialize() => Task.CompletedTask;

    public async Task Truncate()
    {
        var command = StoreCommand.Create($"UPDATE {_tableName} SET effects = NULL");
        await _commandExecutor.ExecuteNonQuery(command);
    }

    public async Task SetEffectResults(StoredId storedId, IReadOnlyList<StoredEffectChange> changes, IStorageSession? session)
    {
        if (changes.Count == 0)
            return;

        var owner = default(ReplicaId);
        var existingEffects = new Dictionary<EffectId, StoredEffect>();
        var snapshotSession = session as SnapshotStorageSession;
        if (snapshotSession != null)
        {
            existingEffects = snapshotSession.Effects;
            owner = snapshotSession.ReplicaId;
        }
        else
        {
            var effects = (await GetEffectResults([storedId]))[storedId];
            foreach (var e in effects)
                existingEffects[e.EffectId] = e;

            var ownerCmd = StoreCommand.Create(
                $"SELECT owner FROM {_tableName} WHERE id = $1",
                [storedId.AsGuid]
            );
            var ownerValue = await _commandExecutor.ExecuteScalar(ownerCmd);
            owner = ownerValue != null && ownerValue != DBNull.Value ? ((Guid)ownerValue).ToReplicaId() : null;
        }

        foreach (var change in changes)
            if (change.Operation == CrudOperation.Delete)
                existingEffects.Remove(change.EffectId);
            else
                existingEffects[change.EffectId] = change.StoredEffect!;

        var content = SnapshotStorageSession.Serialize(existingEffects);

        var command =
            owner != null
                ? StoreCommand.Create(
                    $"UPDATE {_tableName} SET effects = $1 WHERE id = $2 AND owner = $3",
                    [content, storedId.AsGuid, owner.AsGuid])
                : StoreCommand.Create(
                    $"UPDATE {_tableName} SET effects = $1 WHERE id = $2 AND owner IS NULL",
                    [content, storedId.AsGuid]);

        var affectedRows = await _commandExecutor.ExecuteNonQuery(command);
        if (affectedRows == 0)
            throw UnexpectedStateException.ConcurrentModification(storedId);
    }

    public async Task<Dictionary<StoredId, List<StoredEffect>>> GetEffectResults(IEnumerable<StoredId> storedIds)
    {
        var storedIdsList = storedIds.ToList();
        var toReturn = new Dictionary<StoredId, List<StoredEffect>>();

        if (!storedIdsList.Any())
            return toReturn;

        var sql = $@"
            SELECT id, effects
            FROM {_tableName}
            WHERE id = ANY($1)";

        var cmd = StoreCommand.Create(sql, [storedIdsList.Select(s => s.AsGuid).ToList()]);
        await using var reader = await _commandExecutor.Execute(cmd);

        foreach (var storedId in storedIdsList)
            toReturn[storedId] = new List<StoredEffect>();

        while (await reader.ReadAsync())
        {
            var id = reader.GetGuid(0).ToStoredId();
            var hasEffects = !reader.IsDbNull(1);

            if (hasEffects)
            {
                var effectsContent = (byte[])reader.GetValue(1);
                var effectsBytes = BinaryPacker.Split(effectsContent);
                var storedEffects = effectsBytes.Select(effectBytes => StoredEffect.Deserialize(effectBytes!)).ToList();
                toReturn[id] = storedEffects;
            }
        }

        return toReturn;
    }
    
    public async Task Remove(StoredId storedId)
    {
        var sql = $@"UPDATE {_tableName} SET effects = NULL WHERE id = $1";
        var command = StoreCommand.Create(sql, [storedId.AsGuid]);
        await _commandExecutor.ExecuteNonQuery(command);
    }
}