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

namespace Cleipnir.ResilientFunctions.SqlServer;

public class SqlServerEffectsStore(string connectionString, string tablePrefix = "") : IEffectsStore
{
    private readonly string _tableName = tablePrefix == "" ? "RFunctions" : tablePrefix;
    private readonly SqlServerStateStore _sqlServerStateStore = new(connectionString, tablePrefix);
    private readonly SqlServerCommandExecutor _commandExecutor = new(connectionString);

    public async Task Initialize() => await _sqlServerStateStore.Initialize();
    public async Task Truncate()
    {
        var command = StoreCommand.Create($"UPDATE {_tableName} SET Effects = NULL");
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
                $"SELECT Owner FROM {_tableName} WHERE Id = @Id"
            );
            ownerCmd.AddParameter("@Id", storedId.AsGuid);
            owner = (await _commandExecutor.ExecuteScalar(ownerCmd) as Guid?)?.ToReplicaId();
        }

        foreach (var change in changes)
            if (change.Operation == CrudOperation.Delete)
                existingEffects.Remove(change.EffectId);
            else
                existingEffects[change.EffectId] = change.StoredEffect!;

        var content = SnapshotStorageSession.Serialize(existingEffects);

        var command = owner != null
            ? StoreCommand.Create(
                $"UPDATE {_tableName} SET Effects = @Effects WHERE Id = @Id AND Owner = @Owner"
            )
            : StoreCommand.Create(
                $"UPDATE {_tableName} SET Effects = @Effects WHERE Id = @Id AND Owner IS NULL"
            );

        command.AddParameter("@Effects", content);
        command.AddParameter("@Id", storedId.AsGuid);
        if (owner != null)
            command.AddParameter("@Owner", owner.AsGuid);

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

        var sql = @$"
            SELECT Id, Effects
            FROM {_tableName}
            WHERE Id IN ({storedIdsList.InClause()})";

        var command = StoreCommand.Create(sql);
        await using var reader = await _commandExecutor.Execute(command);

        foreach (var storedId in storedIdsList)
            toReturn[storedId] = new List<StoredEffect>();

        while (await reader.ReadAsync())
        {
            var id = reader.GetGuid(0).ToStoredId();
            var hasEffects = !await reader.IsDbNullAsync(1);

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
        var sql = $@"UPDATE {_tableName} SET Effects = NULL WHERE Id = @Id";
        var command = StoreCommand.Create(sql);
        command.AddParameter("@Id", storedId.AsGuid);
        await _commandExecutor.ExecuteNonQuery(command);
    }
}