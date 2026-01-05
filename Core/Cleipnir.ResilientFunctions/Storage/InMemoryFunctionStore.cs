using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Messaging;
using Cleipnir.ResilientFunctions.Storage.Session;
using Cleipnir.ResilientFunctions.Utils;
using Cleipnir.ResilientFunctions.Utils.Arbitrator;
using Cleipnir.ResilientFunctions.Utils.Register;

namespace Cleipnir.ResilientFunctions.Storage;

public class InMemoryFunctionStore : IFunctionStore, IMessageStore
{
    private readonly Dictionary<StoredId, InnerState> _states = new();
    private readonly Dictionary<StoredId, Dictionary<long, StoredMessage>> _messages = new();
    private readonly Lock _sync = new();

    public ITypeStore TypeStore { get; } = new InMemoryTypeStore();
    public IMessageStore MessageStore => this;
    private readonly InMemoryEffectsStore _effectsStore = new();
    public IEffectsStore EffectsStore => _effectsStore;
    private readonly InMemoryCorrelationStore _correlationStore = new();
    public ICorrelationStore CorrelationStore => _correlationStore;
    public Utilities Utilities { get; }
    public ISemaphoreStore SemaphoreStore { get; } = new InMemorySemaphoreStore();
    public IReplicaStore ReplicaStore { get; } = new InMemoryReplicaStore();

    public Task Initialize() => Task.CompletedTask;

    public InMemoryFunctionStore()
    {
        var underlyingRegister = new UnderlyingInMemoryRegister();
        Utilities = new Utilities(
            new Register(underlyingRegister),
            new Arbitrator(underlyingRegister)
        );
    }
    
    #region FunctionStore

    public virtual Task<IStorageSession?> CreateFunction(
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
        lock (_sync)
        {
            if (_states.ContainsKey(storedId))
                return Task.FromResult<IStorageSession?>(null);

            _states[storedId] = new InnerState
            {
                StoredId = storedId,
                HumanInstanceId = humanInstanceId.Value,
                Param = param,
                Status = postponeUntil == null ? Status.Executing : Status.Postponed,
                Exception = null,
                Result = null,
                Expires = postponeUntil ?? leaseExpiration,
                Timestamp = timestamp,
                Parent = parent,
                Owner = owner
            };
            if (!_messages.ContainsKey(storedId)) //messages can already have been added - i.e. paramless started by received message
                _messages[storedId] = new Dictionary<long, StoredMessage>();

            if (messages != null)
                for (var i = 0; i < messages.Count; i++)
                    _messages[storedId][i] = messages[i];                

            var session = owner == null ? null : new SnapshotStorageSession(owner);

            if (effects?.Any() ?? false)
                _effectsStore
                    .SetEffectResults(storedId, effects.Select(e => new StoredEffectChange(storedId, e.EffectId, Operation: CrudOperation.Insert, e)).ToList(), session: session)
                    .GetAwaiter()
                    .GetResult();

            return Task.FromResult<IStorageSession?>(session);
        }
    }

    public Task<int> BulkScheduleFunctions(IEnumerable<IdWithParam> functionsWithParam, StoredId? parent)
    {
        var insertedCount = 0;
        lock (_sync)
        {
            foreach (var (functionId, humanInstanceId, param) in functionsWithParam)
            {
                if (!_states.ContainsKey(functionId))
                {
                    _states[functionId] = new InnerState
                    {
                        StoredId = functionId,
                        HumanInstanceId = humanInstanceId,
                        Exception = null,
                        Expires = 0,
                        Param = param,
                        Result = null,
                        Status = Status.Postponed,
                        Parent = parent
                    };
                    insertedCount++;
                }
            }
        }

        return Task.FromResult(insertedCount);
    }

    public virtual async Task<StoredFlowWithEffectsAndMessages?> RestartExecution(StoredId storedId, ReplicaId owner)
    {
        lock (_sync)
        {
            if (!_states.ContainsKey(storedId))
                return null;

            var state = _states[storedId];
            if (state.Owner != null)
                return null;

            state.Status = Status.Executing;
            state.Expires = 0;
            state.Interrupted = false;
            state.Owner = owner;
        }
        var sf = await GetFunction(storedId);
        var effects = await EffectsStore.GetEffectResults(storedId);
        var messages = await MessageStore.GetMessages(storedId, skip: 0);

        var session = new SnapshotStorageSession(owner);
        foreach (var effect in effects)
            session.Effects[effect.EffectId] = effect;

        session.Version = _effectsStore.GetVersion(storedId);
        session.RowExists = effects.Any();

        return
            sf == null
                ? null
                : new StoredFlowWithEffectsAndMessages(
                    sf,
                    effects,
                    messages,
                    session
                );
    }

    public virtual async Task<Dictionary<StoredId, StoredFlowWithEffectsAndMessages>> RestartExecutions(
        IReadOnlyList<StoredId> storedIds,
        ReplicaId owner)
    {
        if (storedIds.Count == 0)
            return new Dictionary<StoredId, StoredFlowWithEffectsAndMessages>();

        var restartedIds = new List<StoredId>();

        // Restart eligible flows
        lock (_sync)
        {
            foreach (var storedId in storedIds)
            {
                if (!_states.ContainsKey(storedId))
                    continue;

                var state = _states[storedId];
                if (state.Owner != null)
                    continue; // Skip already owned flows

                // Restart this flow
                state.Status = Status.Executing;
                state.Expires = 0;
                state.Interrupted = false;
                state.Owner = owner;

                restartedIds.Add(storedId);
            }
        }
        
        var effectsDict = await EffectsStore.GetEffectResults(storedIds);
        var messagesDict = await MessageStore.GetMessages(storedIds);
        
        // Build result dictionary - only for restarted flows
        var result = new Dictionary<StoredId, StoredFlowWithEffectsAndMessages>();
        foreach (var storedId in restartedIds)
        {
            var sf = await GetFunction(storedId);
            if (sf == null) continue;

            var effects = effectsDict.TryGetValue(storedId, out var effs)
                ? effs
                : new List<StoredEffect>();
            var messages = messagesDict.TryGetValue(storedId, out var msgs)
                ? msgs
                : new List<StoredMessage>();

            var session = new SnapshotStorageSession(owner);
            foreach (var effect in effects)
                session.Effects[effect.EffectId] = effect;

            session.Version = _effectsStore.GetVersion(storedId);
            session.RowExists = effects.Any();

            result[storedId] = new StoredFlowWithEffectsAndMessages(sf, effects, messages, session);
        }

        return result;
    }
    
    public virtual Task<IReadOnlyList<StoredId>> GetExpiredFunctions(long expiresBefore)
    {
        lock (_sync)
            return _states
                .Values
                .Where(s => s.Status == Status.Postponed)
                .Where(s => s.Expires <= expiresBefore)
                .Select(s => s.StoredId)
                .ToList()
                .CastTo<IReadOnlyList<StoredId>>()
                .ToTask();
    }

    public Task<IReadOnlyList<StoredId>> GetSucceededFunctions(long completedBefore)
    {
        lock (_sync)
            return _states
                .Values
                .Where(s => s.Timestamp < completedBefore)
                .Select(s => s.StoredId)
                .ToList()
                .CastTo<IReadOnlyList<StoredId>>()
                .ToTask();
    }

    public Task<IReadOnlyList<StoredId>> GetInterruptedFunctions(IEnumerable<StoredId> ids)
    {
        var idsList = ids.ToHashSet();
        if (idsList.Count == 0)
            return Array.Empty<StoredId>().CastTo<IReadOnlyList<StoredId>>().ToTask();

        lock (_sync)
            return _states
                .Where(kv => idsList.Contains(kv.Key) && kv.Value.Interrupted)
                .Select(kv => kv.Key)
                .ToList()
                .CastTo<IReadOnlyList<StoredId>>()
                .ToTask();
    }

    public virtual Task<bool> SetFunctionState(
        StoredId storedId,
        Status status,
        byte[]? param,
        byte[]? result,
        StoredException? storedException,
        long expires,
        ReplicaId? expectedReplica)
    {
        lock (_sync)
        {
            if (!_states.ContainsKey(storedId))
                return false.ToTask();

            var state = _states[storedId];
            if (state.Owner != expectedReplica)
                return false.ToTask();

            state.Status = status;
            state.Param = param;
            state.Result = result;
            state.Exception = storedException;
            state.Expires = expires;

            return true.ToTask();
        }
    }

    public Task<bool> SucceedFunction(
        StoredId storedId,
        byte[]? result,
        long timestamp,
        ReplicaId? expectedReplica,
        IReadOnlyList<StoredEffect>? effects,
        IReadOnlyList<StoredMessage>? messages,
        IStorageSession? storageSession)
    {
        lock (_sync)
        {
            if (!_states.ContainsKey(storedId)) return false.ToTask();

            var state = _states[storedId];
            if (state.Owner != expectedReplica) return false.ToTask();

            state.Status = Status.Succeeded;
            state.Result = result;
            state.Timestamp = timestamp;
            state.Owner = null;

            return true.ToTask();
        }
    }

    public Task<bool> PostponeFunction(
        StoredId storedId,
        long postponeUntil,
        long timestamp,
        ReplicaId? expectedReplica,
        IReadOnlyList<StoredEffect>? effects,
        IReadOnlyList<StoredMessage>? messages,
        IStorageSession? storageSession)
    {
        lock (_sync)
        {
            if (!_states.ContainsKey(storedId)) return false.ToTask();

            var state = _states[storedId];
            if (state.Owner != expectedReplica) return false.ToTask();

            state.Status = Status.Postponed;
            state.Expires = state.Interrupted ? 0 : postponeUntil;
            state.Interrupted = false;
            state.Timestamp = timestamp;
            state.Owner = null;

            return true.ToTask();
        }
    }

    public Task<bool> FailFunction(
        StoredId storedId,
        StoredException storedException,
        long timestamp,
        ReplicaId? expectedReplica,
        IReadOnlyList<StoredEffect>? effects,
        IReadOnlyList<StoredMessage>? messages,
        IStorageSession? storageSession)
    {
        lock (_sync)
        {
            if (!_states.ContainsKey(storedId)) return false.ToTask();

            var state = _states[storedId];
            if (state.Owner != expectedReplica) return false.ToTask();

            state.Status = Status.Failed;
            state.Exception = storedException;
            state.Timestamp = timestamp;
            state.Owner = null;

            return true.ToTask();
        }
    }

    public Task<bool> SuspendFunction(
        StoredId storedId,
        long timestamp,
        ReplicaId? expectedReplica,
        IReadOnlyList<StoredEffect>? effects,
        IReadOnlyList<StoredMessage>? messages,
        IStorageSession? storageSession)
    {
        lock (_sync)
        {
            if (!_states.ContainsKey(storedId))
                return false.ToTask();

            var state = _states[storedId];
            if (state.Owner != expectedReplica)
                return false.ToTask();

            state.Status = state.Interrupted ? Status.Postponed : Status.Suspended;
            state.Expires = 0;
            state.Timestamp = timestamp;
            state.Owner = null;
            state.Interrupted = false;

            return true.ToTask();
        }
    }

    public Task<IReadOnlyList<ReplicaId>> GetOwnerReplicas()
    {
        lock (_sync)
            return _states.Values
                .Select(s => s.Owner)
                .Where(owner => owner != null)
                .Distinct()
                .ToList()
                .CastTo<IReadOnlyList<ReplicaId>>()
                .ToTask();
    }

    public Task RescheduleCrashedFunctions(ReplicaId replicaId)
    {
        lock (_sync)
            foreach (var state in _states.Values.Where(v => v.Owner == replicaId).ToList())
            {
                state.Owner = null;
                state.Status = Status.Postponed;
                state.Expires = 0;
            }
        
        return Task.CompletedTask;
    }

    public virtual Task<bool> SetParameters(
        StoredId storedId, 
        byte[]? param, 
        byte[]? result, 
        ReplicaId? expectedReplica)
    {
        lock (_sync)
        {
            if (!_states.ContainsKey(storedId)) return false.ToTask();
            var state = _states[storedId];
            if (state.Owner != expectedReplica) return false.ToTask();
            
            state.Param = param;
            state.Result = result;

            return true.ToTask();
        }
    }

    public Task<bool> Interrupt(StoredId storedId)
    {
        lock (_sync)
        {
            if (!_states.TryGetValue(storedId, out var state))
                return false.ToTask();
            
            if (state.Status == Status.Postponed || state.Status == Status.Suspended)
            {
                state.Status = Status.Postponed;
                state.Expires = 0;
            }

            state.Interrupted = true;
            return true.ToTask();
        }
    }

    public async Task Interrupt(IReadOnlyList<StoredId> storedIds)
    {
        foreach (var storedId in storedIds)
            await Interrupt(storedId);
    }

    public Task<bool?> Interrupted(StoredId storedId)
    {
        lock (_sync)
        {
            if (!_states.ContainsKey(storedId))
                return Task.FromResult(default(bool?));

            return ((bool?) _states[storedId].Interrupted).ToTask();
        }
    }

    public Task<Status?> GetFunctionStatus(StoredId storedId)
    {
        lock (_sync)
        {
            if (!_states.ContainsKey(storedId))
                return Task.FromResult(default(Status?));

            var state = _states[storedId];
            return ((Status?) state.Status).ToTask();
        }
    }

    public async Task<IReadOnlyList<StatusAndId>> GetFunctionsStatus(IEnumerable<StoredId> storedIds)
    {
        var toReturn = new List<StatusAndId>();
        foreach (var a in storedIds.Select(id => new { Id = id, Task = GetFunction(id)}))
        {
            var sf = await a.Task;
            if (sf != null)
                toReturn.Add(new StatusAndId(a.Id, sf.Status, sf.Expires));
        }

        return toReturn;
    }

    public virtual Task<StoredFlow?> GetFunction(StoredId storedId)
    {
        lock (_sync)
        {
            if (!_states.ContainsKey(storedId))
                return default(StoredFlow).ToTask();

            var state = _states[storedId];

            return new StoredFlow(
                    storedId,
                    state.HumanInstanceId,
                    state.Param,
                    state.Status,
                    state.Exception,
                    state.Expires,
                    state.Timestamp,
                    state.Interrupted,
                    state.Parent,
                    state.Owner,
                    storedId.Type
                )
                .ToNullable()
                .ToTask();
        }
    }

    public Task<IReadOnlyList<StoredId>> GetInstances(StoredType storedType, Status status)
    {
        lock (_sync)
            return _states
                .Where(kv => kv.Key.Type == storedType)
                .Where(kv => kv.Value.Status == status)
                .Select(kv => kv.Key)
                .ToList()
                .CastTo<IReadOnlyList<StoredId>>()
                .ToTask();
    }

    public Task<IReadOnlyList<StoredId>> GetInstances(StoredType storedType)
    {
        lock (_sync)
            return _states
                .Where(kv => kv.Key.Type == storedType)
                .Select(kv => kv.Key)
                .ToList()
                .CastTo<IReadOnlyList<StoredId>>()
                .ToTask();
    }

    public Task<IReadOnlyList<StoredType>> GetTypes()
    {
        lock (_sync)
            return _states
                .Select(kv => kv.Key.Type)
                .Distinct()
                .ToList()
                .CastTo<IReadOnlyList<StoredType>>()
                .ToTask();
    }

    public virtual Task<bool> DeleteFunction(StoredId storedId)
    {
        lock (_sync)
        {
            _messages.Remove(storedId);
            _effectsStore.Remove(storedId);
            _correlationStore.RemoveCorrelations(storedId);
            
            return _states.Remove(storedId).ToTask();
        }
    }

    public IFunctionStore WithPrefix(string prefix) => new InMemoryFunctionStore();

    public Task<IReadOnlyDictionary<StoredId, byte[]?>> GetResults(IEnumerable<StoredId> storedIds)
    {
        lock (_sync)
        {
            var results = new Dictionary<StoredId, byte[]?>();
            foreach (var storedId in storedIds)
            {
                if (_states.TryGetValue(storedId, out var state))
                    results[storedId] = state.Result;
            }

            return results.CastTo<IReadOnlyDictionary<StoredId, byte[]?>>().ToTask();
        }
    }

    public Task SetResult(StoredId storedId, byte[] result, ReplicaId expectedReplica)
    {
        lock (_sync)
        {
            if (!_states.ContainsKey(storedId))
                return Task.CompletedTask;

            var state = _states[storedId];
            if (state.Owner != expectedReplica)
                return Task.CompletedTask;

            state.Result = result;
            return Task.CompletedTask;
        }
    }

    private class InnerState
    {
        public StoredId StoredId { get; init; } = null!;
        public string HumanInstanceId { get; init; } = null!;
        public byte[]? Param { get; set; }
        public Status Status { get; set; }
        public byte[]? Result { get; set; }
        public StoredException? Exception { get; set; }
        public bool Interrupted { get; set; }
        public long Expires { get; set; }
        public long Timestamp { get; set; }
        public StoredId? Parent { get; set; }
        public ReplicaId? Owner { get; set; }
    }
    #endregion
    
    #region MessageStore

    public virtual Task AppendMessage(StoredId storedId, StoredMessage storedMessage)
    {
        lock (_sync)
        {
            if (!_messages.ContainsKey(storedId))
                _messages[storedId] = new Dictionary<long, StoredMessage>();

            var messages = _messages[storedId];
            messages[messages.Count] = storedMessage;

            return Task.CompletedTask;
        }
    }

    public async Task AppendMessages(IReadOnlyList<StoredIdAndMessage> messages)
    {
        foreach (var (storedId, storedMessage) in messages)
        {
            await AppendMessage(storedId, storedMessage);
            await Interrupt(storedId);
        }
    }

    public async Task AppendMessages(IReadOnlyList<StoredIdAndMessageWithPosition> messages)
    {
        foreach (var (storedId, storedMessage, position) in messages)
        {
            lock (_sync)
            {
                if (!_messages.ContainsKey(storedId))
                    _messages[storedId] = new Dictionary<long, StoredMessage>();

                _messages[storedId].Add(position, storedMessage);
            }

            await Interrupt(storedId);
        }
    }

    public Task<bool> ReplaceMessage(StoredId storedId, long position, StoredMessage storedMessage)
    {
        lock (_sync)
        {
            if (!_messages.ContainsKey(storedId) || _messages[storedId].Count <= position)
                return false.ToTask();

            _messages[storedId][(int)position] = storedMessage;
            return true.ToTask();
        }
    }

    public Task DeleteMessages(StoredId storedId, IEnumerable<long> positions)
    {
        lock (_sync)
        {
            if (!_messages.ContainsKey(storedId))
                return Task.CompletedTask;

            var messages = _messages[storedId];
            foreach (var position in positions)
                messages.Remove(position);

            return Task.CompletedTask;
        }
    }

    public virtual Task Truncate(StoredId storedId)
    {
        lock (_sync)
            _messages[storedId] = new Dictionary<long, StoredMessage>();

        return Task.CompletedTask;
    }

    private IEnumerable<StoredMessage> GetMessages(StoredId storedId)
    {
        lock (_sync)
            return !_messages.ContainsKey(storedId)
                ? Enumerable.Empty<StoredMessage>()
                : _messages[storedId].OrderBy(kv => kv.Key).Select(kv => kv.Value with { Position = kv.Key }).ToList();
    }

    public virtual Task<IReadOnlyList<StoredMessage>> GetMessages(StoredId storedId, long skip)
        => ((IReadOnlyList<StoredMessage>)GetMessages(storedId).Skip((int)skip).ToList()).ToTask();

    public virtual Task<IReadOnlyList<StoredMessage>> GetMessages(StoredId storedId, IReadOnlyList<long> skipPositions)
        => ((IReadOnlyList<StoredMessage>)GetMessages(storedId).Where(m => !skipPositions.Contains(m.Position)).ToList()).ToTask();

    public async Task<Dictionary<StoredId, List<StoredMessage>>> GetMessages(IEnumerable<StoredId> storedIds)
    {
        var dict = new Dictionary<StoredId, List<StoredMessage>>();
        foreach (var storedId in storedIds)
        {
            dict[storedId] = (await GetMessages(storedId, skip: 0)).ToList();
        }

        return dict;
    }

    public Task<IDictionary<StoredId, long>> GetMaxPositions(IReadOnlyList<StoredId> storedIds)
    {
        IDictionary<StoredId, long> positions = new Dictionary<StoredId, long>();
        foreach (var storedId in storedIds)
            positions[storedId] = GetMessages(storedId).Count() - 1;

        return positions.ToTask();
    }

    #endregion
}