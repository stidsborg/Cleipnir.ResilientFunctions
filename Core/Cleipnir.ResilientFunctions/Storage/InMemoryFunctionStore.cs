using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Messaging;
using Cleipnir.ResilientFunctions.Utils;
using Cleipnir.ResilientFunctions.Utils.Arbitrator;
using Cleipnir.ResilientFunctions.Utils.Register;

namespace Cleipnir.ResilientFunctions.Storage;

public class InMemoryFunctionStore : IFunctionStore, IMessageStore
{
    private readonly Dictionary<StoredId, InnerState> _states = new();
    private readonly Dictionary<StoredId, List<StoredMessage>> _messages = new();
    private readonly Lock _sync = new();

    public ITypeStore TypeStore { get; } = new InMemoryTypeStore();
    public IMessageStore MessageStore => this;
    private readonly InMemoryEffectsStore _effectsStore = new();
    public IEffectsStore EffectsStore => _effectsStore;
    private readonly InMemoryTimeoutStore _timeoutStore = new();
    public ITimeoutStore TimeoutStore => _timeoutStore;
    private readonly InMemoryCorrelationStore _correlationStore = new();
    public ICorrelationStore CorrelationStore => _correlationStore;
    public Utilities Utilities { get; }
    
    public IMigrator Migrator { get; } = new InMemoryMigrator();
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

    public virtual Task<bool> CreateFunction(
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
                return false.ToTask();

            _states[storedId] = new InnerState
            {
                StoredId = storedId,
                HumanInstanceId = humanInstanceId.Value,
                Param = param,
                Status = postponeUntil == null ? Status.Executing : Status.Postponed,
                Epoch = 0,
                Exception = null,
                Result = null,
                Expires = postponeUntil ?? leaseExpiration,
                Timestamp = timestamp,
                Parent = parent,
                Owner = owner
            };
            if (!_messages.ContainsKey(storedId)) //messages can already have been added - i.e. paramless started by received message
                _messages[storedId] = new List<StoredMessage>();
            
            _messages[storedId].AddRange(messages ?? []);

            if (effects?.Any() ?? false)
                _effectsStore
                    .SetEffectResults(storedId, effects.Select(e => new StoredEffectChange(storedId, e.StoredEffectId, Operation: CrudOperation.Insert, e)).ToList())
                    .GetAwaiter()
                    .GetResult();

            return true.ToTask();
        }
    }

    public Task BulkScheduleFunctions(IEnumerable<IdWithParam> functionsWithParam, StoredId? parent)
    {
        lock (_sync)
        {
            foreach (var (functionId, humanInstanceId, param) in functionsWithParam)
            {
                if (!_states.ContainsKey(functionId))
                    _states[functionId] = new InnerState
                    {
                        StoredId = functionId,
                        HumanInstanceId = humanInstanceId,
                        Epoch = 0,
                        Exception = null,
                        Expires = 0,
                        Param = param,
                        Result = null,
                        Status = Status.Postponed,
                        Parent = parent
                    };
            }
        }

        return Task.CompletedTask;
    }

    public virtual async Task<StoredFlowWithEffectsAndMessages?> RestartExecution(StoredId storedId, int expectedEpoch, long leaseExpiration, ReplicaId owner)
    {
        lock (_sync)
        {
            if (!_states.ContainsKey(storedId))
                return null;

            var state = _states[storedId];
            if (state.Epoch != expectedEpoch)
                return null;

            state.Epoch += 1;
            state.Status = Status.Executing;
            state.Expires = leaseExpiration;
            state.Interrupted = false;
            state.Owner = owner;
        }
        var sf = await GetFunction(storedId);
        var effects = await EffectsStore.GetEffectResults(storedId);
        var messages = await MessageStore.GetMessages(storedId, skip: 0);
        return
            sf == null
                ? null
                : new StoredFlowWithEffectsAndMessages(
                    sf,
                    effects,
                    messages
                );
    }

    public virtual Task<bool> RenewLease(StoredId storedId, int expectedEpoch, long leaseExpiration)
    {
        lock (_sync)
        {
            if (!_states.ContainsKey(storedId))
                return false.ToTask();

            var state = _states[storedId];
            if (state.Epoch != expectedEpoch)
                return false.ToTask();

            state.Expires = leaseExpiration;
            return true.ToTask();
        }
    }
    
    public async Task<int> RenewLeases(IReadOnlyList<LeaseUpdate> leaseUpdates, long leaseExpiration)
    {
        var affected = 0;
        foreach (var (id, expectedEpoch) in leaseUpdates)
        {
            var success = await RenewLease(id, expectedEpoch, leaseExpiration);
            if (success)
                affected++;
        }

        return affected;
    }

    public virtual Task<IReadOnlyList<IdAndEpoch>> GetExpiredFunctions(long expiresBefore)
    {
        lock (_sync)
            return _states
                .Values
                .Where(s => s.Status == Status.Executing || s.Status == Status.Postponed)
                .Where(s => s.Expires <= expiresBefore)
                .Select(s => new IdAndEpoch(s.StoredId, s.Epoch))
                .ToList()
                .CastTo<IReadOnlyList<IdAndEpoch>>()
                .ToTask();
    }

    public Task<IReadOnlyList<StoredInstance>> GetSucceededFunctions(StoredType storedType, long completedBefore)
    {
        lock (_sync)
            return _states
                .Values
                .Where(s => s.StoredId.Type == storedType && s.Timestamp < completedBefore)
                .Select(s => s.StoredId.Instance)
                .ToList()
                .CastTo<IReadOnlyList<StoredInstance>>()
                .ToTask();
    }

    public virtual Task<bool> SetFunctionState(
        StoredId storedId,
        Status status,
        byte[]? param,
        byte[]? result,
        StoredException? storedException,
        long expires,
        int expectedEpoch)
    {
        lock (_sync)
        {
            if (!_states.ContainsKey(storedId))
                return false.ToTask();

            var state = _states[storedId];
            if (state.Epoch != expectedEpoch)
                return false.ToTask();

            state.Status = status;
            state.Param = param;
            state.Result = result;
            state.Exception = storedException;
            state.Expires = expires;

            state.Epoch += 1;

            return true.ToTask();
        }
    }

    public Task<bool> SucceedFunction(
        StoredId storedId, 
        byte[]? result, 
        long timestamp,
        int expectedEpoch, 
        IReadOnlyList<StoredEffect>? effects,
        IReadOnlyList<StoredMessage>? messages,
        ComplimentaryState complimentaryState)
    {
        lock (_sync)
        {
            if (!_states.ContainsKey(storedId)) return false.ToTask();

            var state = _states[storedId];
            if (state.Epoch != expectedEpoch) return false.ToTask();

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
        bool ignoreInterrupted,
        int expectedEpoch, 
        IReadOnlyList<StoredEffect>? effects,
        IReadOnlyList<StoredMessage>? messages,
        ComplimentaryState complimentaryState)
    {
        lock (_sync)
        {
            if (!_states.ContainsKey(storedId)) return false.ToTask();

            var state = _states[storedId];
            if (state.Epoch != expectedEpoch) return false.ToTask();

            if (!ignoreInterrupted && state.Interrupted)
                return false.ToTask();
            
            state.Status = Status.Postponed;
            state.Expires = postponeUntil;
            state.Timestamp = timestamp;
            state.Owner = null;
            
            return true.ToTask();
        }
    }

    public Task<bool> FailFunction(
        StoredId storedId, 
        StoredException storedException, 
        long timestamp,
        int expectedEpoch, 
        IReadOnlyList<StoredEffect>? effects,
        IReadOnlyList<StoredMessage>? messages,
        ComplimentaryState complimentaryState)
    {
        lock (_sync)
        {
            if (!_states.ContainsKey(storedId)) return false.ToTask();

            var state = _states[storedId];
            if (state.Epoch != expectedEpoch) return false.ToTask();

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
        int expectedEpoch, 
        IReadOnlyList<StoredEffect>? effects,
        IReadOnlyList<StoredMessage>? messages,
        ComplimentaryState complimentaryState)
    {
        lock (_sync)
        {
            if (!_states.ContainsKey(storedId))
                return false.ToTask();

            var state = _states[storedId];
            if (state.Epoch != expectedEpoch)
                return false.ToTask();

            if (state.Interrupted)
                return false.ToTask();
                
            state.Status = Status.Suspended;
            state.Timestamp = timestamp;
            state.Owner = null;
            
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
                state.Epoch += 1;
            }
        
        return Task.CompletedTask;
    }

    public virtual Task<bool> SetParameters(
        StoredId storedId, 
        byte[]? param, 
        byte[]? result, 
        int expectedEpoch)
    {
        lock (_sync)
        {
            if (!_states.ContainsKey(storedId)) return false.ToTask();
            var state = _states[storedId];
            if (state.Epoch != expectedEpoch) return false.ToTask();
            
            state.Param = param;
            state.Result = result;
            
            state.Epoch += 1;

            return true.ToTask();
        }
    }

    public Task<bool> Interrupt(StoredId storedId, bool onlyIfExecuting)
    {
        lock (_sync)
        {
            if (!_states.TryGetValue(storedId, out var state))
                return false.ToTask();
            
            if (state.Status != Status.Executing && onlyIfExecuting)
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
            await Interrupt(storedId, onlyIfExecuting: false);
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

    public Task<StatusAndEpoch?> GetFunctionStatus(StoredId storedId)
    {
        lock (_sync)
        {
            if (!_states.ContainsKey(storedId))
                return Task.FromResult(default(StatusAndEpoch));

            var state = _states[storedId];
            return ((StatusAndEpoch?) new StatusAndEpoch(state.Status, state.Epoch)).ToTask();
        }
    }

    public async Task<IReadOnlyList<StatusAndEpochWithId>> GetFunctionsStatus(IEnumerable<StoredId> storedIds)
    {
        var toReturn = new List<StatusAndEpochWithId>();
        foreach (var a in storedIds.Select(id => new { Id = id, Task = GetFunction(id)}))
        {
            var sf = await a.Task;
            if (sf != null)
                toReturn.Add(new StatusAndEpochWithId(a.Id, sf.Status, sf.Epoch, sf.Expires));
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
                    state.Result,
                    state.Exception,
                    state.Epoch,
                    state.Expires,
                    state.Timestamp,
                    state.Interrupted,
                    state.Parent,
                    state.Owner
                )
                .ToNullable()
                .ToTask();
        }
    }

    public Task<IReadOnlyList<StoredInstance>> GetInstances(StoredType storedType, Status status)
    {
        lock (_sync)
            return _states
                .Where(kv => kv.Key.Type == storedType)
                .Where(kv => kv.Value.Status == status)
                .Select(kv => kv.Key.Instance)
                .ToList()
                .CastTo<IReadOnlyList<StoredInstance>>()
                .ToTask();
    }

    public Task<IReadOnlyList<StoredInstance>> GetInstances(StoredType storedType)
    {
        lock (_sync)
            return _states
                .Where(kv => kv.Key.Type == storedType)
                .Select(kv => kv.Key.Instance)
                .ToList()
                .CastTo<IReadOnlyList<StoredInstance>>()
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
            _timeoutStore.Remove(storedId);
            _correlationStore.RemoveCorrelations(storedId);
            
            return _states.Remove(storedId).ToTask();
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
        public int Epoch { get; set; }
        public bool Interrupted { get; set; }
        public long Expires { get; set; }
        public long Timestamp { get; set; }
        public StoredId? Parent { get; set; }
        public ReplicaId? Owner { get; set; }
    }
    #endregion
    
    #region MessageStore

    public virtual Task<FunctionStatus?> AppendMessage(StoredId storedId, StoredMessage storedMessage)
    {
        lock (_sync)
        {
            if (!_messages.ContainsKey(storedId))
                _messages[storedId] = new List<StoredMessage>();

            var messages = _messages[storedId];
            messages.Add(storedMessage);

            if (!_states.ContainsKey(storedId))
                return Task.FromResult(default(FunctionStatus));
            
            return Task.FromResult((FunctionStatus?)
                new FunctionStatus(_states[storedId].Status, Epoch: _states[storedId].Epoch)
            );
        }
    }

    public async Task AppendMessages(IReadOnlyList<StoredIdAndMessage> messages, bool interrupt = true)
    {
        foreach (var (storedId, storedMessage) in messages)
        {
            await AppendMessage(storedId, storedMessage);
            if (interrupt)
                await Interrupt(storedId, onlyIfExecuting: false);
        }
    }

    public async Task AppendMessages(IReadOnlyList<StoredIdAndMessageWithPosition> messages, bool interrupt = true)
    {
        foreach (var (storedId, storedMessage, position) in messages)
        {
            lock (_sync)
            {
                if (!_messages.ContainsKey(storedId))
                    _messages[storedId] = new List<StoredMessage>();

                var flowMessages = _messages[storedId];
                flowMessages.Insert(position, storedMessage);
            }
            
            if (interrupt)
                await Interrupt(storedId, onlyIfExecuting: false);
        }
    }

    public Task<bool> ReplaceMessage(StoredId storedId, int position, StoredMessage storedMessage)
    {
        lock (_sync)
        {
            if (!_messages.ContainsKey(storedId) || _messages[storedId].Count <= position)
                return false.ToTask();
            
            _messages[storedId][position] = storedMessage;
            return true.ToTask();
        }
    }

    public virtual Task Truncate(StoredId storedId)
    {
        lock (_sync)
            _messages[storedId] = new List<StoredMessage>();

        return Task.CompletedTask;
    }

    private IEnumerable<StoredMessage> GetMessages(StoredId storedId)
    {
        lock (_sync)
            return !_messages.ContainsKey(storedId) 
                ? Enumerable.Empty<StoredMessage>() 
                : _messages[storedId].ToList();
    }

    public virtual Task<IReadOnlyList<StoredMessage>> GetMessages(StoredId storedId, int skip)
        => ((IReadOnlyList<StoredMessage>)GetMessages(storedId).Skip(skip).ToList()).ToTask();

    public Task<IDictionary<StoredId, int>> GetMaxPositions(IReadOnlyList<StoredId> storedIds)
    {
        IDictionary<StoredId, int> positions = new Dictionary<StoredId, int>();
        foreach (var storedId in storedIds)
            positions[storedId] = GetMessages(storedId).Count() - 1;
        
        return positions.ToTask();
    }

    #endregion
}