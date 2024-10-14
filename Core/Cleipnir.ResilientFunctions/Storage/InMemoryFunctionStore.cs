using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Messaging;
using Cleipnir.ResilientFunctions.Utils;
using Cleipnir.ResilientFunctions.Utils.Arbitrator;
using Cleipnir.ResilientFunctions.Utils.Monitor;
using Cleipnir.ResilientFunctions.Utils.Register;

namespace Cleipnir.ResilientFunctions.Storage;

public class InMemoryFunctionStore : IFunctionStore, IMessageStore
{
    private readonly Dictionary<FlowId, InnerState> _states = new();
    private readonly Dictionary<FlowId, List<StoredMessage>> _messages = new();
    private readonly object _sync = new();

    public IMessageStore MessageStore => this;
    private readonly InMemoryEffectsStore _effectsStore = new();
    public IEffectsStore EffectsStore => _effectsStore;
    private readonly InMemoryTimeoutStore _timeoutStore = new();
    public ITimeoutStore TimeoutStore => _timeoutStore;
    private readonly InMemoryCorrelationStore _correlationStore = new();
    public ICorrelationStore CorrelationStore => _correlationStore;
    public Utilities Utilities { get; }
    
    public IMigrator Migrator { get; } = new InMemoryMigrator();

    public Task Initialize() => Task.CompletedTask;

    public InMemoryFunctionStore()
    {
        var underlyingRegister = new UnderlyingInMemoryRegister();
        Utilities = new Utilities(
            new Monitor(underlyingRegister),
            new Register(underlyingRegister),
            new Arbitrator(underlyingRegister)
        );
    }
    
    #region FunctionStore

    public virtual Task<bool> CreateFunction(
        FlowId flowId,
        string? param,
        long leaseExpiration,
        long? postponeUntil,
        long timestamp)
    {
        lock (_sync)
        {
            if (_states.ContainsKey(flowId))
                return false.ToTask();

            _states[flowId] = new InnerState
            {
                FlowId = flowId,
                Param = param,
                Status = postponeUntil == null ? Status.Executing : Status.Postponed,
                Epoch = 0,
                Exception = null,
                Result = null,
                Expires = postponeUntil ?? leaseExpiration,
                Timestamp = timestamp
            };
            if (!_messages.ContainsKey(flowId)) //messages can already have been added - i.e. paramless started by received message
                _messages[flowId] = new List<StoredMessage>();

            return true.ToTask();
        }
    }

    public Task BulkScheduleFunctions(IEnumerable<IdWithParam> functionsWithParam)
    {
        lock (_sync)
        {
            foreach (var (functionId, param) in functionsWithParam)
            {
                if (!_states.ContainsKey(functionId))
                    _states[functionId] = new InnerState
                    {
                        FlowId = functionId,
                        DefaultState = null,
                        Epoch = 0,
                        Exception = null,
                        InterruptCount = 0,
                        Expires = 0,
                        Param = param,
                        Result = null,
                        Status = Status.Postponed
                    };
            }
        }

        return Task.CompletedTask;
    }

    public virtual Task<StoredFlow?> RestartExecution(FlowId flowId, int expectedEpoch, long leaseExpiration)
    {
        lock (_sync)
        {
            if (!_states.ContainsKey(flowId))
                return default(StoredFlow).ToTask();

            var state = _states[flowId];
            if (state.Epoch != expectedEpoch)
                return default(StoredFlow).ToTask();

            state.Epoch += 1;
            state.Status = Status.Executing;
            state.Expires = leaseExpiration;
            return GetFunction(flowId);
        }
    }

    public virtual Task<bool> RenewLease(FlowId flowId, int expectedEpoch, long leaseExpiration)
    {
        lock (_sync)
        {
            if (!_states.ContainsKey(flowId))
                return false.ToTask();

            var state = _states[flowId];
            if (state.Epoch != expectedEpoch)
                return false.ToTask();

            state.Expires = leaseExpiration;
            return true.ToTask();
        }
    }
    
    public virtual Task<IReadOnlyList<IdAndEpoch>> GetExpiredFunctions(long expiresBefore)
    {
        lock (_sync)
            return _states
                .Values
                .Where(s => s.Status == Status.Executing || s.Status == Status.Postponed)
                .Where(s => s.Expires <= expiresBefore)
                .Select(s => new IdAndEpoch(s.FlowId, s.Epoch))
                .ToList()
                .CastTo<IReadOnlyList<IdAndEpoch>>()
                .ToTask();
    }

    public Task<IReadOnlyList<FlowInstance>> GetSucceededFunctions(FlowType flowType, long completedBefore)
    {
        lock (_sync)
            return _states
                .Values
                .Where(s => s.FlowId.Type == flowType && s.Timestamp < completedBefore)
                .Select(s => s.FlowId.Instance)
                .ToList()
                .CastTo<IReadOnlyList<FlowInstance>>()
                .ToTask();
    }

    public virtual Task<bool> SetFunctionState(
        FlowId flowId,
        Status status,
        string? param,
        string? result,
        StoredException? storedException,
        long expires,
        int expectedEpoch)
    {
        lock (_sync)
        {
            if (!_states.ContainsKey(flowId))
                return false.ToTask();

            var state = _states[flowId];
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
        FlowId flowId, 
        string? result, 
        string? defaultState, 
        long timestamp,
        int expectedEpoch, 
        ComplimentaryState complimentaryState)
    {
        lock (_sync)
        {
            if (!_states.ContainsKey(flowId)) return false.ToTask();

            var state = _states[flowId];
            if (state.Epoch != expectedEpoch) return false.ToTask();

            state.Status = Status.Succeeded;
            state.Result = result;
            state.Timestamp = timestamp;
            state.DefaultState = defaultState;

            return true.ToTask();
        }
    }

    public Task<bool> PostponeFunction(
        FlowId flowId, 
        long postponeUntil, 
        string? defaultState, 
        long timestamp,
        int expectedEpoch, 
        ComplimentaryState complimentaryState)
    {
        lock (_sync)
        {
            if (!_states.ContainsKey(flowId)) return false.ToTask();

            var state = _states[flowId];
            if (state.Epoch != expectedEpoch) return false.ToTask();

            state.Status = Status.Postponed;
            state.Expires = postponeUntil;
            state.Timestamp = timestamp;
            state.DefaultState = defaultState;
            
            return true.ToTask();
        }
    }

    public Task<bool> FailFunction(
        FlowId flowId, 
        StoredException storedException, 
        string? defaultState, 
        long timestamp,
        int expectedEpoch, 
        ComplimentaryState complimentaryState)
    {
        lock (_sync)
        {
            if (!_states.ContainsKey(flowId)) return false.ToTask();

            var state = _states[flowId];
            if (state.Epoch != expectedEpoch) return false.ToTask();

            state.Status = Status.Failed;
            state.Exception = storedException;
            state.Timestamp = timestamp;
            state.DefaultState = defaultState;
            
            return true.ToTask();
        }
    }

    public Task<bool> SuspendFunction(
        FlowId flowId, 
        long expectedInterruptCount, 
        string? defaultState, 
        long timestamp,
        int expectedEpoch, 
        ComplimentaryState complimentaryState)
    {
        lock (_sync)
        {
            if (!_states.ContainsKey(flowId))
                return false.ToTask();

            var state = _states[flowId];
            if (state.Epoch != expectedEpoch)
                return false.ToTask();

            if (state.InterruptCount != expectedInterruptCount)
                return false.ToTask();
                
            state.Status = Status.Suspended;
            state.Timestamp = timestamp;
            state.DefaultState = defaultState;
            
            return true.ToTask();
        }
    }

    public virtual Task<bool> SetParameters(
        FlowId flowId, 
        string? param, 
        string? result, 
        int expectedEpoch)
    {
        lock (_sync)
        {
            if (!_states.ContainsKey(flowId)) return false.ToTask();
            var state = _states[flowId];
            if (state.Epoch != expectedEpoch) return false.ToTask();
            
            state.Param = param;
            state.Result = result;
            
            state.Epoch += 1;

            return true.ToTask();
        }
    }

    public Task SetDefaultState(FlowId flowId, string? stateJson)
    {
        lock (_sync)
            if (_states.TryGetValue(flowId, out var state))
                state.DefaultState = stateJson;

        return Task.CompletedTask;
    }

    public Task<bool> Interrupt(FlowId flowId)
    {
        lock (_sync)
        {
            var success = _states.TryGetValue(flowId, out var state);
            if (!success)
                return false.ToTask();

            if (state!.Status == Status.Postponed || state.Status == Status.Suspended)
            {
                state.Status = Status.Postponed;
                state.Expires = 0;
            }
            
            state.InterruptCount++;
            return true.ToTask();
        }
    }

    public Task<bool> IncrementInterruptCount(FlowId flowId)
    {
        lock (_sync)
        {
            var success = _states.TryGetValue(flowId, out var state);
            if (!success)
                return false.ToTask();
            
            if (state!.Status != Status.Executing)
                return false.ToTask();
            
            state.InterruptCount++;
            return true.ToTask();
        }
    }

    public Task<long?> GetInterruptCount(FlowId flowId)
    {
        lock (_sync) 
            return _states.TryGetValue(flowId, out var state) 
                ? state.InterruptCount.CastTo<long?>().ToTask() 
                : default(long?).ToTask();
    }

    public Task<StatusAndEpoch?> GetFunctionStatus(FlowId flowId)
    {
        lock (_sync)
        {
            if (!_states.ContainsKey(flowId))
                return Task.FromResult(default(StatusAndEpoch));

            var state = _states[flowId];
            return ((StatusAndEpoch?) new StatusAndEpoch(state.Status, state.Epoch)).ToTask();
        }
    }
    
    public virtual Task<StoredFlow?> GetFunction(FlowId flowId)
    {
        lock (_sync)
        {
            if (!_states.ContainsKey(flowId))
                return default(StoredFlow).ToTask();

            var state = _states[flowId];

            return new StoredFlow(
                    flowId,
                    state.Param,
                    state.DefaultState,
                    state.Status,
                    state.Result,
                    state.Exception,
                    state.Epoch,
                    state.Expires,
                    state.Timestamp,
                    state.InterruptCount
                )
                .ToNullable()
                .ToTask();
        }
    }

    public Task<IReadOnlyList<FlowInstance>> GetInstances(FlowType flowType, Status status)
    {
        lock (_sync)
            return _states
                .Where(kv => kv.Key.Type == flowType)
                .Where(kv => kv.Value.Status == status)
                .Select(kv => kv.Key.Instance)
                .ToList()
                .CastTo<IReadOnlyList<FlowInstance>>()
                .ToTask();
    }

    public Task<IReadOnlyList<FlowInstance>> GetInstances(FlowType flowType)
    {
        lock (_sync)
            return _states
                .Where(kv => kv.Key.Type == flowType)
                .Select(kv => kv.Key.Instance)
                .ToList()
                .CastTo<IReadOnlyList<FlowInstance>>()
                .ToTask();
    }

    public Task<IReadOnlyList<FlowType>> GetTypes()
    {
        lock (_sync)
            return _states
                .Select(kv => kv.Key.Type)
                .Distinct()
                .ToList()
                .CastTo<IReadOnlyList<FlowType>>()
                .ToTask();
    }

    public virtual Task<bool> DeleteFunction(FlowId flowId)
    {
        lock (_sync)
        {
            _messages.Remove(flowId);
            _effectsStore.Remove(flowId);
            _timeoutStore.Remove(flowId);
            _correlationStore.RemoveCorrelations(flowId);
            
            return _states.Remove(flowId).ToTask();
        }
    }

    private class InnerState
    {
        public FlowId FlowId { get; init; } = null!;
        public string? Param { get; set; }
        public string? DefaultState { get; set; }
        public Status Status { get; set; }
        public string? Result { get; set; }
        public StoredException? Exception { get; set; }
        public int Epoch { get; set; }
        public long InterruptCount { get; set; }
        public long Expires { get; set; }
        public long Timestamp { get; set; }
    }
    #endregion
    
    #region MessageStore

    public virtual Task<FunctionStatus?> AppendMessage(FlowId flowId, StoredMessage storedMessage)
    {
        lock (_sync)
        {
            if (!_messages.ContainsKey(flowId))
                _messages[flowId] = new List<StoredMessage>();

            var messages = _messages[flowId];
            messages.Add(storedMessage);

            if (!_states.ContainsKey(flowId))
                return Task.FromResult(default(FunctionStatus));
            
            return Task.FromResult((FunctionStatus?)
                new FunctionStatus(_states[flowId].Status, Epoch: _states[flowId].Epoch)
            );
        }
    }
    
    public Task<bool> ReplaceMessage(FlowId flowId, int position, StoredMessage storedMessage)
    {
        lock (_sync)
        {
            if (!_messages.ContainsKey(flowId) || _messages[flowId].Count <= position)
                return false.ToTask();
            
            _messages[flowId][position] = storedMessage;
            return true.ToTask();
        }
    }

    public virtual Task Truncate(FlowId flowId)
    {
        lock (_sync)
            _messages[flowId] = new List<StoredMessage>();

        return Task.CompletedTask;
    }

    private IEnumerable<StoredMessage> GetMessages(FlowId flowId)
    {
        lock (_sync)
            return !_messages.ContainsKey(flowId) 
                ? Enumerable.Empty<StoredMessage>() 
                : _messages[flowId].ToList();
    }

    public virtual Task<IReadOnlyList<StoredMessage>> GetMessages(FlowId flowId, int skip)
        => ((IReadOnlyList<StoredMessage>)GetMessages(flowId).Skip(skip).ToList()).ToTask();
    
    #endregion
}