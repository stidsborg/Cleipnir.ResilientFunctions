﻿using System.Collections.Generic;
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
    private readonly Dictionary<FunctionId, InnerState> _states = new();
    private readonly Dictionary<FunctionId, List<StoredMessage>> _messages = new();
    private readonly object _sync = new();

    public IMessageStore MessageStore => this;
    private readonly InMemoryEffectsStore _effectsStore = new();
    public IEffectsStore EffectsStore => _effectsStore;
    private readonly InMemoryStatesStore _statesStore = new();
    public IStatesStore StatesStore => _statesStore;
    private readonly InMemoryTimeoutStore _timeoutStore = new();
    public ITimeoutStore TimeoutStore => _timeoutStore;
    private readonly InMemoryCorrelationStore _correlationStore = new();
    public ICorrelationStore CorrelationStore => _correlationStore;
    public Utilities Utilities { get; }
    
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
        FunctionId functionId,
        string? param,
        long leaseExpiration,
        long? postponeUntil,
        long timestamp)
    {
        lock (_sync)
        {
            if (_states.ContainsKey(functionId))
                return false.ToTask();

            _states[functionId] = new InnerState
            {
                FunctionId = functionId,
                Param = param,
                Status = postponeUntil == null ? Status.Executing : Status.Postponed,
                Epoch = 0,
                Exception = null,
                Result = null,
                PostponeUntil = postponeUntil,
                LeaseExpiration = leaseExpiration,
                Timestamp = timestamp
            };
            if (!_messages.ContainsKey(functionId)) //messages can already have been added - i.e. paramless started by received message
                _messages[functionId] = new List<StoredMessage>();

            return true.ToTask();
        }
    }

    public Task BulkScheduleFunctions(IEnumerable<FunctionIdWithParam> functionsWithParam)
    {
        lock (_sync)
        {
            foreach (var (functionId, param) in functionsWithParam)
            {
                if (!_states.ContainsKey(functionId))
                    _states[functionId] = new InnerState
                    {
                        FunctionId = functionId,
                        DefaultState = null,
                        Epoch = 0,
                        Exception = null,
                        InterruptCount = 0,
                        LeaseExpiration = 0,
                        Param = param,
                        PostponeUntil = 0,
                        Result = null,
                        Status = Status.Postponed
                    };
            }
        }

        return Task.CompletedTask;
    }

    public virtual Task<StoredFunction?> RestartExecution(FunctionId functionId, int expectedEpoch, long leaseExpiration)
    {
        lock (_sync)
        {
            if (!_states.ContainsKey(functionId))
                return default(StoredFunction).ToTask();

            var state = _states[functionId];
            if (state.Epoch != expectedEpoch)
                return default(StoredFunction).ToTask();

            state.Epoch += 1;
            state.Status = Status.Executing;
            state.LeaseExpiration = leaseExpiration;
            return GetFunction(functionId);
        }
    }

    public virtual Task<bool> RenewLease(FunctionId functionId, int expectedEpoch, long leaseExpiration)
    {
        lock (_sync)
        {
            if (!_states.ContainsKey(functionId))
                return false.ToTask();

            var state = _states[functionId];
            if (state.Epoch != expectedEpoch)
                return false.ToTask();

            state.LeaseExpiration = leaseExpiration;
            return true.ToTask();
        }
    }

    public virtual Task<IReadOnlyList<InstanceIdAndEpoch>> GetCrashedFunctions(FunctionTypeId functionTypeId, long leaseExpiresBefore)
    {
        lock (_sync)
            return _states
                .Values
                .Where(s => s.FunctionId.TypeId == functionTypeId)
                .Where(s => s.Status == Status.Executing)
                .Where(s => s.LeaseExpiration < leaseExpiresBefore)
                .Select(s => new InstanceIdAndEpoch(s.FunctionId.InstanceId, s.Epoch))
                .ToList()
                .CastTo<IReadOnlyList<InstanceIdAndEpoch>>()
                .ToTask();
    }

    public virtual Task<IReadOnlyList<InstanceIdAndEpoch>> GetPostponedFunctions(FunctionTypeId functionTypeId, long isEligibleBefore)
    {
        lock (_sync)
            return _states
                .Values
                .Where(s => s.FunctionId.TypeId == functionTypeId)
                .Where(s => s.Status == Status.Postponed)
                .Where(s => s.PostponeUntil <= isEligibleBefore)
                .Select(s => new InstanceIdAndEpoch(s.FunctionId.InstanceId, s.Epoch))
                .ToList()
                .CastTo<IReadOnlyList<InstanceIdAndEpoch>>()
                .ToTask();
    }

    public Task<IReadOnlyList<FunctionInstanceId>> GetSucceededFunctions(FunctionTypeId functionTypeId, long completedBefore)
    {
        lock (_sync)
            return _states
                .Values
                .Where(s => s.FunctionId.TypeId == functionTypeId && s.Timestamp < completedBefore)
                .Select(s => s.FunctionId.InstanceId)
                .ToList()
                .CastTo<IReadOnlyList<FunctionInstanceId>>()
                .ToTask();
    }

    public virtual Task<bool> SetFunctionState(
        FunctionId functionId,
        Status status,
        string? param,
        string? result,
        StoredException? storedException,
        long? postponeUntil,
        int expectedEpoch)
    {
        lock (_sync)
        {
            if (!_states.ContainsKey(functionId))
                return false.ToTask();

            var state = _states[functionId];
            if (state.Epoch != expectedEpoch)
                return false.ToTask();

            state.Status = status;
            state.Param = param;
            state.Result = result;
            state.Exception = storedException;
            state.PostponeUntil = postponeUntil;

            state.Epoch += 1;

            return true.ToTask();
        }
    }

    public Task<bool> SucceedFunction(
        FunctionId functionId, 
        string? result, 
        string? defaultState, 
        long timestamp,
        int expectedEpoch, 
        ComplimentaryState complimentaryState)
    {
        lock (_sync)
        {
            if (!_states.ContainsKey(functionId)) return false.ToTask();

            var state = _states[functionId];
            if (state.Epoch != expectedEpoch) return false.ToTask();

            state.Status = Status.Succeeded;
            state.Result = result;
            state.Timestamp = timestamp;
            state.DefaultState = defaultState;

            return true.ToTask();
        }
    }

    public Task<bool> PostponeFunction(
        FunctionId functionId, 
        long postponeUntil, 
        string? defaultState, 
        long timestamp,
        int expectedEpoch, 
        ComplimentaryState complimentaryState)
    {
        lock (_sync)
        {
            if (!_states.ContainsKey(functionId)) return false.ToTask();

            var state = _states[functionId];
            if (state.Epoch != expectedEpoch) return false.ToTask();

            state.Status = Status.Postponed;
            state.PostponeUntil = postponeUntil;
            state.Timestamp = timestamp;
            state.DefaultState = defaultState;
            
            return true.ToTask();
        }
    }

    public Task<bool> FailFunction(
        FunctionId functionId, 
        StoredException storedException, 
        string? defaultState, 
        long timestamp,
        int expectedEpoch, 
        ComplimentaryState complimentaryState)
    {
        lock (_sync)
        {
            if (!_states.ContainsKey(functionId)) return false.ToTask();

            var state = _states[functionId];
            if (state.Epoch != expectedEpoch) return false.ToTask();

            state.Status = Status.Failed;
            state.Exception = storedException;
            state.Timestamp = timestamp;
            state.DefaultState = defaultState;
            
            return true.ToTask();
        }
    }

    public Task<bool> SuspendFunction(
        FunctionId functionId, 
        long expectedInterruptCount, 
        string? defaultState, 
        long timestamp,
        int expectedEpoch, 
        ComplimentaryState complimentaryState)
    {
        lock (_sync)
        {
            if (!_states.ContainsKey(functionId))
                return false.ToTask();

            var state = _states[functionId];
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
        FunctionId functionId, 
        string? param, 
        string? result, 
        int expectedEpoch)
    {
        lock (_sync)
        {
            if (!_states.ContainsKey(functionId)) return false.ToTask();
            var state = _states[functionId];
            if (state.Epoch != expectedEpoch) return false.ToTask();
            
            state.Param = param;
            state.Result = result;
            
            state.Epoch += 1;

            return true.ToTask();
        }
    }

    public Task SetDefaultState(FunctionId functionId, string? stateJson)
    {
        lock (_sync)
            if (_states.TryGetValue(functionId, out var state))
                state.DefaultState = stateJson;

        return Task.CompletedTask;
    }

    public Task<bool> IncrementInterruptCount(FunctionId functionId)
    {
        lock (_sync)
        {
            var success = _states.TryGetValue(functionId, out var state);
            if (!success)
                return false.ToTask();
            
            if (state!.Status != Status.Executing)
                return false.ToTask();
            
            state.InterruptCount++;
            return true.ToTask();
        }
    }

    public Task<long?> GetInterruptCount(FunctionId functionId)
    {
        lock (_sync) 
            return _states.TryGetValue(functionId, out var state) 
                ? state.InterruptCount.CastTo<long?>().ToTask() 
                : default(long?).ToTask();
    }

    public Task<StatusAndEpoch?> GetFunctionStatus(FunctionId functionId)
    {
        lock (_sync)
        {
            if (!_states.ContainsKey(functionId))
                return Task.FromResult(default(StatusAndEpoch));

            var state = _states[functionId];
            return ((StatusAndEpoch?) new StatusAndEpoch(state.Status, state.Epoch)).ToTask();
        }
    }
    
    public virtual Task<StoredFunction?> GetFunction(FunctionId functionId)
    {
        lock (_sync)
        {
            if (!_states.ContainsKey(functionId))
                return default(StoredFunction).ToTask();

            var state = _states[functionId];

            return new StoredFunction(
                    functionId,
                    state.Param,
                    state.DefaultState,
                    state.Status,
                    state.Result,
                    state.Exception,
                    state.PostponeUntil,
                    state.Epoch,
                    state.LeaseExpiration,
                    state.Timestamp,
                    state.InterruptCount
                )
                .ToNullable()
                .ToTask();
        }
    }
    
    public virtual Task<bool> DeleteFunction(FunctionId functionId)
    {
        lock (_sync)
        {
            _messages.Remove(functionId);
            _effectsStore.Remove(functionId);
            _statesStore.Remove(functionId);
            _timeoutStore.Remove(functionId);
            _correlationStore.RemoveCorrelations(functionId);
            
            return _states.Remove(functionId).ToTask();
        }
    }

    private class InnerState
    {
        public FunctionId FunctionId { get; init; } = null!;
        public string? Param { get; set; }
        public string? DefaultState { get; set; }
        public Status Status { get; set; }
        public string? Result { get; set; }
        public StoredException? Exception { get; set; }
        public long? PostponeUntil { get; set; }
        public int Epoch { get; set; }
        public long InterruptCount { get; set; }
        public long LeaseExpiration { get; set; }
        public long Timestamp { get; set; }
    }
    #endregion
    
    #region MessageStore

    public virtual Task<FunctionStatus?> AppendMessage(FunctionId functionId, StoredMessage storedMessage)
    {
        lock (_sync)
        {
            if (!_messages.ContainsKey(functionId))
                _messages[functionId] = new List<StoredMessage>();

            var messages = _messages[functionId];
            messages.Add(storedMessage);

            if (!_states.ContainsKey(functionId))
                return Task.FromResult(default(FunctionStatus));
            
            return Task.FromResult((FunctionStatus?)
                new FunctionStatus(_states[functionId].Status, Epoch: _states[functionId].Epoch)
            );
        }
    }
    
    public Task<bool> ReplaceMessage(FunctionId functionId, int position, StoredMessage storedMessage)
    {
        lock (_sync)
        {
            if (!_messages.ContainsKey(functionId) || _messages[functionId].Count <= position)
                return false.ToTask();
            
            _messages[functionId][position] = storedMessage;
            return true.ToTask();
        }
    }

    public virtual Task Truncate(FunctionId functionId)
    {
        lock (_sync)
            _messages[functionId] = new List<StoredMessage>();

        return Task.CompletedTask;
    }

    private IEnumerable<StoredMessage> GetMessages(FunctionId functionId)
    {
        lock (_sync)
            return !_messages.ContainsKey(functionId) 
                ? Enumerable.Empty<StoredMessage>() 
                : _messages[functionId].ToList();
    }

    public virtual Task<IReadOnlyList<StoredMessage>> GetMessages(FunctionId functionId, int skip)
        => ((IReadOnlyList<StoredMessage>)GetMessages(functionId).Skip(skip).ToList()).ToTask();

    public Task<bool> HasMoreMessages(FunctionId functionId, int skip)
        => GetMessages(functionId, skip).SelectAsync(messages => messages.Any());

    #endregion
}