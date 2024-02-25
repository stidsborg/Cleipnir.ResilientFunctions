using System;
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
    private readonly Dictionary<FunctionId, InnerState> _states = new();
    private readonly Dictionary<FunctionId, List<StoredMessage>> _messages = new();
    private readonly object _sync = new();

    public IMessageStore MessageStore => this;
    public IActivityStore ActivityStore { get; } = new InMemoryActivityStore();
    public ITimeoutStore TimeoutStore { get; } = new InMemoryTimeoutStore();
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
        StoredParameter param,
        StoredState storedState,
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
                State = storedState,
                Status = postponeUntil == null ? Status.Executing : Status.Postponed,
                Epoch = 0,
                Exception = null,
                Result = new StoredResult(ResultJson: null, ResultType: null),
                PostponeUntil = postponeUntil,
                LeaseExpiration = leaseExpiration,
                Timestamp = timestamp
            };
            _messages[functionId] = new List<StoredMessage>();

            return true.ToTask();
        }
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

    public virtual Task<IEnumerable<StoredExecutingFunction>> GetCrashedFunctions(FunctionTypeId functionTypeId, long leaseExpiresBefore)
    {
        lock (_sync)
            return _states
                .Values
                .Where(s => s.FunctionId.TypeId == functionTypeId)
                .Where(s => s.Status == Status.Executing)
                .Where(s => s.LeaseExpiration < leaseExpiresBefore)
                .Select(s => new StoredExecutingFunction(s.FunctionId.InstanceId, s.Epoch, s.LeaseExpiration))
                .ToList()
                .AsEnumerable()
                .ToTask();
    }

    public virtual Task<IEnumerable<StoredPostponedFunction>> GetPostponedFunctions(FunctionTypeId functionTypeId, long isEligibleBefore)
    {
        lock (_sync)
            return _states
                .Values
                .Where(s => s.FunctionId.TypeId == functionTypeId)
                .Where(s => s.Status == Status.Postponed)
                .Where(s => s.PostponeUntil <= isEligibleBefore)
                .Select(s =>
                    new StoredPostponedFunction(
                        s.FunctionId.InstanceId,
                        s.Epoch,
                        s.PostponeUntil!.Value
                    )
                )
                .ToList()
                .AsEnumerable()
                .ToTask();
    }
    
    public virtual Task<bool> SetFunctionState(
        FunctionId functionId,
        Status status,
        StoredParameter storedParameter,
        StoredState storedState,
        StoredResult storedResult,
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
            state.Param = storedParameter;
            state.State = storedState;
            state.Result = storedResult;
            state.Exception = storedException;
            state.PostponeUntil = postponeUntil;

            state.Epoch += 1;

            return true.ToTask();
        }
    }

    public virtual Task<bool> SaveStateForExecutingFunction( 
        FunctionId functionId,
        string stateJson,
        int expectedEpoch,
        ComplimentaryState _)
    {
        lock (_sync)
        {
            if (!_states.ContainsKey(functionId)) return false.ToTask();
            var state = _states[functionId];
            if (state.Epoch != expectedEpoch) return false.ToTask();

            state.State = state.State with { StateJson = stateJson };
            return true.ToTask();
        }
    }

    public virtual Task<bool> SetParameters(
        FunctionId functionId, 
        StoredParameter storedParameter, 
        StoredState storedState, 
        StoredResult storedResult, 
        int expectedEpoch)
    {
        lock (_sync)
        {
            if (!_states.ContainsKey(functionId)) return false.ToTask();
            var state = _states[functionId];
            if (state.Epoch != expectedEpoch) return false.ToTask();
            
            state.Param = storedParameter;
            state.State = storedState;
            state.Result = storedResult;
            
            state.Epoch += 1;

            return true.ToTask();
        }
    }

    public virtual Task<bool> SucceedFunction(
        FunctionId functionId, 
        StoredResult result, 
        string stateJson,
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
            state.State = state.State with { StateJson = stateJson };
            state.Timestamp = timestamp;

            return true.ToTask();
        }
    }

    public virtual Task<bool> PostponeFunction(
        FunctionId functionId, 
        long postponeUntil, 
        string stateJson,
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
            state.State = state.State with { StateJson = stateJson };
            state.Timestamp = timestamp;
            
            return true.ToTask();
        }
    }
    
    public virtual Task<bool> SuspendFunction(
        FunctionId functionId, 
        int expectedMessageCount, 
        string stateJson,
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

            if (_messages[functionId].Count > expectedMessageCount)
                return PostponeFunction(functionId, postponeUntil: 0, stateJson, timestamp, expectedEpoch, complimentaryState);
                
            state.Status = Status.Suspended;
            state.State = state.State with { StateJson = stateJson };
            state.Timestamp = timestamp;
            
            return true.ToTask();
        }
    }

    public Task IncrementSignalCount(FunctionId functionId)
    {
        lock (_sync)
        {
            var success = _states.TryGetValue(functionId, out var state);
            if (success)
                state!.SignalCount++;
        }

        return Task.CompletedTask;
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

    public virtual Task<bool> FailFunction(
        FunctionId functionId, 
        StoredException storedException, 
        string stateJson,
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
            state.State = state.State with { StateJson = stateJson };
            state.Timestamp = timestamp;
            
            return true.ToTask();
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
                    state.State,
                    state.Status,
                    state.Result,
                    state.Exception,
                    state.PostponeUntil,
                    state.Epoch,
                    state.LeaseExpiration,
                    state.Timestamp,
                    state.SignalCount
                )
                .ToNullable()
                .ToTask();
        }
    }
    
    public virtual Task<bool> DeleteFunction(FunctionId functionId, int? expectedEpoch = null)
    {
        lock (_sync)
        {
            if (!_states.ContainsKey(functionId))
                return false.ToTask();
            
            if (expectedEpoch == null)
            {
                _states.Remove(functionId);
                _messages.Remove(functionId);
                return true.ToTask();
            }
            
            var state = _states[functionId];
            if (state.Epoch == expectedEpoch.Value)
            {
                _states.Remove(functionId);
                _messages.Remove(functionId);
                return true.ToTask();
            }
            
            return false.ToTask();
        }
    }

    private class InnerState
    {
        public FunctionId FunctionId { get; init; } = null!;
        public StoredParameter Param { get; set; } = null!;
        public StoredState State { get; set; } = null!;
        public Status Status { get; set; }
        public StoredResult Result { get; set; } = new StoredResult(ResultJson: null, ResultType: null);
        public StoredException? Exception { get; set; }
        public long? PostponeUntil { get; set; }
        public int Epoch { get; set; }
        public long SignalCount { get; set; }
        public long LeaseExpiration { get; set; }
        public long Timestamp { get; set; }
    }
    #endregion
    
    #region MessageStore

    public virtual Task<FunctionStatus> AppendMessage(FunctionId functionId, StoredMessage storedMessage)
    {
        lock (_sync)
        {
            if (!_messages.ContainsKey(functionId))
                _messages[functionId] = new List<StoredMessage>();

            var messages = _messages[functionId];
            messages.Add(storedMessage);
            
            return Task.FromResult(
                new FunctionStatus(_states[functionId].Status, Epoch: _states[functionId].Epoch)
            );
        }
    }

    public virtual Task<FunctionStatus> AppendMessage(FunctionId functionId, string messageJson, string messageType, string? idempotencyKey = null)
        => AppendMessage(functionId, new StoredMessage(messageJson, messageType, idempotencyKey));

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

    public virtual Task<IEnumerable<StoredMessage>> GetMessages(FunctionId functionId)
    {
        lock (_sync)
        {
            if (!_messages.ContainsKey(functionId))
                return Enumerable.Empty<StoredMessage>().ToTask();

            return _messages[functionId].ToList().AsEnumerable().ToTask();
        }
    }

    public virtual MessagesSubscription SubscribeToMessages(FunctionId functionId)
    {
        var disposed = false;
        var skip = 0;

        var subscription = new MessagesSubscription(
            pullNewMessages: () =>
            {
                List<StoredMessage>? messages;

                lock (_sync)
                    if (disposed)
                        return Task.FromResult((IReadOnlyList<StoredMessage>) Array.Empty<StoredMessage>());
                    else if (_messages.ContainsKey(functionId) && _messages[functionId].Count > skip)
                    {
                        messages = _messages[functionId].Skip(skip).ToList();
                        skip += messages.Count;
                    }
                    else
                        messages = new List<StoredMessage>();

                return Task.FromResult((IReadOnlyList<StoredMessage>) messages);
            },
            dispose: () =>
            {
                lock (_sync)
                    disposed = true;
                
                return ValueTask.CompletedTask;
            }
        );

        return subscription;
    }

    #endregion
}