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
    public IEffectsStore EffectsStore { get; } = new InMemoryEffectsStore();
    public IStatesStore StatesStore { get; } = new InMemoryStatesStore();
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
            state.Result = storedResult;
            state.Exception = storedException;
            state.PostponeUntil = postponeUntil;

            state.Epoch += 1;

            return true.ToTask();
        }
    }

    public virtual Task<bool> SetParameters(
        FunctionId functionId, 
        StoredParameter storedParameter, 
        StoredResult storedResult, 
        int expectedEpoch)
    {
        lock (_sync)
        {
            if (!_states.ContainsKey(functionId)) return false.ToTask();
            var state = _states[functionId];
            if (state.Epoch != expectedEpoch) return false.ToTask();
            
            state.Param = storedParameter;
            state.Result = storedResult;
            
            state.Epoch += 1;

            return true.ToTask();
        }
    }

    public virtual Task<bool> SucceedFunction(
        FunctionId functionId, 
        StoredResult result, 
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

            return true.ToTask();
        }
    }

    public virtual Task<bool> PostponeFunction(
        FunctionId functionId, 
        long postponeUntil, 
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
            
            return true.ToTask();
        }
    }
    
    public virtual Task<bool> SuspendFunction(
        FunctionId functionId, 
        long expectedInterruptCount, 
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
            
            return true.ToTask();
        }
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

    public virtual Task<bool> FailFunction(
        FunctionId functionId, 
        StoredException storedException, 
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
    
    public virtual Task DeleteFunction(FunctionId functionId)
    {
        lock (_sync)
        {
            _states.Remove(functionId);
            _messages.Remove(functionId);
        }

        return Task.CompletedTask;
    }

    private class InnerState
    {
        public FunctionId FunctionId { get; init; } = null!;
        public StoredParameter Param { get; set; } = null!;
        public Status Status { get; set; }
        public StoredResult Result { get; set; } = new StoredResult(ResultJson: null, ResultType: null);
        public StoredException? Exception { get; set; }
        public long? PostponeUntil { get; set; }
        public int Epoch { get; set; }
        public long InterruptCount { get; set; }
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
        => GetMessages(functionId, skip).SelectAsync(msgs => msgs.Any());

    #endregion
}