using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Messaging;

namespace Cleipnir.ResilientFunctions.Storage;

public class InMemoryFunctionStore : IFunctionStore
{
    private readonly Dictionary<FunctionId, State> _states = new();
    private readonly object _sync = new();

    public IEventStore EventStore { get; } = new InMemoryEventStore();
    public Task Initialize() => Task.CompletedTask;

    public Task<bool> CreateFunction(
        FunctionId functionId, 
        StoredParameter param, 
        StoredScrapbook storedScrapbook,
        long crashedCheckFrequency)
    {
        lock (_sync)
        {
            if (_states.ContainsKey(functionId))
                return false.ToTask();

            _states[functionId] = new State
            {
                FunctionId = functionId,
                Param = param,
                Scrapbook = storedScrapbook,
                Status = Status.Executing,
                Epoch = 0,
                SignOfLife = 0,
                Exception = null,
                Result = new StoredResult(ResultJson: null, ResultType: null),
                PostponeUntil = null,
                CrashedCheckFrequency = crashedCheckFrequency
            };

            return true.ToTask();
        }
    }

    public Task<bool> IncrementEpoch(FunctionId functionId, int expectedEpoch)
    {
        lock (_sync)
        {
            if (!_states.ContainsKey(functionId)) return false.ToTask();
            var state = _states[functionId];
            if (state.Epoch != expectedEpoch)
                return false.ToTask();

            state.Epoch += 1;
            return true.ToTask();
        }
    }

    public Task<bool> RestartExecution(
        FunctionId functionId,
        Tuple<StoredParameter, StoredScrapbook>? paramAndScrapbook, 
        int expectedEpoch,
        long crashedCheckFrequency)
    {
        lock (_sync)
        {
            if (!_states.ContainsKey(functionId))
                return false.ToTask();

            var state = _states[functionId];
            if (state.Epoch != expectedEpoch)
                return false.ToTask();

            if (paramAndScrapbook != null)
            {
                var (param, scrapbook) = paramAndScrapbook;
                state.Param = param;
                state.Scrapbook = scrapbook;   
            }

            state.Epoch += 1;
            state.Status = Status.Executing;
            state.CrashedCheckFrequency = crashedCheckFrequency;
            return true.ToTask();
        }
    }

    public Task<bool> UpdateSignOfLife(FunctionId functionId, int expectedEpoch, int newSignOfLife)
    {
        lock (_sync)
        {
            if (!_states.ContainsKey(functionId))
                return false.ToTask();

            var state = _states[functionId];
            if (state.Epoch != expectedEpoch)
                return false.ToTask();

            state.SignOfLife = newSignOfLife;
            return true.ToTask();
        }
    }

    public Task<IEnumerable<StoredExecutingFunction>> GetExecutingFunctions(FunctionTypeId functionTypeId)
    {
        lock (_sync)
            return _states
                .Values
                .Where(s => s.FunctionId.TypeId == functionTypeId)
                .Where(s => s.Status == Status.Executing)
                .Select(s => new StoredExecutingFunction(s.FunctionId.InstanceId, s.Epoch, s.SignOfLife, s.CrashedCheckFrequency))
                .ToList()
                .AsEnumerable()
                .ToTask();
    }

    public Task<IEnumerable<StoredPostponedFunction>> GetPostponedFunctions(FunctionTypeId functionTypeId, long expiresBefore)
    {
        lock (_sync)
            return _states
                .Values
                .Where(s => s.FunctionId.TypeId == functionTypeId)
                .Where(s => s.Status == Status.Postponed)
                .Where(s => s.PostponeUntil <= expiresBefore)
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

    public Task<bool> SetFunctionState(
        FunctionId functionId, 
        Status status, 
        StoredParameter storedParameter,
        StoredScrapbook storedScrapbook, 
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
            state.Scrapbook = storedScrapbook;
            state.Result = storedResult;
            state.Exception = storedException;
            state.PostponeUntil = postponeUntil;
            state.Epoch += 1;

            return true.ToTask();
        }
    }

    public Task<bool> SaveScrapbookForExecutingFunction(FunctionId functionId, string scrapbookJson, int expectedEpoch)
    {
        lock (_sync)
        {
            if (!_states.ContainsKey(functionId)) return false.ToTask();
            var state = _states[functionId];
            if (state.Epoch != expectedEpoch) return false.ToTask();

            state.Scrapbook = state.Scrapbook with { ScrapbookJson = scrapbookJson };
            return true.ToTask();
        }
    }

    public Task<bool> SetParameters(FunctionId functionId, StoredParameter storedParameter, StoredScrapbook storedScrapbook, int expectedEpoch)
    {
        lock (_sync)
        {
            if (!_states.ContainsKey(functionId)) return false.ToTask();
            var state = _states[functionId];
            if (state.Epoch != expectedEpoch) return false.ToTask();
            
            state.Param = storedParameter;
            state.Scrapbook = storedScrapbook;
            state.Epoch += 1;

            return true.ToTask();
        }
    }

    public Task<bool> SucceedFunction(FunctionId functionId, StoredResult result, string scrapbookJson, int expectedEpoch)
    {
        lock (_sync)
        {
            if (!_states.ContainsKey(functionId)) return false.ToTask();

            var state = _states[functionId];
            if (state.Epoch != expectedEpoch) return false.ToTask();
            
            state.Status = Status.Succeeded;
            state.Result = result;
            state.Scrapbook = state.Scrapbook with { ScrapbookJson = scrapbookJson };
            
            return true.ToTask();
        }
    }

    public Task<bool> PostponeFunction(FunctionId functionId, long postponeUntil, string scrapbookJson, int expectedEpoch)
    {
        lock (_sync)
        {
            if (!_states.ContainsKey(functionId)) return false.ToTask();

            var state = _states[functionId];
            if (state.Epoch != expectedEpoch) return false.ToTask();
            
            state.Status = Status.Postponed;
            state.PostponeUntil = postponeUntil;
            state.Scrapbook = state.Scrapbook with { ScrapbookJson = scrapbookJson };
            
            return true.ToTask();
        }
    }
    
    public Task<bool> SuspendFunction(FunctionId functionId, int suspendUntilEventSourceCountAtLeast, string scrapbookJson, int expectedEpoch)
    {
        lock (_sync)
        {
            if (!_states.ContainsKey(functionId)) return false.ToTask();

            var state = _states[functionId];
            if (state.Epoch != expectedEpoch) return false.ToTask();
            
            state.Status = Status.Suspended;
            state.SuspendUntilEventSourceCountAtLeast = suspendUntilEventSourceCountAtLeast;
            state.Scrapbook = state.Scrapbook with { ScrapbookJson = scrapbookJson };
            
            return true.ToTask();
        }
    }

    public Task<bool> FailFunction(FunctionId functionId, StoredException storedException, string scrapbookJson, int expectedEpoch)
    {
        lock (_sync)
        {
            if (!_states.ContainsKey(functionId)) return false.ToTask();

            var state = _states[functionId];
            if (state.Epoch != expectedEpoch) return false.ToTask();
            
            state.Status = Status.Failed;
            state.Exception = storedException;
            state.Scrapbook = state.Scrapbook with { ScrapbookJson = scrapbookJson };
            
            return true.ToTask();
        }
    }

    public Task<StoredFunction?> GetFunction(FunctionId functionId)
    {
        lock (_sync)
        {
            if (!_states.ContainsKey(functionId))
                return default(StoredFunction).ToTask();

            var state = _states[functionId];

            return new StoredFunction(
                    functionId,
                    state.Param,
                    state.Scrapbook,
                    state.Status,
                    state.Result,
                    state.Exception,
                    state.PostponeUntil,
                    state.SuspendUntilEventSourceCountAtLeast,
                    state.Epoch,
                    state.SignOfLife,
                    state.CrashedCheckFrequency
                )
                .ToNullable()
                .ToTask();
        }
    }

    public Task<StoredFunctionStatus?> GetFunctionStatus(FunctionId functionId)
    {
        lock (_sync)
        {
            if (!_states.ContainsKey(functionId))
                return default(StoredFunctionStatus).ToTask();

            var state = _states[functionId];
            return new StoredFunctionStatus(functionId, state.Status, state.Epoch)
                .CastTo<StoredFunctionStatus?>()
                .ToTask();
        }
    }

    public Task<bool> DeleteFunction(FunctionId functionId, int? expectedEpoch = null, Status? expectedStatus = null)
    {
        lock (_sync)
        {
            if (!_states.ContainsKey(functionId))
                return false.ToTask();
            
            if (expectedEpoch == null && expectedStatus == null)
            {
                _states.Remove(functionId);
                return true.ToTask();
            }
            var state = _states[functionId];
            if (expectedEpoch != null && state.Epoch == expectedEpoch.Value && 
                expectedStatus != null && state.Status == expectedStatus.Value)
            {
                _states.Remove(functionId);
                return true.ToTask();
            }

            if (expectedEpoch != null && state.Epoch == expectedEpoch.Value && expectedStatus == null)
            {
                _states.Remove(functionId);
                return true.ToTask();
            }

            if (expectedStatus != null && state.Status == expectedStatus.Value && expectedEpoch == null)
            {
                _states.Remove(functionId);
                return true.ToTask();
            }
            
            return false.ToTask();
        }
    }

    private class State
    {
        public FunctionId FunctionId { get; init; } = null!;
        public StoredParameter Param { get; set; } = null!;
        public StoredScrapbook Scrapbook { get; set; } = null!;
        public Status Status { get; set; }
        public StoredResult Result { get; set; } = new StoredResult(ResultJson: null, ResultType: null);
        public StoredException? Exception { get; set; }
        public long? PostponeUntil { get; set; }
        public int Epoch { get; set; }
        public int SignOfLife { get; set; }
        public long CrashedCheckFrequency { get; set; }
        public int SuspendUntilEventSourceCountAtLeast { get; set; }
    }
}