using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;

namespace Cleipnir.ResilientFunctions.Storage;

public class InMemoryFunctionStore : IFunctionStore
{
    private readonly Dictionary<FunctionId, State> _states = new();
    private readonly object _sync = new();

    public Task Initialize() => Task.CompletedTask;

    public Task<bool> CreateFunction(
        FunctionId functionId, 
        StoredParameter param, 
        StoredScrapbook storedScrapbook,
        long crashedCheckFrequency, 
        int version)
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
                ErrorJson = null,
                Result = null,
                PostponeUntil = null,
                CrashedCheckFrequency = crashedCheckFrequency,
                Version = version
            };

            return true.ToTask();
        }
    }

    public Task<bool> TryToBecomeLeader(FunctionId functionId, Status newStatus, int expectedEpoch, int newEpoch, long crashedCheckFrequency, int version)
    {
        lock (_sync)
        {
            if (!_states.ContainsKey(functionId))
                return false.ToTask();

            var state = _states[functionId];
            if (state.Epoch != expectedEpoch)
                return false.ToTask();

            state.Epoch = newEpoch;
            state.Status = newStatus;
            state.CrashedCheckFrequency = crashedCheckFrequency;
            state.Version = version;
            return true.ToTask();
        }
    }

    public Task<bool> TryToBecomeLeader(FunctionId functionId, Status newStatus, int expectedEpoch, int newEpoch, long crashedCheckFrequency, int version, string scrapbookJson)
    {
        lock (_sync)
        {
            if (!_states.ContainsKey(functionId))
                return false.ToTask();

            var state = _states[functionId];
            if (state.Epoch != expectedEpoch)
                return false.ToTask();

            state.Epoch = newEpoch;
            state.Status = newStatus;
            state.CrashedCheckFrequency = crashedCheckFrequency;
            state.Version = version;
            state.Scrapbook =  new StoredScrapbook(scrapbookJson, state.Scrapbook.ScrapbookType!);
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

    public Task<IEnumerable<StoredExecutingFunction>> GetExecutingFunctions(FunctionTypeId functionTypeId, int versionUpperBound)
    {
        lock (_sync)
            return _states
                .Values
                .Where(s => s.FunctionId.TypeId == functionTypeId)
                .Where(s => s.Status == Status.Executing)
                .Where(s => s.Version <= versionUpperBound)
                .Select(s => new StoredExecutingFunction(s.FunctionId.InstanceId, s.Epoch, s.SignOfLife, s.CrashedCheckFrequency))
                .ToList()
                .AsEnumerable()
                .ToTask();
    }

    public Task<IEnumerable<StoredPostponedFunction>> GetPostponedFunctions(FunctionTypeId functionTypeId, long expiresBefore, int versionUpperBound)
    {
        lock (_sync)
            return _states
                .Values
                .Where(s => s.FunctionId.TypeId == functionTypeId)
                .Where(s => s.Status == Status.Postponed)
                .Where(s => s.Version <= versionUpperBound)
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
        string scrapbookJson, 
        StoredResult? result, 
        string? errorJson,
        long? postponedUntil, 
        int expectedEpoch
    )
    {
        lock (_sync)
        {
            if (!_states.ContainsKey(functionId))
                return false.ToTask();

            var state = _states[functionId];
            if (state.Epoch != expectedEpoch)
                return false.ToTask();

            state.Status = status;
            state.Scrapbook = state.Scrapbook with { ScrapbookJson = scrapbookJson };

            state.Result = result;
            state.ErrorJson = errorJson;
            state.PostponeUntil = postponedUntil;

            return true.ToTask();
        }
    }

    public Task<bool> SetScrapbook(FunctionId functionId, string scrapbookJson, int expectedEpoch)
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

    public Task<bool> SetParameters(FunctionId functionId, StoredParameter? storedParameter, StoredScrapbook? storedScrapbook, int expectedEpoch)
    {
        lock (_sync)
        {
            if (!_states.ContainsKey(functionId)) return false.ToTask();
            var state = _states[functionId];
            if (state.Epoch != expectedEpoch) return false.ToTask();

            if (storedParameter != null)
                state.Param = storedParameter;

            if (storedScrapbook != null)
                state.Scrapbook = storedScrapbook;

            return true.ToTask();
        }
    }

    public Task<bool> FailFunction(FunctionId functionId, string errorJson, string scrapbookJson, int expectedEpoch)
    {
        lock (_sync)
        {
            if (!_states.ContainsKey(functionId)) return false.ToTask();

            var state = _states[functionId];
            if (state.Epoch != expectedEpoch) return false.ToTask();
            
            state.Status = Status.Failed;
            state.ErrorJson = errorJson;
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
                    state.ErrorJson,
                    state.PostponeUntil,
                    state.Version,
                    state.Epoch,
                    state.SignOfLife,
                    state.CrashedCheckFrequency
                )
                .ToNullable()
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
        public StoredResult? Result { get; set; }
        public string? ErrorJson { get; set; }
        public long? PostponeUntil { get; set; }
        public int Epoch { get; set; }
        public int SignOfLife { get; set; }
        public long CrashedCheckFrequency { get; set; }
        public int Version { get; set; }
    }
}