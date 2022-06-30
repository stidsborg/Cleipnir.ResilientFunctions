using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;

namespace Cleipnir.ResilientFunctions.Storage;


// TODO: Move to stores
public class InMemoryFunctionStore : IFunctionStore
{
    private readonly Dictionary<FunctionId, State> _states = new();
    private readonly object _sync = new();

    public Task Initialize() => Task.CompletedTask;

    public Task<bool> CreateFunction(
        FunctionId functionId, 
        StoredParameter param,
        string? scrapbookType,
        Status initialStatus, 
        int initialEpoch, 
        int initialSignOfLife
    )
    {
        lock (_sync)
        {
            if (_states.ContainsKey(functionId))
                return false.ToTask();

            _states[functionId] = new State
            {
                FunctionId = functionId,
                Param = param,
                Scrapbook = scrapbookType == null ? null : new StoredScrapbook(ScrapbookJson: null, scrapbookType),
                Status = Status.Executing,
                Epoch = initialEpoch,
                SignOfLife = initialSignOfLife,
                ErrorJson = null,
                Result = null,
                PostponeUntil = null
            };

            return true.ToTask();
        }
    }

    public Task<bool> TryToBecomeLeader(FunctionId functionId, Status newStatus, int expectedEpoch, int newEpoch)
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
                .Select(s => new StoredExecutingFunction(s.FunctionId.InstanceId, s.Epoch, s.SignOfLife))
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
        string? scrapbookJson, 
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
            state.Scrapbook = scrapbookJson == null
                ? null
                : new StoredScrapbook(scrapbookJson, state.Scrapbook!.ScrapbookType);

            state.Result = result;
            state.ErrorJson = errorJson;
            state.PostponeUntil = postponedUntil;

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
                    state.Epoch,
                    state.SignOfLife
                )
                .ToNullable()
                .ToTask();
        }
    }
    
    private class State
    {
        public FunctionId FunctionId { get; init; } = new FunctionId("", "");
        public StoredParameter Param { get; init; } = new StoredParameter("", "");
        public StoredScrapbook? Scrapbook { get; set; }
        public Status Status { get; set; }
        public StoredResult? Result { get; set; }
        public string? ErrorJson { get; set; }
        public long? PostponeUntil { get; set; }
        public int Epoch { get; set; }
        public int SignOfLife { get; set; }
    }
}