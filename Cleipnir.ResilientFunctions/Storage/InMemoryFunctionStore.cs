using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Utils;

namespace Cleipnir.ResilientFunctions.Storage
{
    public class InMemoryFunctionStore : IFunctionStore
    {
        private readonly Dictionary<FunctionId, State> _states = new();
        private readonly object _sync = new();

        private class State
        {
            public Parameter Parameter1 { get; init; } = new("", "");
            public Parameter? Parameter2 { get; set; }
            public Result? Result { get; set; }
            public long SignOfLife { get; set; }
            public Scrapbook? Scrapbook { get; set; }
        }

        public Task<bool> StoreFunction(
            FunctionId functionId, 
            Parameter param1,
            Parameter? param2,
            string? scrapbookType, 
            long initialSignOfLife)
        {
            lock (_sync)
            {
                if (_states.ContainsKey(functionId))
                    return Task.FromResult(false);

                _states[functionId] = new State
                {
                    SignOfLife = initialSignOfLife,
                    Parameter1 = param1,
                    Parameter2 = param2,
                    Scrapbook = scrapbookType != null
                        ? new Scrapbook(null, scrapbookType, 0)
                        : null,
                    Result = null
                };
                
                return Task.FromResult(true);
            }
        }

        public Task<bool> UpdateScrapbook(
            FunctionId functionId, 
            string scrapbookJson,
            int expectedVersionStamp, 
            int newVersionStamp
        )
        {
            lock (_sync)
            {
                var state = _states[functionId];
                var prevScrapbook = state.Scrapbook!;

                if (prevScrapbook.VersionStamp != expectedVersionStamp)
                    return false.ToTask();

                state.Scrapbook = new Scrapbook(scrapbookJson, prevScrapbook.ScrapbookType, newVersionStamp);
                return true.ToTask();
            }
        }

        public Task<IEnumerable<NonCompletedFunction>> GetNonCompletedFunctions(FunctionTypeId functionTypeId)
        {
            lock (_sync)
            {
                return _states
                    .Where(kv =>
                    {
                        var functionId = kv.Key;
                        var state = kv.Value;
                        return state.Result == null && functionId.TypeId == functionTypeId;
                    })
                    .Select(kv =>
                    {
                        var (functionId, state) = kv;
                        return new NonCompletedFunction(functionId.InstanceId, state.SignOfLife);
                    })
                    .ToList()
                    .AsEnumerable()
                    .ToTask();
            }
        }

        public Task<bool> UpdateSignOfLife(FunctionId functionId, long expectedSignOfLife, long newSignOfLife)
        {
            lock (_sync)
            {
                if (_states[functionId].SignOfLife != expectedSignOfLife)
                    return false.ToTask();

                _states[functionId].SignOfLife = newSignOfLife;
                return true.ToTask();
            }
        }

        public Task StoreFunctionResult(FunctionId functionId, string resultJson, string resultType)
        {
            lock (_sync)
                _states[functionId].Result = new Result(resultJson, resultType);

            return Task.CompletedTask;
        }

        public Task<Result?> GetFunctionResult(FunctionId functionId)
        {
            lock (_sync)
                return _states.ContainsKey(functionId)
                    ? _states[functionId].Result.ToTask()
                    : default(Result?).ToTask();
        }

        public Task<StoredFunction?> GetFunction(FunctionId functionId)
        {
            lock (_sync)
            {
                if (!_states.ContainsKey(functionId))
                    return default(StoredFunction?).ToTask();
                
                var state = _states[functionId];
                var sf = new StoredFunction(
                    functionId,
                    state.Parameter1,
                    state.Parameter2,
                    state.Scrapbook,
                    state.SignOfLife,
                    state.Result
                );
                return sf.ToNullable().ToTask();
            }
        }
    }
}