using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;

namespace Cleipnir.ResilientFunctions.Storage
{
    public class InMemoryFunctionStore : IFunctionStore
    {
        private readonly Dictionary<FunctionId, Param> _params = new();
        private readonly Dictionary<FunctionId, FunctionResult> _results = new();
        private readonly Dictionary<FunctionId, long> _signOfLives = new();
        private readonly HashSet<FunctionId> _nonCompleted = new();
        private readonly object _sync = new();
        
        public Task<bool> StoreFunction(FunctionId functionId, string paramJson, string paramType, long initialSignOfLife)
        {
            lock (_sync)
            {
                if (_params.ContainsKey(functionId))
                    return Task.FromResult(false);

                _params[functionId] = new Param(paramJson, paramType);
                _signOfLives[functionId] = initialSignOfLife;
                _nonCompleted.Add(functionId);
                return Task.FromResult(true);
            }
        }

        public Task<IEnumerable<StoredFunction>> GetNonCompletedFunctions(FunctionTypeId functionTypeId, long olderThan)
        {
            lock (_sync)
            {
                var nonCompleted = _nonCompleted
                    .Select(key =>
                    {
                        var (paramJson, paramType) = _params[key];
                        var signOfLife = _signOfLives[key];
                        return new StoredFunction(
                            new FunctionId(functionTypeId, key.InstanceId),
                            paramJson, paramType,
                            signOfLife
                        );
                    })
                    .Where(s => s.SignOfLife < olderThan)
                    .ToList()
                    .AsEnumerable();

                return Task.FromResult(nonCompleted);
            }
        }

        public Task<bool> UpdateSignOfLife(FunctionId functionId, long expectedSignOfLife, long newSignOfLife)
        {
            lock (_sync)
            {
                if (!_signOfLives.ContainsKey(functionId))
                    return Task.FromResult(false);

                var currentSigOfLife = _signOfLives[functionId];
                if (currentSigOfLife != expectedSignOfLife)
                    return Task.FromResult(false);

                _signOfLives[functionId] = newSignOfLife;
                return Task.FromResult(true);
            }
        }

        public Task StoreFunctionResult(FunctionId functionId, string resultJson, string resultType)
        {
            lock (_sync)
            {
                _results[functionId] = new FunctionResult(resultJson, resultType);
                _nonCompleted.Remove(functionId);
            }

            return Task.CompletedTask;
        }

        public Task<FunctionResult?> GetFunctionResult(FunctionId functionId)
        {
            lock (_sync) 
                return Task.FromResult(
                    _results.ContainsKey(functionId) ? _results[functionId] : null
                );
        }
        private record Param(string ParamJson, string ParamType);
    }
}