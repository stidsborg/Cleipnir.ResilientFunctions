using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;

namespace Cleipnir.ResilientFunctions.Storage;

public class InMemoryCorrelationStore : ICorrelationStore
{
    private readonly Dictionary<FlowId, HashSet<string>> _correlations = new();
    private readonly Dictionary<string, HashSet<FlowId>> _reverseLookup = new();
    private readonly object _sync = new();
    
    public Task Initialize() => Task.CompletedTask;

    public Task Truncate()
    {
        lock (_sync)
        {
            _correlations.Clear();
            _reverseLookup.Clear();
        }

        return Task.CompletedTask;
    }

    public Task SetCorrelation(FlowId flowId, string correlationId)
    {
        lock (_sync)
        {
            if (!_correlations.ContainsKey(flowId))
                _correlations[flowId] = new HashSet<string>();

            _correlations[flowId].Add(correlationId);

            if (!_reverseLookup.ContainsKey(correlationId))
                _reverseLookup[correlationId] = new HashSet<FlowId>();

            _reverseLookup[correlationId].Add(flowId);
        }

        return Task.CompletedTask;
    }

    public Task<IReadOnlyList<FlowId>> GetCorrelations(string correlationId)
    {
        lock (_sync)
        {
            if (!_reverseLookup.ContainsKey(correlationId))
                return new List<FlowId>().CastTo<IReadOnlyList<FlowId>>().ToTask();

            return _reverseLookup[correlationId].ToList().CastTo<IReadOnlyList<FlowId>>().ToTask();
        }
    }

    public Task<IReadOnlyList<FlowInstance>> GetCorrelations(FlowType flowType, string correlationId)
    {
        lock (_sync)
        {
            return _correlations
                .Where(kv => kv.Key.Type == flowType && kv.Value.Contains(correlationId))
                .Select(kv => kv.Key.Instance)
                .ToList()
                .CastTo<IReadOnlyList<FlowInstance>>()
                .ToTask();
        }
    }

    public Task<IReadOnlyList<string>> GetCorrelations(FlowId flowId)
    {
        lock (_sync)
        {
            if (!_correlations.ContainsKey(flowId))
                return new List<string>().CastTo<IReadOnlyList<string>>().ToTask();

            return _correlations[flowId]
                .ToList()
                .CastTo<IReadOnlyList<string>>()
                .ToTask();
        }
    }

    public Task RemoveCorrelations(FlowId flowId)
    {
        lock (_sync)
        {
            if (!_correlations.ContainsKey(flowId))
                return Task.CompletedTask;

            var correlations = _correlations[flowId];
            foreach (var correlation in correlations)
                _reverseLookup[correlation].Remove(flowId);

            _correlations.Remove(flowId);
        }

        return Task.CompletedTask;
    }

    public Task RemoveCorrelation(FlowId flowId, string correlationId)
    {
        lock (_sync)
        {
            _correlations[flowId].Remove(correlationId);
            _reverseLookup[correlationId].Remove(flowId);
        }

        return Task.CompletedTask;
    }
}