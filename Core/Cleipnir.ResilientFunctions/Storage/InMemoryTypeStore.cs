using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;

namespace Cleipnir.ResilientFunctions.Storage;

public class InMemoryTypeStore : ITypeStore
{
    private ImmutableDictionary<FlowType, int> _flowTypes = ImmutableDictionary<FlowType, int>.Empty;
    private readonly object _sync = new();
    
    public Task<int> InsertOrGetFlowType(FlowType flowType)
    {
        lock (_sync)
        {
            if (_flowTypes.TryGetValue(flowType, out var index))
                return index.ToTask();

            index = _flowTypes.Count;
            _flowTypes = _flowTypes.SetItem(flowType, index);
            return index.ToTask();
        }
    }

    public Task<IReadOnlyDictionary<FlowType, int>> GetAllFlowTypes() 
        => ((IReadOnlyDictionary<FlowType, int>) _flowTypes.ToDictionary(kv => kv.Key, kv => kv.Value)).ToTask();
}