using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;

namespace Cleipnir.ResilientFunctions.Storage;

public class InMemoryTypeStore : ITypeStore
{
    private ImmutableDictionary<FlowType, ushort> _flowTypes = ImmutableDictionary<FlowType, ushort>.Empty;
    private readonly Lock _sync = new();
    
    public Task<StoredType> InsertOrGetStoredType(FlowType flowType)
    {
        lock (_sync)
        {
            if (_flowTypes.TryGetValue(flowType, out var index))
                return index.ToStoredType().ToTask();

            index = (ushort) _flowTypes.Count;
            _flowTypes = _flowTypes.SetItem(flowType, index);
            return index.ToStoredType().ToTask();
        }
    }

    public Task<IReadOnlyDictionary<FlowType, StoredType>> GetAllFlowTypes() 
        => ((IReadOnlyDictionary<FlowType, StoredType>) _flowTypes.ToDictionary(kv => kv.Key, kv => kv.Value.ToStoredType())).ToTask();
}