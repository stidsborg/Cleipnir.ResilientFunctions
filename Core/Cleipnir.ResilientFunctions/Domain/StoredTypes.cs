using System.Collections.Generic;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.Domain;

public class StoredTypes(ITypeStore typeStore)
{
    private volatile IReadOnlyDictionary<FlowType, StoredType> _cache = new Dictionary<FlowType, StoredType>();
    
    public async Task<StoredType> InsertOrGet(FlowType flowType)
    {
        if (_cache.TryGetValue(flowType, out var cachedStoredType))
            return cachedStoredType;

        var storedType = await typeStore.InsertOrGetStoredType(flowType);
        _cache = await typeStore.GetAllFlowTypes();
        
        return storedType;
    }
}