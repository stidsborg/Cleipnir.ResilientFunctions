using System.Collections.Generic;
using System.Collections.Immutable;
using System.Threading;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Helpers;

namespace Cleipnir.ResilientFunctions.Storage;

public class InMemoryTypeStore : ITypeStore
{
    private ImmutableDictionary<TypeId, byte[]> _types = ImmutableDictionary<TypeId, byte[]>.Empty;
    private readonly Lock _sync = new();

    public Task InsertTypes(IReadOnlyDictionary<TypeId, byte[]> types)
    {
        lock (_sync)
            _types = _types.SetItems(types);

        return Task.CompletedTask;
    }

    public Task<IReadOnlyDictionary<TypeId, byte[]>> GetAllTypes()
        => ((IReadOnlyDictionary<TypeId, byte[]>) _types).ToTask();
}
