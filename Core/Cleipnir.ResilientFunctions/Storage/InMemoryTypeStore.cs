using System.Collections.Generic;
using System.Collections.Immutable;
using System.Threading;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Helpers;

namespace Cleipnir.ResilientFunctions.Storage;

public class InMemoryTypeStore : ITypeStore
{
    private ImmutableDictionary<long, byte[]> _types = ImmutableDictionary<long, byte[]>.Empty;
    private readonly Lock _sync = new();

    public Task InsertTypes(IReadOnlyDictionary<long, byte[]> types)
    {
        lock (_sync)
            _types = _types.SetItems(types);

        return Task.CompletedTask;
    }

    public Task<IReadOnlyDictionary<long, byte[]>> GetAllTypes()
        => ((IReadOnlyDictionary<long, byte[]>) _types).ToTask();
}
