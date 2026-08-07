using System.Collections.Generic;
using System.Threading.Tasks;

namespace Cleipnir.ResilientFunctions.Storage;

/// <summary>
/// Persisted mapping between a .NET type's id and its encoded form - the UTF-8 bytes of its simple qualified name
/// (see <see cref="Domain.TypeMapper"/>). An effect result records the id of the type it was serialized as, so a
/// mapping row must be persisted before the first effect referencing it - without the row the effect's result can
/// never be deserialized. Ids are content-derived (SHA-256 of the encoded type), so inserting an already-present
/// mapping is a no-op.
/// </summary>
public interface ITypeStore
{
    public Task InsertTypes(IReadOnlyDictionary<TypeId, byte[]> types);
    public Task<IReadOnlyDictionary<TypeId, byte[]>> GetAllTypes();
}
