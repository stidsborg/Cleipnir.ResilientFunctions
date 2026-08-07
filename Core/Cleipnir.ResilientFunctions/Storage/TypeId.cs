using System.Buffers.Binary;

namespace Cleipnir.ResilientFunctions.Storage;

/// <summary>
/// Content-derived identity of a .NET type: the first 8 bytes of the SHA-256 hash of the type's encoded form
/// (see <see cref="Domain.TypeMapper"/>). Persisted inside effect results and messages in place of the encoded
/// type itself; the id -> encoded-type mapping lives in the <see cref="ITypeStore"/>.
/// </summary>
public readonly record struct TypeId(long Value)
{
    public byte[] Serialize()
    {
        var bytes = new byte[sizeof(long)];
        BinaryPrimitives.WriteInt64LittleEndian(bytes, Value);
        return bytes;
    }

    public static TypeId Deserialize(byte[] bytes) => new(BinaryPrimitives.ReadInt64LittleEndian(bytes));
}
