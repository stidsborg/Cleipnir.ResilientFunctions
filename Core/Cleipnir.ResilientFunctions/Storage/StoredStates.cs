using System.Collections.Generic;
using System.Linq;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Storage.Utils;

namespace Cleipnir.ResilientFunctions.Storage;

public enum StoredStateType : byte
{
    Param = 0,
    Instance = 1,
    Effect = 2,
    Message = 3
}

public record StoredStateEntity(StoredStateType Type, string Id, byte[] Content, bool Deleted)
{
    public byte[] Serialize()
    {
        var type = new[] { (byte)Type };
        var idBytes = Id.ToUtf8Bytes();
        var deleted = new[] {Deleted ? (byte)1 : (byte)0};

        return BinaryPacker.Pack(type, idBytes, Content, deleted);
    }

    public static StoredStateEntity Deserialize(byte[] serialized)
    {
        var arrays = BinaryPacker.Split(serialized, expectedPieces: 4);
        var type = arrays[0]![0];
        var id = arrays[1]!.ToStringFromUtf8Bytes();
        var content = arrays[2]!;
        var deleted = arrays[3]![0];
        return new StoredStateEntity((StoredStateType) type, id, content, Deleted: deleted != 0);
    }
}

public record StoredStates(StoredStateEntity[] Entities)
{
    public byte[][] Serialize() => Entities.Select(entity => entity.Serialize()).ToArray();
    
    public static List<StoredStateEntity> Deserialize(byte[][] bytes)
    {
        var states = new List<StoredStateEntity>();
        foreach (var array in bytes)
        {
            var state = StoredStateEntity.Deserialize(array);
            states.Add(state);
        }

        return states;
    }
}