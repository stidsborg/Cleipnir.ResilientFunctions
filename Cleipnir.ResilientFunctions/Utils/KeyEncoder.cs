using System;

namespace Cleipnir.ResilientFunctions.Utils;

public static class KeyEncoder
{
    public static string Encode(string groupId, string? instanceId)
    {
        if (instanceId == null)
            return $"-01{groupId}";

        if (groupId.Length > 999)
            throw new ArgumentOutOfRangeException(nameof(groupId), "Length exceed 999 characters");
        
        return $"{groupId.Length:D3}{groupId}{instanceId}";
    }

    public static GroupAndInstance Decode(string encoded)
    {
        var length = int.Parse(encoded[..3]);
        if (length == -1)
            return new GroupAndInstance(encoded[3..], null);

        var rest = encoded[3..];
        var group = rest[..length];
        var instance = rest[length..];
        return new GroupAndInstance(group, instance);
    }
}

public record GroupAndInstance(string GroupId, string? InstanceId);