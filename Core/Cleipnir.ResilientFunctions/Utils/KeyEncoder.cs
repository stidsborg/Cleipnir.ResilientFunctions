using System;

namespace Cleipnir.ResilientFunctions.Utils;

public static class KeyEncoder
{
    public static string Encode(string group, string key)
    {
        if (group.Length > 999)
            throw new ArgumentOutOfRangeException(nameof(group), "Length exceed 999 characters");
        
        return $"{group.Length:D3}{group}{key}";
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