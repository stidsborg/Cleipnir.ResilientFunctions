using System;
using System.Net;
using System.Security.Cryptography;
using System.Text;

namespace Cleipnir.ResilientFunctions.Storage;

public static class StoredIdFactory
{
    public static Guid FromString(string instanceId)
    {
        var bytes = Encoding.UTF8.GetBytes(instanceId);
        using SHA256 sha256 = SHA256.Create();
        var hashedBytes = sha256.ComputeHash(bytes);
        var guid = new Guid(hashedBytes[..16]);
        return guid;
    }

    public static Guid FromInt(int instanceId)
    {
        byte[] bytes = new byte[16];
        var intBytes = BitConverter.GetBytes(IPAddress.HostToNetworkOrder(instanceId));
        intBytes.CopyTo(bytes, 0);
        return new Guid(bytes);
    }

    public static Guid FromLong(long instanceId)
    {
        byte[] bytes = new byte[16];
        var longBytes = BitConverter.GetBytes(IPAddress.HostToNetworkOrder(instanceId));
        longBytes.CopyTo(bytes, 0);
        return new Guid(bytes);
    }
}