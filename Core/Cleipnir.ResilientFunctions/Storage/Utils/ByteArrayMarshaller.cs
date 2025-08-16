using System;
using System.Buffers.Binary;
using System.Collections.Generic;

namespace Cleipnir.ResilientFunctions.Storage.Utils;

public static class ByteArrayMarshaller
{
    public static byte[] Serialize(params byte[]?[] arrays)
    {
        var totalSize = 0;
        foreach (var array in arrays)
            totalSize += array == null ? 4 : array.Length + 4;
        
        var destinationArray = new byte[totalSize];
        var i = 0;
        foreach (var sourceArray in arrays)
        {
            var lengthSpan = destinationArray.AsSpan(start: i, length: 4);
            BinaryPrimitives.WriteInt32LittleEndian(lengthSpan, sourceArray?.Length ?? -1); 
            i += 4;
            if (sourceArray == null || sourceArray.Length == 0) continue;
            Buffer.BlockCopy(sourceArray, srcOffset: 0, destinationArray, dstOffset: i, count: sourceArray.Length);
            i += sourceArray.Length;
        }

        return destinationArray;
    }

    public static IReadOnlyList<ReadOnlyMemory<byte>?> Deserialize(byte[] sourceArray, int? expectedCount = null)
    {
        expectedCount ??= 10;
        var arrays = new List<ReadOnlyMemory<byte>?>(expectedCount.Value);
        
        var i = 0;
        while (i < sourceArray.Length)
        {
            var lengthSpan = sourceArray.AsSpan(start: i, length: 4);
            var length = BinaryPrimitives.ReadInt32LittleEndian(lengthSpan);
            i += 4;
            if (length == -1)
            {
                arrays.Add(null);
                continue;   
            }
            
            var segment = new ReadOnlyMemory<byte>(sourceArray, start: i, length);
            arrays.Add(segment);
            i += length;
        }

        return arrays;
    }
}