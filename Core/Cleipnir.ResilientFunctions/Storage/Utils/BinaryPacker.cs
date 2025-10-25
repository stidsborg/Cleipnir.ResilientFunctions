using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;

namespace Cleipnir.ResilientFunctions.Storage.Utils;

public static class BinaryPacker
{
    public static byte[] Pack(params byte[]?[] arrays)
    {
        var combinedArrSize = arrays.Sum(arr => arr?.Length ?? 0) + sizeof(int) * arrays.Length;
        var combinedArr = new byte[combinedArrSize];
        var outer = 0;
        foreach(var array in arrays)
        {
            var length = array?.Length ?? -1;
            var lengthBytes = BitConverter.GetBytes(IPAddress.HostToNetworkOrder(length));
            combinedArr[outer] = lengthBytes[0];
            combinedArr[++outer] = lengthBytes[1]; 
            combinedArr[++outer] = lengthBytes[2]; 
            combinedArr[++outer] = lengthBytes[3];
            outer++;
            
            if (array == null)
                continue;
            
            Array.Copy(array, sourceIndex: 0, combinedArr, destinationIndex: outer, length: array.Length);
            outer += array.Length;
        }

        return combinedArr;
    }

    public static IReadOnlyList<byte[]?> Split(byte[] array, int expectedPieces = 1)
    {
        var arrays = new List<byte[]?>(expectedPieces);
        var source = array;
        for (var i = 0; i < source.Length;)
        {
            var arraySize = IPAddress.NetworkToHostOrder(BitConverter.ToInt32(source[i..(i + 4)]));
            i += 4;
            if (arraySize == -1)
            {
                arrays.Add(null);
                continue;
            }
            
            var destination = new byte[arraySize];
            
            Array.Copy(source, sourceIndex: i, destination, destinationIndex: 0, length: destination.Length);
            i += arraySize;
            
            arrays.Add(destination);
        }
        
        return arrays.ToArray();
    }
    
    public static byte[] Append(byte[] source, params byte[]?[] arrays)
    {
        var combinedArrSize = arrays.Sum(arr => arr?.Length ?? 0) + sizeof(int) * arrays.Length;
        var combinedArr = new byte[source.Length + combinedArrSize];
        
        Array.Copy(source, combinedArr, source.Length);
        
        var outer = source.Length;
        foreach(var array in arrays)
        {
            var length = array?.Length ?? -1;
            var lengthBytes = BitConverter.GetBytes(IPAddress.HostToNetworkOrder(length));
            combinedArr[outer] = lengthBytes[0];
            combinedArr[++outer] = lengthBytes[1]; 
            combinedArr[++outer] = lengthBytes[2]; 
            combinedArr[++outer] = lengthBytes[3];
            outer++;
            
            if (array == null)
                continue;
            
            Array.Copy(array, sourceIndex: 0, combinedArr, destinationIndex: outer, length: array.Length);
            outer += array.Length;
        }

        return combinedArr;
    }

}