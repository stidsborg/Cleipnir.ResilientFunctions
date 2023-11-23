using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Text.RegularExpressions;

namespace Cleipnir.ResilientFunctions.Helpers;

public static class Helpers
{
    public static long GenerateRandomLong()
    {
        var guid = Guid.NewGuid();
        var guidBytes = guid.ToByteArray();

        var randomLongBytes = new byte[8];

        for (var i = 0; i < 8; i++)
            randomLongBytes[i] = (byte) (guidBytes[i] ^ guidBytes[i + 8]);

        var randomLong = BitConverter.ToInt64(randomLongBytes, 0);
        return randomLong;
    }
        
    public static int GenerateRandomInt()
    {
        var guid = Guid.NewGuid();
        var guidBytes = guid.ToByteArray();
            
        Span<byte> randomBytes = stackalloc byte[4];

        for (var i = 0; i < 4; i++)
        for (var j = 0; j < 16/4; j++)
            randomBytes[i] = (byte) (randomBytes[i] ^ guidBytes[i +j]);

        ReadOnlySpan<byte> readOnlyBytes = randomBytes;
            
        var randomInt = BitConverter.ToInt32(readOnlyBytes);
        return randomInt;
    }

    public static T[] RandomlyPermute<T>(this IEnumerable<T> t)
    {
        var random = new Random(GenerateRandomInt());

        var arr = t.ToArray();
        for (var i = 0; i < arr.Length - 1; i++)
        {
            var v = arr[i];
            var toSwap = random.Next(arr.Length - i) + i;
            arr[i] = arr[toSwap];
            arr[toSwap] = v;
        }

        return arr;
    }

    internal static string MinifyJson(string json) => Regex.Replace(json, "(\"(?:[^\"\\\\]|\\\\.)*\")|\\s+", "$1");
}