using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;

namespace Cleipnir.ResilientFunctions.Helpers;

internal static class Helpers
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

    public static IEnumerable<T> WithRandomOffset<T>(this IReadOnlyList<T> elms)
    {
        if (elms.Count == 0)
            yield break;
        
        var offset = Random.Shared.Next(0, elms.Count);
        var i = offset;
        do
        {
            yield return elms[i];
            i = (i + 1) % elms.Count;
        } while (i != offset);
    }
    
    internal static string MinifyJson(string json) => Regex.Replace(json, "(\"(?:[^\"\\\\]|\\\\.)*\")|\\s+", "$1");
    
    public static byte[] ToUtf8Bytes(this string str) => Encoding.UTF8.GetBytes(str);
    public static string ToStringFromUtf8Bytes(this byte[] bytes) => Encoding.UTF8.GetString(bytes);

    public static Guid ToGuid(this string s) => Guid.Parse(s);
    
    public static string StringJoin(this IEnumerable<string> strings, string separator) => string.Join(separator, strings);
}