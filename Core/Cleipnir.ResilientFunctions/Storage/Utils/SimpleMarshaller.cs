using System.Collections.Generic;
using System.Text;

namespace Cleipnir.ResilientFunctions.Storage.Utils;

public static class SimpleMarshaller
{
    public static string Serialize(params string?[] strings)
    {
        var builder = new StringBuilder();
        foreach (var s in strings)
        {
            builder.Append($"{s?.Length ?? -1}|");
            builder.Append(s);
        }

        return builder.ToString();
    }

    public static IReadOnlyList<string?> Deserialize(string s, int? expectedCount = null)
    {
        expectedCount ??= 10;
        var strings = new List<string?>(expectedCount.Value);
        
        var i = 0;
        while (i < s.Length)
        {
            var bulkLength = BulkLength(s, ref i);
            var bulkString = 
                bulkLength == -1 
                    ? default
                    : s[i..(bulkLength + i)];
            
            strings.Add(bulkString);
            i += bulkLength == -1 ? 0 : bulkLength;
        }

        return strings;
    }

    private static int BulkLength(string s, ref int i)
    {
        var lengthStr = "";
        while (s[i] != '|')
        {
            lengthStr += s[i].ToString();
            i++;    
        }

        i++;
        return int.Parse(lengthStr);
    }
}