using System.Text;

namespace Cleipnir.ResilientFunctions.AzureBlob;

public static class SimpleDictionaryMarshaller
{
    public static string Serialize(IReadOnlyDictionary<string, string?> dictionary)
    {
        var builder = new StringBuilder();
        foreach (var (key, value) in dictionary)
        {
            builder.Append($"{key.Length}|");
            builder.Append(key);
            builder.Append($"{value?.Length ?? -1}|");
            builder.Append(value);
        }

        return builder.ToString();
    }

    public static Dictionary<string, string?> Deserialize(string s, int? expectedCount = null)
    {
        expectedCount ??= 10;
        var dictionary = new Dictionary<string, string?>(expectedCount.Value);
        
        var i = 0;
        while (i < s.Length)
        {
            var bulkLength = BulkLength(s, ref i);
            var key = s[i..(bulkLength + i)];
            i += bulkLength;
            
            bulkLength = BulkLength(s, ref i);
            var value = 
                bulkLength == -1 
                    ? default
                    : s[i..(bulkLength + i)];
            
            i += bulkLength == -1 ? 0 : bulkLength;

            dictionary[key] = value;
        }

        return dictionary;
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