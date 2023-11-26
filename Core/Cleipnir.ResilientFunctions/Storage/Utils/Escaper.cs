using System;
using System.Text;

namespace Cleipnir.ResilientFunctions.Storage.Utils;

public static class Escaper
{
    public static string Escape(string delimiter, params string[] strings)
    {
        for (var i = 0; i < strings.Length; i++)
            strings[i] = strings[i].Replace(delimiter, $"{delimiter}{delimiter}");

        return string.Join(delimiter, strings);
    }

    public static string[] Unescape(string s, char delimiter, int arraySize)
    {
        if (s == "") return Array.Empty<string>();
        
        var strings = new string[arraySize];
        var builder = new StringBuilder();
        var arrayIndex = 0;
        var stringIndex = 0;
        while (stringIndex < s.Length)
        {
            if (s[stringIndex] == delimiter && s[stringIndex + 1] == delimiter)
            {
                builder.Append(delimiter);
                stringIndex += 2;
                continue;
            }
            
            if (s[stringIndex] == delimiter)
            {
                strings[arrayIndex] = builder.ToString();
                arrayIndex++;
                builder.Clear();
            }
            else
                builder.Append(s[stringIndex]);
            
            stringIndex++;
        }

        strings[arrayIndex] = builder.ToString();

        return strings;
    }
}