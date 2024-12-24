using System.Collections.Generic;
using System.Linq;

namespace Cleipnir.ResilientFunctions.Domain;

public record EffectId(string Id, EffectType Type, string Context)
{
    public string Serialize()
    {
        var (id, type, context) = this;
        if (id.All(c => c != '.' && c != '\\'))
            return context == ""
                ? $"{(char)type}{id}"
                : $"{context}.{(char)type}{id}";
        
        var escapedIdList = new List<char>(id.Length * 2);
        foreach (var idChar in id)
            switch (idChar)
            {
                case '.':
                    escapedIdList.Add('\\');
                    escapedIdList.Add('.');
                    break;
                case '\\':
                    escapedIdList.Add('\\');
                    escapedIdList.Add('\\');
                    break;
                default:
                    escapedIdList.Add(idChar);
                    break;
            }
        
        var escapedId = new string(escapedIdList.ToArray());
        return context == ""
            ? $"{(char)type}{escapedId}"
            : $"{context}.{(char)type}{escapedId}"; 
    }

    public static EffectId Deserialize(string serialized)
    {
        int pos;
        for (pos = serialized.Length - 1; pos > 0; pos--)
            if (serialized[pos] == '.' && serialized[pos - 1] != '\\')
                break;

        if (serialized[pos] == '.')
            pos++;
        
        var type = (EffectType) serialized[pos];
        var context = pos - 1 < 0 ? "" : serialized[..(pos - 1)]; 
        var id = serialized[(pos + 1)..];
        if (id.Any(c => c == '\\'))
        {
            id = id.Replace("\\.", ".").Replace("\\\\", "\\");
        }

        return new EffectId(id, type, context);
    }
}

public static class EffectIdExtensions
{
    public static EffectId ToEffectId(this string value, EffectType? effectType = null, string? context = null) => new(value, effectType ?? EffectType.Effect, context ?? "");
}