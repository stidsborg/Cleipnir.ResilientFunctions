using System.Collections.Generic;
using System.Linq;

namespace Cleipnir.ResilientFunctions.Domain;

public record EffectId(string Id, string Context)
{
    public SerializedEffectId Serialize()
    {
        var (id, context) = this;
        if (id.All(c => c != '.' && c != '\\'))
            return new SerializedEffectId(
                context == ""
                    ? $"E{id}"
                    : $"{context}.E{id}"
                );

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
        return new SerializedEffectId(
            context == ""
                ? $"E{escapedId}"
                : $"{context}.E{escapedId}"
            );
    }

    public static EffectId Deserialize(SerializedEffectId serialized) => Deserialize(serialized.Value);
    public static EffectId Deserialize(string serialized)
    {
        int pos;
        for (pos = serialized.Length - 1; pos > 0; pos--)
            if (serialized[pos] == '.' && serialized[pos - 1] != '\\')
                break;

        if (serialized[pos] == '.')
            pos++;

        var context = pos - 1 < 0 ? "" : serialized[..(pos - 1)];
        var id = serialized[(pos + 1)..];
        if (id.Any(c => c == '\\'))
        {
            id = id.Replace("\\.", ".").Replace("\\\\", "\\");
        }

        return new EffectId(id, context);
    }

    public static EffectId CreateWithRootContext(string id)
        => new(id, Context: "");

    public static EffectId CreateWithCurrentContext(string id)
        => new(id, EffectContext.CurrentContext.Parent?.Serialize().Value ?? "");
}

public record SerializedEffectId(string Value);

public static class EffectIdExtensions
{
    public static EffectId ToEffectId(this string value, string? context = null) => new(value, context ?? "");
    public static SerializedEffectId ToSerializedEffectId(this string value) => new(value);
}