namespace Cleipnir.ResilientFunctions.Domain;

public record EffectId(string Value, bool IsState)
{
    public string Serialize() => IsState ? $"S!{Value}" : $"E!{Value}" ;

    public static EffectId Deserialize(string s)
    {
        var suffix = s[2..];
        var isState = s[0] == 'S';
        
        return new EffectId(suffix, isState);
    }
}

public static class EffectIdExtensions
{
    public static EffectId ToEffectId(this string value, bool isState = false) => new(value, isState);
}