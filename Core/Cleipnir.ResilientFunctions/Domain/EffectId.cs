namespace Cleipnir.ResilientFunctions.Domain;

public record EffectId(string Value, bool IsState);

public static class EffectIdExtensions
{
    public static EffectId ToEffectId(this string value, bool isState = false) => new(value, isState);
}