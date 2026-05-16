namespace Cleipnir.ResilientFunctions.Domain;

public record EffectResult(EffectId Id, object? Value, string? Alias)
{
    public static EffectResult Create(EffectId id, object? value) => new(id, value, Alias: null);
};