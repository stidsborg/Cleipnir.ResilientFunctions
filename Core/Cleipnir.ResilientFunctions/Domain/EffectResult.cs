namespace Cleipnir.ResilientFunctions.Domain;

public record EffectResult(EffectId Id, object? Value, string? Alias, bool Delete = false)
{
    public static EffectResult Create(EffectId id, object? value) => new(id, value, Alias: null);
    public static EffectResult Clear(EffectId id) => new(id, Value: null, Alias: null, Delete: true);
};
