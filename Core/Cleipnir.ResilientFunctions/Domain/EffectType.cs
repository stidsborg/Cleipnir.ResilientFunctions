namespace Cleipnir.ResilientFunctions.Domain;

public enum EffectType
{
    Effect = 'E',
    State = 'S',
    System = 'Y'
}

public static class EffectTypeExtensions
{
    public static bool IsState(this EffectType effectType) => effectType == EffectType.State;
    public static bool IsSystem(this EffectType effectType) => effectType == EffectType.System;
    public static bool IsEffect(this EffectType effectType) => effectType == EffectType.Effect;
}