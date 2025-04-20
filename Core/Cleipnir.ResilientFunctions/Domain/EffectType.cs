namespace Cleipnir.ResilientFunctions.Domain;

public enum EffectType
{
    Effect = 'E',
    State = 'S',
    System = 'Y',
    Timeout = 'T',
    Retry = 'R'
}

public static class EffectTypeExtensions
{
    public static bool IsState(this EffectType effectType) => effectType == EffectType.State;
    public static bool IsSystem(this EffectType effectType) => effectType == EffectType.System;
    public static bool IsEffect(this EffectType effectType) => effectType == EffectType.Effect;
    public static bool IsTimeout(this EffectType effectType) => effectType == EffectType.Timeout;
    public static bool IsRetry(this EffectType effectType) => effectType == EffectType.Retry;
}