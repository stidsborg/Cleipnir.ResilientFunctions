using Cleipnir.ResilientFunctions.Domain;

namespace Cleipnir.ResilientFunctions.Helpers;

public class Unit
{
    public static Unit Instance { get; } = new();
}
public class UnitScrapbook : RScrapbook { }