using System;

namespace Cleipnir.ResilientFunctions.Storage;

[Flags]
public enum Status
{
    Executing = 0,
    Succeeded = 1,
    Failed = 2,
    Postponed = 4,
    Barricaded = 8,
}