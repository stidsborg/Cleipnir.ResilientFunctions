using System.Collections.Generic;

namespace Cleipnir.ResilientFunctions.Storage;

public class StoreCommand
{
    public required string Sql { get; init; }
    public required List<object> Parameters { get; init; }
}