using System;

namespace Cleipnir.ResilientFunctions.Storage;

public record ComplimentaryState(Func<string?> StoredParameterFunc, long LeaseLength);