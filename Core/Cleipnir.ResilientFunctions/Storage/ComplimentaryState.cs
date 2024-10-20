using System;

namespace Cleipnir.ResilientFunctions.Storage;

public record ComplimentaryState(Func<byte[]?> StoredParameterFunc, long LeaseLength);