using System;

namespace Cleipnir.ResilientFunctions.Storage;

public record ComplimentaryState(
    Func<StoredParameter> StoredParameterFunc, 
    Func<StoredState> StoredStateFunc, 
    long LeaseLength
);