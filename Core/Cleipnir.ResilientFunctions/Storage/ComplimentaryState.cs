using System;
using Cleipnir.ResilientFunctions.Domain;

namespace Cleipnir.ResilientFunctions.Storage;

public record ComplimentaryState(
    Func<StoredParameter> StoredParameterFunc, 
    Func<StoredScrapbook> StoredScrapbookFunc, 
    long LeaseLength,
    FunctionId? SendResultTo
);