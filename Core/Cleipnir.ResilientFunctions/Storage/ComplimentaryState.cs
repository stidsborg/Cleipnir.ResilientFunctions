using System;
using Cleipnir.ResilientFunctions.Domain;

namespace Cleipnir.ResilientFunctions.Storage;

public record ComplimentaryState2(
    Func<StoredParameter> StoredParameterFunc, 
    Func<StoredScrapbook> StoredScrapbookFunc, 
    long LeaseLength,
    FunctionId? SendResultTo
);