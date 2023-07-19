using System;
using Cleipnir.ResilientFunctions.Domain;

namespace Cleipnir.ResilientFunctions.CoreRuntime.Invocation;

public record FunctionState<TParam, TScrapbook, TReturn>(
    FunctionId FunctionId, 
    Status Status,
    int Epoch,
    long SignOfLifeFrequency,
    TParam Param, 
    TScrapbook Scrapbook, 
    TReturn? Result,
    DateTime? PostponedUntil,
    PreviouslyThrownException? PreviouslyThrownException
) where TParam : notnull where TScrapbook : RScrapbook, new();