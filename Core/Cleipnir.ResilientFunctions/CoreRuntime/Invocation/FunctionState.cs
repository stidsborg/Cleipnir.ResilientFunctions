using System;
using Cleipnir.ResilientFunctions.Domain;

namespace Cleipnir.ResilientFunctions.CoreRuntime.Invocation;

public record FunctionState<TParam, TScrapbook, TReturn>(
    FunctionId FunctionId, 
    Status Status,
    int Epoch,
    int Version,
    long CrashedCheckFrequency,
    TParam Param, 
    TScrapbook Scrapbook, 
    TReturn? Result,
    DateTime? PostponedUntil,
    Exception? Error
) where TParam : notnull where TScrapbook : RScrapbook, new();