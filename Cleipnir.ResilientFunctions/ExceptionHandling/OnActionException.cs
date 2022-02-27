using System;
using Cleipnir.ResilientFunctions.Domain;

namespace Cleipnir.ResilientFunctions.ExceptionHandling;

public delegate Return OnActionException<TParam>(
    Exception exception,
    FunctionInstanceId functionInstanceId,
    TParam param
) where TParam : notnull;

public delegate Return OnActionException<TParam, TScrapbook>(
    Exception exception,
    TScrapbook scrapbook,
    FunctionInstanceId functionInstanceId,
    TParam param
) where TParam : notnull where TScrapbook : RScrapbook;