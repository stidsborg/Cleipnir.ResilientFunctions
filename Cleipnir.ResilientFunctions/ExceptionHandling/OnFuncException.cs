using System;
using Cleipnir.ResilientFunctions.Domain;

namespace Cleipnir.ResilientFunctions.ExceptionHandling;

public delegate Return<TReturn> OnFuncException<TParam, TReturn>(
    Exception exception,
    FunctionInstanceId functionInstanceId,
    TParam param
) where TParam : notnull;

public delegate Return<TReturn> OnFuncException<TParam, TScrapbook, TReturn>(
    Exception exception,
    TScrapbook scrapbook,
    FunctionInstanceId functionInstanceId,
    TParam param
) where TParam : notnull where TScrapbook : RScrapbook, new();