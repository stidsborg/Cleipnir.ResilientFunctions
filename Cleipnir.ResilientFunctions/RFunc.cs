using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;

namespace Cleipnir.ResilientFunctions;

public delegate Task<RResult<TResult>> RFunc<TParam, TResult>(TParam param, Action? onPersisted = null)
    where TParam : notnull where TResult : notnull;
public delegate Task<RResult> RAction<TParam>(TParam param, Action? onPersisted = null)
    where TParam : notnull;
public delegate Task<RResult> RAction<TParam, TScrapbook>(TParam param, TScrapbook scrapbook, Action? onPersisted = null)
    where TParam : notnull where TScrapbook : RScrapbook, new();

