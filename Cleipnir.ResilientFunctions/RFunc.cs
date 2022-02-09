using System;
using System.Threading.Tasks;

namespace Cleipnir.ResilientFunctions;

public delegate Task<RResult<TResult>> RFunc<TParam, TResult>(TParam param, Action? onPersisted = null)
    where TParam : notnull where TResult : notnull;
public delegate Task<RResult> RAction<TParam>(TParam param, Action? onPersisted = null)
    where TParam : notnull;
    