using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions;

public delegate Task<RResult<TResult>> RFunc<TParam, TResult>(TParam param, Action? onPersisted = null, bool reInvoke = false, Status[]? onlyReInvokeWhen = null)
    where TParam : notnull where TResult : notnull;
public delegate Task<RResult> RAction<TParam>(TParam param, Action? onPersisted = null, bool reInvoke = false, Status[]? onlyReInvokeWhen = null)
    where TParam : notnull;
    