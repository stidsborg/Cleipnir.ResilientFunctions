using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions;

public delegate Task<RResult> ReInvokeAction<TParam, TScrapbook>(
    string instanceId,
    Action<TParam, TScrapbook> initializer,
    IEnumerable<Status> expectedStatuses
) where TParam : notnull where TScrapbook : RScrapbook;

public delegate Task<RResult> ReInvokeAction<TParam>(
    string instanceId,
    Action<TParam> initializer,
    IEnumerable<Status> expectedStatuses
) where TParam : notnull;

public delegate Task<RResult<TResult>> ReInvokeFunc<TParam, TResult>(
    string instanceId,
    Action<TParam> initializer,
    IEnumerable<Status> expectedStatuses
) where TParam : notnull where TResult : notnull;

public delegate Task<RResult<TResult>> ReInvokeFunc<TParam, TScrapbook, TResult>(
    string instanceId,
    Action<TParam, TScrapbook> initializer,
    IEnumerable<Status> expectedStatuses
) where TParam : notnull where TScrapbook : RScrapbook where TResult : notnull;

public record RFuncRegistration<TParam, TResult>(
    RFunc<TParam, TResult> RFunc,
    ReInvokeFunc<TParam, TResult> ReInvoke,
    Schedule<TParam> Schedule
) where TParam : notnull where TResult : notnull;

public record RFuncRegistration<TParam, TScrapbook, TResult> (
    RFunc<TParam, TResult> RFunc,
    ReInvokeFunc<TParam, TScrapbook, TResult> ReInvoke,
    Schedule<TParam> Schedule
) where TParam : notnull where TScrapbook : RScrapbook where TResult : notnull;

public record RActionRegistration<TParam, TScrapbook>(
    RAction<TParam> RAction,
    ReInvokeAction<TParam, TScrapbook> ReInvoke,
    Schedule<TParam> Schedule
) where TParam : notnull where TScrapbook : RScrapbook;

public record RActionRegistration<TParam>(
    RAction<TParam> RAction,
    ReInvokeAction<TParam> ReInvoke,
    Schedule<TParam> Schedule
) where TParam : notnull;

public delegate Task Schedule<TParam>(TParam param);