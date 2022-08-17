using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;

namespace Cleipnir.ResilientFunctions;

public static class RFunc
{
    public delegate Task<TReturn> Invoke<in TParam, TReturn>(string functionInstanceId, TParam param)
        where TParam : notnull;

    public delegate Task<TReturn> ReInvoke<TReturn>(
        string functionInstanceId,
        IEnumerable<Status> expectedStatuses,
        int? expectedEpoch = null
    );
    
    public delegate Task<TReturn> ReInvoke<TScrapbook, TReturn>(
        string functionInstanceId,
        IEnumerable<Status> expectedStatuses,
        int? expectedEpoch = null,
        Action<TScrapbook>? scrapbookUpdater = null
    );
}

public record RFunc<TParam, TReturn>(
    RFunc.Invoke<TParam, TReturn> Invoke,
    RFunc.ReInvoke<TReturn> ReInvoke,
    Schedule<TParam> Schedule,
    ScheduleReInvocation ScheduleReInvocation
) where TParam : notnull;

public record RFunc<TParam, TScrapbook, TReturn>(
    RFunc.Invoke<TParam, TReturn> Invoke,
    RFunc.ReInvoke<TScrapbook, TReturn> ReInvoke,
    Schedule<TParam> Schedule,
    ScheduleReInvocation<TScrapbook> ScheduleReInvocation
) where TParam : notnull;
    