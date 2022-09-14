using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;

namespace Cleipnir.ResilientFunctions;

public static class RAction
{
    public delegate Task Invoke<in TParam>(string functionInstanceId, TParam param) where TParam : notnull;

    public delegate Task ReInvoke(
        string functionInstanceId,
        IEnumerable<Status> expectedStatuses,
        int? expectedEpoch = null
    );
    
    public delegate Task ReInvoke<TScrapbook>(
        string functionInstanceId,
        IEnumerable<Status> expectedStatuses,
        int? expectedEpoch = null,
        Action<TScrapbook>? scrapbookUpdater = null
    );
}

public record RAction<TParam>(
    RAction.Invoke<TParam> Invoke,
    RAction.ReInvoke<RScrapbook> ReInvoke,
    Schedule<TParam> Schedule,
    ScheduleReInvocation<RScrapbook> ScheduleReInvocation
) : RAction<TParam, RScrapbook>(Invoke, ReInvoke, Schedule, ScheduleReInvocation) where TParam : notnull;

public record RAction<TParam, TScrapbook>(
    RAction.Invoke<TParam> Invoke,
    RAction.ReInvoke<TScrapbook> ReInvoke,
    Schedule<TParam> Schedule,
    ScheduleReInvocation<TScrapbook> ScheduleReInvocation
) where TParam : notnull;

public static class RActionExtensions
{
    public static RAction<TParam> ConvertToRActionWithoutScrapbook<TParam>(this RAction<TParam, RScrapbook> rAction) 
        where TParam : notnull 
        => new(rAction.Invoke, rAction.ReInvoke, rAction.Schedule, rAction.ScheduleReInvocation);
} 