using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;

namespace Cleipnir.ResilientFunctions;

public static class RAction
{
    public delegate Task Invoke<in TParam, in TScrapbook>(string functionInstanceId, TParam param, TScrapbook? scrapbook = null) 
        where TParam : notnull where TScrapbook : RScrapbook, new();

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
    RAction.Invoke<TParam, RScrapbook> Invoke,
    RAction.ReInvoke<RScrapbook> ReInvoke,
    Schedule<TParam, RScrapbook> Schedule,
    ScheduleReInvocation<RScrapbook> ScheduleReInvocation
) : RAction<TParam, RScrapbook>(Invoke, ReInvoke, Schedule, ScheduleReInvocation) where TParam : notnull;

public record RAction<TParam, TScrapbook>(
    RAction.Invoke<TParam, TScrapbook> Invoke,
    RAction.ReInvoke<TScrapbook> ReInvoke,
    Schedule<TParam, TScrapbook> Schedule,
    ScheduleReInvocation<TScrapbook> ScheduleReInvocation
) where TParam : notnull where TScrapbook : RScrapbook, new(); 

public static class RActionExtensions
{
    public static RAction<TParam> ConvertToRActionWithoutScrapbook<TParam>(this RAction<TParam, RScrapbook> rAction) 
        where TParam : notnull 
        => new(rAction.Invoke, rAction.ReInvoke, rAction.Schedule, rAction.ScheduleReInvocation);
} 