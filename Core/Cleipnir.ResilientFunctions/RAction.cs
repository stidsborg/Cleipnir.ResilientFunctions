using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;

namespace Cleipnir.ResilientFunctions;

public static class RAction
{
    public delegate Task Invoke<in TParam, in TScrapbook>(string functionInstanceId, TParam param, TScrapbook? scrapbook = null) 
        where TParam : notnull where TScrapbook : RScrapbook, new();

    public delegate Task ReInvoke<TScrapbook>(
        string functionInstanceId,
        IEnumerable<Status> expectedStatuses,
        int? expectedEpoch = null,
        Action<TScrapbook>? scrapbookUpdater = null
    );
    
    public delegate Task Schedule<in TParam, TScrapbook>(string functionInstanceId, TParam param, TScrapbook? scrapbook = null) 
        where TParam : notnull where TScrapbook : RScrapbook, new();

    public delegate Task ScheduleReInvoke<TScrapbook>(
        string functionInstanceId, 
        IEnumerable<Status> expectedStatuses, 
        int? expectedEpoch = null,
        Action<TScrapbook>? scrapbookUpdater = null
    );
}

public record RAction<TParam>(
    RAction.Invoke<TParam, RScrapbook> Invoke,
    RAction.ReInvoke<RScrapbook> ReInvoke,
    RAction.Schedule<TParam, RScrapbook> Schedule,
    RAction.ScheduleReInvoke<RScrapbook> ScheduleReInvoke
) : RAction<TParam, RScrapbook>(Invoke, ReInvoke, Schedule, ScheduleReInvoke) where TParam : notnull;

public record RAction<TParam, TScrapbook>(
    RAction.Invoke<TParam, TScrapbook> Invoke,
    RAction.ReInvoke<TScrapbook> ReInvoke,
    RAction.Schedule<TParam, TScrapbook> Schedule,
    RAction.ScheduleReInvoke<TScrapbook> ScheduleReInvoke
) where TParam : notnull where TScrapbook : RScrapbook, new(); 

public static class RActionExtensions
{
    public static RAction<TParam> ConvertToRActionWithoutScrapbook<TParam>(this RAction<TParam, RScrapbook> rAction) 
        where TParam : notnull 
        => new(rAction.Invoke, rAction.ReInvoke, rAction.Schedule, rAction.ScheduleReInvoke);
} 