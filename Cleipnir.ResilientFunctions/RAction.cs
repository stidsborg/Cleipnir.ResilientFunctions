using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions;

public static class RAction
{
    public delegate Task<RResult> Invoke<TParam>(TParam param) where TParam : notnull;

    public delegate Task<RResult> ReInvoke<TParam>(
        string functionInstanceId,
        Action<TParam> initializer,
        IEnumerable<Status> expectedStatuses
    ) where TParam : notnull;
    
    public delegate Task<RResult> ReInvoke<TParam, TScrapbook>(
        string functionInstanceId,
        Action<TParam, TScrapbook> initializer,
        IEnumerable<Status> expectedStatuses
    ) where TParam : notnull where TScrapbook : RScrapbook;
    
    public delegate Task Schedule<TParam>(TParam param)
        where TParam : notnull;
}

public class RAction<TParam> where TParam : notnull
{
    public RAction.Invoke<TParam> Invoke { get; }
    public RAction.ReInvoke<TParam> ReInvoke { get; }
    public Schedule<TParam> Schedule { get; }
    
    public RAction(RAction.Invoke<TParam> invoke, RAction.ReInvoke<TParam> reInvoke, Schedule<TParam> schedule)
    {
        Invoke = invoke;
        ReInvoke = reInvoke;
        Schedule = schedule;
    }
} 

public class RAction<TParam, TScrapbook> 
    where TParam : notnull where TScrapbook : RScrapbook
{
    public RAction.Invoke<TParam> Invoke { get; }
    public RAction.ReInvoke<TParam, TScrapbook> ReInvoke { get; }
    public Schedule<TParam> Schedule { get; }
    
    public RAction(RAction.Invoke<TParam> invoke, RAction.ReInvoke<TParam, TScrapbook> reInvoke, Schedule<TParam> schedule)
    {
        Invoke = invoke;
        ReInvoke = reInvoke;
        Schedule = schedule;
    }
} 