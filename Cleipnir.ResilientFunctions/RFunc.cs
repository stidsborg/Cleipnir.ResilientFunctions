using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions;

public static class RFunc
{
    public delegate Task<RResult<TResult>> Invoke<TParam, TResult>(TParam param)
        where TParam : notnull;

    public delegate Task<RResult<TResult>> ReInvoke<TParam, TResult>(
        string functionInstanceId,
        Action<TParam> initializer,
        IEnumerable<Status> expectedStatuses
    ) where TParam : notnull;
    public delegate Task<RResult<TResult>> ReInvoke<TParam, TScrapbook, TResult>(
        string functionInstanceId,
        Action<TParam, TScrapbook> initializer,
        IEnumerable<Status> expectedStatuses
    ) where TParam : notnull where TScrapbook : RScrapbook;
    public delegate Task Schedule<TParam>(TParam param)
         where TParam : notnull;
}

public class RFunc<TParam, TResult> where TParam : notnull
{
    public RFunc.Invoke<TParam, TResult> Invoke { get; }
    public RFunc.ReInvoke<TParam, TResult> ReInvoke { get; }
    public Schedule<TParam> Schedule { get; }
    
    public RFunc(RFunc.Invoke<TParam, TResult> invoke, RFunc.ReInvoke<TParam, TResult> reInvoke, Schedule<TParam> schedule)
    {
        Invoke = invoke;
        ReInvoke = reInvoke;
        Schedule = schedule;
    }
} 

public class RFunc<TParam, TScrapbook, TResult> 
    where TParam : notnull where TScrapbook : RScrapbook
{
    public RFunc.Invoke<TParam, TResult> Invoke { get; }
    public RFunc.ReInvoke<TParam, TScrapbook, TResult> ReInvoke { get; }
    public Schedule<TParam> Schedule { get; }
    
    public RFunc(RFunc.Invoke<TParam, TResult> invoke, RFunc.ReInvoke<TParam, TScrapbook, TResult> reInvoke, Schedule<TParam> schedule)
    {
        Invoke = invoke;
        ReInvoke = reInvoke;
        Schedule = schedule;
    }
} 
    