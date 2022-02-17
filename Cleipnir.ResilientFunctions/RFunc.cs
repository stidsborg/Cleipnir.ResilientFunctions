using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions;

public static class RFunc
{
    public delegate Task<RResult<TReturn>> Invoke<TParam, TReturn>(TParam param)
        where TParam : notnull;

    public delegate Task<RResult<TReturn>> ReInvoke<TParam, TReturn>(
        string functionInstanceId,
        Action<TParam> initializer,
        IEnumerable<Status> expectedStatuses
    ) where TParam : notnull;
    public delegate Task<RResult<TReturn>> ReInvoke<TParam, TScrapbook, TReturn>(
        string functionInstanceId,
        Action<TParam, TScrapbook> initializer,
        IEnumerable<Status> expectedStatuses
    ) where TParam : notnull where TScrapbook : RScrapbook;
    public delegate Task Schedule<TParam>(TParam param)
         where TParam : notnull;
}

public class RFunc<TParam, TReturn> where TParam : notnull
{
    public RFunc.Invoke<TParam, TReturn> Invoke { get; }
    public RFunc.ReInvoke<TParam, TReturn> ReInvoke { get; }
    public Schedule<TParam> Schedule { get; }
    
    public RFunc(RFunc.Invoke<TParam, TReturn> invoke, RFunc.ReInvoke<TParam, TReturn> reInvoke, Schedule<TParam> schedule)
    {
        Invoke = invoke;
        ReInvoke = reInvoke;
        Schedule = schedule;
    }
} 

public class RFunc<TParam, TScrapbook, TReturn> 
    where TParam : notnull where TScrapbook : RScrapbook
{
    public RFunc.Invoke<TParam, TReturn> Invoke { get; }
    public RFunc.ReInvoke<TParam, TScrapbook, TReturn> ReInvoke { get; }
    public Schedule<TParam> Schedule { get; }
    
    public RFunc(RFunc.Invoke<TParam, TReturn> invoke, RFunc.ReInvoke<TParam, TScrapbook, TReturn> reInvoke, Schedule<TParam> schedule)
    {
        Invoke = invoke;
        ReInvoke = reInvoke;
        Schedule = schedule;
    }
} 
    