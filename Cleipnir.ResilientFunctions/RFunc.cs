using System.Collections.Generic;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;

namespace Cleipnir.ResilientFunctions;

public static class RFunc
{
    public delegate Task<RResult<TReturn>> Invoke<in TParam, TReturn>(string functionInstanceId, TParam param)
        where TParam : notnull;

    public delegate Task<RResult<TReturn>> ReInvoke<TReturn>(
        string functionInstanceId,
        IEnumerable<Status> expectedStatuses
    );
}

public class RFunc<TParam, TReturn> where TParam : notnull
{
    public RFunc.Invoke<TParam, TReturn> Invoke { get; }
    public RFunc.ReInvoke<TReturn> ReInvoke { get; }
    public Schedule<TParam> Schedule { get; }
    
    public RFunc(RFunc.Invoke<TParam, TReturn> invoke, RFunc.ReInvoke<TReturn> reInvoke, Schedule<TParam> schedule)
    {
        Invoke = invoke;
        ReInvoke = reInvoke;
        Schedule = schedule;
    }
} 
    