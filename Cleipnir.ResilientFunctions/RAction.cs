using System.Collections.Generic;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;

namespace Cleipnir.ResilientFunctions;

public static class RAction
{
    public delegate Task<RResult> Invoke<TParam>(TParam param) where TParam : notnull;

    public delegate Task<RResult> ReInvoke(
        string functionInstanceId,
        IEnumerable<Status> expectedStatuses
    );
}

public class RAction<TParam> where TParam : notnull
{
    public RAction.Invoke<TParam> Invoke { get; }
    public RAction.ReInvoke ReInvoke { get; }
    public Schedule<TParam> Schedule { get; }
    
    public RAction(RAction.Invoke<TParam> invoke, RAction.ReInvoke reInvoke, Schedule<TParam> schedule)
    {
        Invoke = invoke;
        ReInvoke = reInvoke;
        Schedule = schedule;
    }
} 