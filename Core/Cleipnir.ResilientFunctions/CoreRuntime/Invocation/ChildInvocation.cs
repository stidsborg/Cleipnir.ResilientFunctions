using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Messaging;
using Cleipnir.ResilientFunctions.Reactive.Extensions;

namespace Cleipnir.ResilientFunctions.CoreRuntime.Invocation;

internal static class ChildInvocation
{
    public static async Task<TReturn> StartChild<TParam, TScrapbook, TReturn>(
        RFunc<TParam, TScrapbook, TReturn> registration,
        string instanceId,
        TParam param,
        FunctionId parentId,
        Messages messages
    ) where TScrapbook : RScrapbook, new() where TParam : notnull
    {
        var childId = new FunctionId(registration.TypeId, instanceId);
        var childStarted = messages
            .Existing
            .OfType<FunctionStarted>()
            .Any(msg => msg.FunctionId == childId);

        if (!childStarted)
        {
            await registration.Schedule(instanceId, param, sendResultTo: parentId);
            await messages.AppendMessage(new FunctionStarted(childId), $"FunctionStarted¤{childId}");
        }
        
        var functionCompletion = await messages
            .OfType<FunctionCompletion<TReturn>>()
            .Where(msg => msg.Sender.TypeId == registration.TypeId && msg.Sender.InstanceId == instanceId)
            .SuspendUntilFirst();

        return functionCompletion.Result;
    }
    
    public static async Task<TReturn> StartChild<TParam, TReturn>(
        RFunc<TParam, TReturn> registration,
        string instanceId,
        TParam param,
        FunctionId parentId,
        Messages messages
    ) where TParam : notnull
    {
        var childId = new FunctionId(registration.TypeId, instanceId);
        var childStarted = messages
            .Existing
            .OfType<FunctionStarted>()
            .Any(msg => msg.FunctionId == childId);

        if (!childStarted)
        {
            await registration.Schedule(instanceId, param, sendResultTo: parentId);
            await messages.AppendMessage(new FunctionStarted(childId), $"FunctionStarted¤{childId}");
        }
        
        var functionCompletion = await messages
            .OfType<FunctionCompletion<TReturn>>()
            .Where(msg => msg.Sender == childId)
            .SuspendUntilFirst();

        return functionCompletion.Result;
    }
    
    public static async Task StartChild<TParam>(
        RAction<TParam> registration,
        string instanceId,
        TParam param,
        FunctionId parentId,
        Messages messages
    ) where TParam : notnull
    {
        var childId = new FunctionId(registration.TypeId, instanceId);
        var childStarted = messages
            .Existing
            .OfType<FunctionStarted>()
            .Any(msg => msg.FunctionId == childId);

        if (!childStarted)
        {
            await registration.Schedule(instanceId, param, sendResultTo: parentId);
            await messages.AppendMessage(new FunctionStarted(childId), $"FunctionStarted¤{childId}");
        }
        
        await messages
            .OfType<FunctionCompletion>()
            .Where(msg => msg.Sender == childId)
            .SuspendUntilFirst();
    }
    
    public static async Task StartChild<TParam, TScrapbook>(
        RAction<TParam, TScrapbook> registration,
        string instanceId,
        TParam param,
        FunctionId parentId,
        Messages messages
    ) where TScrapbook : RScrapbook, new() where TParam : notnull
    {
        var childId = new FunctionId(registration.TypeId, instanceId);
        var childStarted = messages
            .Existing
            .OfType<FunctionStarted>()
            .Any(msg => msg.FunctionId == childId);
        
        if (!childStarted)
        {
            await registration.Schedule(instanceId, param, sendResultTo: parentId);
            await messages.AppendMessage(new FunctionStarted(childId), $"FunctionStarted¤{childId}");
        }
        
        await messages
            .OfType<FunctionCompletion>()
            .Where(msg => msg.Sender == childId)
            .SuspendUntilFirst();
    }
}