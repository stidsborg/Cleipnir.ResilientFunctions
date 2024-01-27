using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Messaging;

namespace Cleipnir.ResilientFunctions.CoreRuntime.Invocation;

public class Context : IDisposable
{
    public FunctionId FunctionId { get; }
    public Messages Messages { get; }
    public Activities Activities { get; }
    public Utilities Utilities { get; }
    
    public Context(FunctionId functionId, Messages messages, Activities activities, Utilities utilities)
    {
        FunctionId = functionId;
        Utilities = utilities;
        Messages = messages;
        Activities = activities;
    }

    public void Deconstruct(out Activities activities, out Messages messages)
    {
        activities = Activities;
        messages = Messages;
    }
    
    public void Dispose() => Messages.Dispose();

    #region StartChild Methods

    public Task<TReturn> StartChild<TParam, TScrapbook, TReturn>(
        FuncRegistration<TParam, TScrapbook, TReturn> registration,
        string instanceId,
        TParam param
    ) where TScrapbook : RScrapbook, new() where TParam : notnull
        => ChildInvocation.StartChild(registration, instanceId, param, parentId: FunctionId, Messages);
    
    public Task<TReturn> StartChild<TParam, TReturn>(
        FuncRegistration<TParam, TReturn> registration,
        string instanceId,
        TParam param
    ) where TParam : notnull
        => ChildInvocation.StartChild(registration, instanceId, param, parentId: FunctionId, Messages);
    
    public Task StartChild<TParam>(
        ActionRegistration<TParam> registration,
        string instanceId,
        TParam param
    ) where TParam : notnull
        => ChildInvocation.StartChild(registration, instanceId, param, parentId: FunctionId, Messages);
    
    public Task StartChild<TParam, TScrapbook>(
        RAction<TParam, TScrapbook> registration,
        string instanceId,
        TParam param,
        FunctionId parentId,
        Messages messages
    ) where TScrapbook : RScrapbook, new() where TParam : notnull
        => ChildInvocation.StartChild(registration, instanceId, param, parentId: FunctionId, Messages);

    #endregion
}