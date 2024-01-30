using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Messaging;

namespace Cleipnir.ResilientFunctions;

public static class FuncRegistration
{
    public delegate Task<TReturn> Invoke<in TParam, in TScrapbook, TReturn>(
        string functionInstanceId, 
        TParam param, 
        TScrapbook? scrapbook = null
    ) where TParam : notnull where TScrapbook : RScrapbook, new();
    
    public delegate Task Schedule<in TParam, TScrapbook>(
        string functionInstanceId, 
        TParam param, 
        TScrapbook? scrapbook = null
    ) where TParam : notnull where TScrapbook : RScrapbook, new();
    
    public delegate Task ScheduleAt<in TParam, TScrapbook>(
        string functionInstanceId, 
        TParam param,
        DateTime delayUntil,
        TScrapbook? scrapbook = null
    ) where TParam : notnull where TScrapbook : RScrapbook, new();
}

public class FuncRegistration<TParam, TReturn> where TParam : notnull
{
    public FunctionTypeId TypeId { get; }
    
    public FuncRegistration.Invoke<TParam, RScrapbook, TReturn> Invoke { get; }
    public FuncRegistration.Schedule<TParam, RScrapbook> Schedule { get; }
    public FuncRegistration.ScheduleAt<TParam, RScrapbook> ScheduleAt { get; }
    private readonly FuncRegistration<TParam, RScrapbook, TReturn> _funcRegistration; 
    public MessageWriters MessageWriters { get; }

    public FuncRegistration(FuncRegistration<TParam, RScrapbook, TReturn> funcRegistration)
    {
        TypeId = funcRegistration.TypeId;
        
        _funcRegistration = funcRegistration;
        
        Invoke = funcRegistration.Invoke;
        Schedule = funcRegistration.Schedule;
        ScheduleAt = funcRegistration.ScheduleAt;
        MessageWriters = funcRegistration.MessageWriters;
    }

    public Task<ControlPanel<TParam, RScrapbook, TReturn>?> ControlPanel(FunctionInstanceId functionInstanceId)
        => _funcRegistration.ControlPanel(functionInstanceId);
    
    public Task ScheduleIn(
        string functionInstanceId,
        TParam param,
        TimeSpan delay,
        RScrapbook? scrapbook = null
    ) => ScheduleAt(
        functionInstanceId,
        param,
        delayUntil: DateTime.UtcNow.Add(delay),
        scrapbook
    );
}

public class FuncRegistration<TParam, TScrapbook, TReturn> where TParam : notnull where TScrapbook : RScrapbook, new()
{
    public FunctionTypeId TypeId { get; }
    
    public FuncRegistration.Invoke<TParam, TScrapbook, TReturn> Invoke { get; }
    public FuncRegistration.Schedule<TParam, TScrapbook> Schedule { get; }
    public FuncRegistration.ScheduleAt<TParam, TScrapbook> ScheduleAt { get; }
    private ControlPanels<TParam, TScrapbook, TReturn> ControlPanels { get; }
    public MessageWriters MessageWriters { get; }

    public FuncRegistration(
        FunctionTypeId functionTypeId,
        FuncRegistration.Invoke<TParam, TScrapbook, TReturn> invoke,
        FuncRegistration.Schedule<TParam, TScrapbook> schedule,
        FuncRegistration.ScheduleAt<TParam, TScrapbook> scheduleAt,
        ControlPanels<TParam, TScrapbook, TReturn> controlPanel, 
        MessageWriters messageWriters)
    {
        TypeId = functionTypeId;
        
        Invoke = invoke;
        Schedule = schedule;
        ScheduleAt = scheduleAt;

        ControlPanels = controlPanel;
        MessageWriters = messageWriters;
    }

    public Task<ControlPanel<TParam, TScrapbook, TReturn>?> ControlPanel(FunctionInstanceId functionInstanceId)
        => ControlPanels.For(functionInstanceId);
    
    public Task ScheduleIn(
        string functionInstanceId,
        TParam param,
        TimeSpan delay,
        TScrapbook? scrapbook = null
    ) => ScheduleAt(
        functionInstanceId,
        param,
        delayUntil: DateTime.UtcNow.Add(delay),
        scrapbook
    );
}