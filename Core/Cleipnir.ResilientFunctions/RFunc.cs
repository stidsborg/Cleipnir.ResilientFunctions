using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Messaging;

namespace Cleipnir.ResilientFunctions;

public static class RFunc
{
    public delegate Task<TReturn> Invoke<in TParam, in TScrapbook, TReturn>(
        string functionInstanceId, 
        TParam param, 
        TScrapbook? scrapbook = null
    ) where TParam : notnull where TScrapbook : RScrapbook, new();
    
    public delegate Task Schedule<in TParam, TScrapbook>(
        string functionInstanceId, 
        TParam param, 
        TScrapbook? scrapbook = null,
        FunctionId? sendResultTo = null
    ) where TParam : notnull where TScrapbook : RScrapbook, new();
    
    public delegate Task ScheduleAt<in TParam, TScrapbook>(
        string functionInstanceId, 
        TParam param,
        DateTime delayUntil,
        TScrapbook? scrapbook = null,
        FunctionId? sendResultTo = null
    ) where TParam : notnull where TScrapbook : RScrapbook, new();
}

public class RFunc<TParam, TReturn> where TParam : notnull
{
    public FunctionTypeId TypeId { get; }
    
    public RFunc.Invoke<TParam, RScrapbook, TReturn> Invoke { get; }
    public RFunc.Schedule<TParam, RScrapbook> Schedule { get; }
    public RFunc.ScheduleAt<TParam, RScrapbook> ScheduleAt { get; }
    private readonly RFunc<TParam, RScrapbook, TReturn> _rFunc; 
    public MessageWriters MessageWriters { get; }

    public RFunc(RFunc<TParam, RScrapbook, TReturn> rFunc)
    {
        TypeId = rFunc.TypeId;
        
        _rFunc = rFunc;
        
        Invoke = rFunc.Invoke;
        Schedule = rFunc.Schedule;
        ScheduleAt = rFunc.ScheduleAt;
        MessageWriters = rFunc.MessageWriters;
    }

    public Task<ControlPanel<TParam, RScrapbook, TReturn>?> ControlPanel(FunctionInstanceId functionInstanceId)
        => _rFunc.ControlPanel(functionInstanceId);
    
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

public class RFunc<TParam, TScrapbook, TReturn> where TParam : notnull where TScrapbook : RScrapbook, new()
{
    public FunctionTypeId TypeId { get; }
    
    public RFunc.Invoke<TParam, TScrapbook, TReturn> Invoke { get; }
    public RFunc.Schedule<TParam, TScrapbook> Schedule { get; }
    public RFunc.ScheduleAt<TParam, TScrapbook> ScheduleAt { get; }
    private ControlPanels<TParam, TScrapbook, TReturn> ControlPanels { get; }
    public MessageWriters MessageWriters { get; }

    public RFunc(
        FunctionTypeId functionTypeId,
        RFunc.Invoke<TParam, TScrapbook, TReturn> invoke,
        RFunc.Schedule<TParam, TScrapbook> schedule,
        RFunc.ScheduleAt<TParam, TScrapbook> scheduleAt,
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