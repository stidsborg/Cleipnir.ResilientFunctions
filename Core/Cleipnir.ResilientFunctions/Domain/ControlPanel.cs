using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Cleipnir.ResilientFunctions.Domain.Exceptions;
using Cleipnir.ResilientFunctions.Helpers;

namespace Cleipnir.ResilientFunctions.Domain;

public class ControlPanelFactory<TParam, TScrapbook> where TParam : notnull where TScrapbook : RScrapbook, new()
{
    private readonly FunctionTypeId _functionTypeId;
    private readonly InvocationHelper<TParam, TScrapbook, Unit> _invocationHelper;
    private readonly RAction.ReInvoke _reInvoke;
    private readonly RAction.ScheduleReInvoke _scheduleReInvoke;

    internal ControlPanelFactory(
        FunctionTypeId functionTypeId, 
        InvocationHelper<TParam, TScrapbook, Unit> invocationHelper, 
        RAction.ReInvoke reInvoke, 
        RAction.ScheduleReInvoke scheduleReInvoke)
    {
        _invocationHelper = invocationHelper;
        _reInvoke = reInvoke;
        _scheduleReInvoke = scheduleReInvoke;
        _functionTypeId = functionTypeId;
    }
    
    public async Task<ControlPanel<TParam, TScrapbook>?> For(FunctionInstanceId functionInstanceId)
    {
        var functionId = new FunctionId(_functionTypeId, functionInstanceId);
        var f = await _invocationHelper.GetFunction(functionId);
        if (f == null)
            return null;
        
        return new ControlPanel<TParam, TScrapbook>(
            _invocationHelper,
            _reInvoke,
            _scheduleReInvoke,
            functionId,
            f.Status,
            f.Epoch,
            f.Version,
            f.CrashedCheckFrequency,
            f.Param,
            f.Scrapbook,
            f.PostponedUntil,
            f.PreviouslyThrownException
        );
    }
}

public class ControlPanel<TParam, TScrapbook> where TParam : notnull where TScrapbook : RScrapbook, new()
{
    private readonly InvocationHelper<TParam, TScrapbook, Unit> _invocationHelper;
    private readonly RAction.ReInvoke _reInvoke;
    private readonly RAction.ScheduleReInvoke _scheduleReInvoke;
    
    internal ControlPanel(
        InvocationHelper<TParam, TScrapbook, Unit> invocationHelper,
        RAction.ReInvoke reInvoke,
        RAction.ScheduleReInvoke scheduleReInvoke,
        FunctionId functionId, 
        Status status, 
        int epoch, 
        int version,
        long crashedCheckFrequency,
        TParam param, 
        TScrapbook scrapbook, 
        DateTime? postponedUntil, 
        PreviouslyThrownException? previouslyThrownException)
    {
        _invocationHelper = invocationHelper;
        _reInvoke = reInvoke;
        _scheduleReInvoke = scheduleReInvoke;
        FunctionId = functionId;
        Status = status;
        Epoch = epoch;
        Version = version;
        CrashedCheckFrequency = crashedCheckFrequency;
        Param = param;
        Scrapbook = scrapbook;
        PostponedUntil = postponedUntil;
        PreviouslyThrownException = previouslyThrownException;
    }

    public FunctionId FunctionId { get; }
    public Status Status { get; private set; }
    
    public int Epoch { get; private set; }
    public int Version { get; private set; }
    public long CrashedCheckFrequency { get; private set; }
    
    public TParam Param { get; set; }
    public TScrapbook Scrapbook { get; set; }
    
    public DateTime? PostponedUntil { get; private set; }
    public PreviouslyThrownException? PreviouslyThrownException { get; private set; }

    public Task<bool> Succeed()
        => _invocationHelper.SetFunctionState(
            FunctionId, Status.Succeeded, Param, Scrapbook, postponeUntil: null, exception: null, Epoch
        );
    
    public Task<bool> Postpone(DateTime until) 
        => _invocationHelper.SetFunctionState(
            FunctionId, Status.Postponed, Param, Scrapbook, until, exception: null, Epoch
        );
    public Task<bool> Postpone(TimeSpan delay) => Postpone(DateTime.UtcNow + delay);
    
    public Task<bool> Fail(Exception exception) 
        => _invocationHelper.SetFunctionState(
            FunctionId, Status.Failed, Param, Scrapbook, postponeUntil: null, exception, Epoch
        );

    public async Task<bool> SaveParameterAndScrapbook() 
        => await _invocationHelper.SetParameterAndScrapbook(FunctionId, Param, Scrapbook, Epoch);

    public Task<bool> Delete() => _invocationHelper.Delete(FunctionId, Epoch);

    public async Task ReInvoke() => await _reInvoke(FunctionId.InstanceId.Value, Epoch);
    public async Task ScheduleReInvoke() => await _scheduleReInvoke(FunctionId.InstanceId.Value, Epoch);
    
    public async Task Refresh()
    {
        var sf = await _invocationHelper.GetFunction(FunctionId);
        if (sf == null)
            throw new UnexpectedFunctionState(FunctionId, $"Function '{FunctionId}' not found");

        Status = sf.Status;
        Epoch = sf.Epoch;
        Version = sf.Version;
        CrashedCheckFrequency = sf.CrashedCheckFrequency;
        Param = sf.Param;
        Scrapbook = sf.Scrapbook;
        PostponedUntil = sf.PostponedUntil;
        PreviouslyThrownException = sf.PreviouslyThrownException;
    }
}

public class ControlPanelFactory<TParam, TScrapbook, TReturn> where TParam : notnull where TScrapbook : RScrapbook, new()
{
    private readonly FunctionTypeId _functionTypeId;
    private readonly InvocationHelper<TParam, TScrapbook, TReturn> _invocationHelper;
    private readonly RFunc.ReInvoke<TReturn> _reInvoke;
    private readonly RFunc.ScheduleReInvoke _scheduleReInvoke;

    internal ControlPanelFactory(
        FunctionTypeId functionTypeId, 
        InvocationHelper<TParam, TScrapbook, TReturn> invocationHelper, 
        RFunc.ReInvoke<TReturn> reInvoke, 
        RFunc.ScheduleReInvoke scheduleReInvoke)
    {
        _invocationHelper = invocationHelper;
        _reInvoke = reInvoke;
        _scheduleReInvoke = scheduleReInvoke;
        _functionTypeId = functionTypeId;
    }

    public async Task<ControlPanel<TParam, TScrapbook, TReturn>?> For(FunctionInstanceId functionInstanceId)
    {
        var functionId = new FunctionId(_functionTypeId, functionInstanceId);
        var f = await _invocationHelper.GetFunction(functionId);
        if (f == null)
            return null;
        
        return new ControlPanel<TParam, TScrapbook, TReturn>(
            _invocationHelper,
            _reInvoke,
            _scheduleReInvoke,
            functionId,
            f.Status,
            f.Epoch,
            f.Version,
            f.CrashedCheckFrequency,
            f.Param,
            f.Scrapbook,
            f.Result,
            f.PostponedUntil,
            f.PreviouslyThrownException
        );
    }
}

public class ControlPanel<TParam, TScrapbook, TReturn> where TParam : notnull where TScrapbook : RScrapbook, new()
{
    private readonly InvocationHelper<TParam, TScrapbook, TReturn> _invocationHelper;
    private readonly RFunc.ReInvoke<TReturn> _reInvoke;
    private readonly RFunc.ScheduleReInvoke _scheduleReInvoke;

    internal ControlPanel(
        InvocationHelper<TParam, TScrapbook, TReturn> invocationHelper,
        RFunc.ReInvoke<TReturn> reInvoke,
        RFunc.ScheduleReInvoke scheduleReInvoke,
        FunctionId functionId, 
        Status status, 
        int epoch, 
        int version,
        long crashedCheckFrequency,
        TParam param, 
        TScrapbook scrapbook, 
        TReturn? result,
        DateTime? postponedUntil, 
        PreviouslyThrownException? previouslyThrownException)
    {
        _invocationHelper = invocationHelper;
        _reInvoke = reInvoke;
        _scheduleReInvoke = scheduleReInvoke;
        FunctionId = functionId;
        Status = status;
        Epoch = epoch;
        Version = version;
        CrashedCheckFrequency = crashedCheckFrequency;
        Param = param;
        Scrapbook = scrapbook;
        Result = result;
        PostponedUntil = postponedUntil;
        PreviouslyThrownException = previouslyThrownException;
    }

    public FunctionId FunctionId { get; }
    public Status Status { get; private set; }
    
    public int Epoch { get; private set; }
    public int Version { get; private set; }
    public long CrashedCheckFrequency { get; private set; }
    
    public TParam Param { get; set; }
    public TScrapbook Scrapbook { get; set; }
    public TReturn? Result { get; set; }
    
    public DateTime? PostponedUntil { get; private set; }
    public PreviouslyThrownException? PreviouslyThrownException { get; private set; }

    public Task<bool> Succeed(TReturn result)
        => _invocationHelper.SetFunctionState(
            FunctionId, Status.Succeeded, Param, Scrapbook, result, PostponedUntil, exception: null, Epoch
        );
    
    public Task<bool> Postpone(DateTime until) 
        => _invocationHelper.SetFunctionState(
            FunctionId, Status.Postponed, Param, Scrapbook, result: default, until, exception: null, Epoch
        );
    public Task<bool> Postpone(TimeSpan delay) => Postpone(DateTime.UtcNow + delay);
    
    public Task<bool> Fail(Exception exception) 
        => _invocationHelper.SetFunctionState(
            FunctionId, Status.Failed, Param, Scrapbook, result: default, postponeUntil: null, exception, Epoch
        );
    
    public Task<bool> SaveParameterAndScrapbook()
        => _invocationHelper.SetParameterAndScrapbook(FunctionId, Param, Scrapbook, Epoch);
    
    public Task<bool> Delete() => _invocationHelper.Delete(FunctionId, Epoch);

    public async Task<TReturn> ReInvoke() => await _reInvoke(FunctionId.InstanceId.Value, Epoch);
    public async Task ScheduleReInvoke() => await _scheduleReInvoke(FunctionId.InstanceId.Value, Epoch);
    
    public async Task Refresh()
    {
        var sf = await _invocationHelper.GetFunction(FunctionId);
        if (sf == null)
            throw new UnexpectedFunctionState(FunctionId, $"Function '{FunctionId}' not found");

        Status = sf.Status;
        Epoch = sf.Epoch;
        Version = sf.Version;
        CrashedCheckFrequency = sf.CrashedCheckFrequency;
        Param = sf.Param;
        Scrapbook = sf.Scrapbook;
        Result = sf.Result;
        PostponedUntil = sf.PostponedUntil;
        PreviouslyThrownException = sf.PreviouslyThrownException;
    }
}