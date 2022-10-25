﻿using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Cleipnir.ResilientFunctions.Helpers;

namespace Cleipnir.ResilientFunctions.Domain;

public class ControlPanelFactory<TParam, TScrapbook> where TParam : notnull where TScrapbook : RScrapbook, new()
{
    private readonly FunctionTypeId _functionTypeId;
    private readonly InvocationHelper<TParam, TScrapbook, Unit> _invocationHelper;

    internal ControlPanelFactory(FunctionTypeId functionTypeId, InvocationHelper<TParam, TScrapbook, Unit> invocationHelper)
    {
        _invocationHelper = invocationHelper;
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
            functionId,
            f.Status,
            f.Epoch,
            f.Version,
            f.CrashedCheckFrequency,
            f.Param,
            f.Scrapbook,
            f.PostponedUntil,
            f.Error
        );
    }
}

public class ControlPanel<TParam, TScrapbook> where TParam : notnull where TScrapbook : RScrapbook, new()
{
    private readonly InvocationHelper<TParam, TScrapbook, Unit> _invocationHelper;

    internal ControlPanel(
        InvocationHelper<TParam, TScrapbook, Unit> invocationHelper, 
        FunctionId functionId, 
        Status status, 
        int epoch, 
        int version,
        long crashedCheckFrequency,
        TParam param, 
        TScrapbook scrapbook, 
        DateTime? postponedUntil, 
        Exception? failedWithException)
    {
        _invocationHelper = invocationHelper;
        FunctionId = functionId;
        Status = status;
        Epoch = epoch;
        Version = version;
        CrashedCheckFrequency = crashedCheckFrequency;
        Param = param;
        Scrapbook = scrapbook;
        PostponedUntil = postponedUntil;
        FailedWithException = failedWithException;
    }

    public FunctionId FunctionId { get; }
    public Status Status { get; }
    
    public int Epoch { get; }
    public int Version { get; }
    public long CrashedCheckFrequency { get; }
    
    public TParam Param { get; set; }
    public TScrapbook Scrapbook { get; set; }
    
    public DateTime? PostponedUntil { get; }
    public Exception? FailedWithException { get; }

    public Task<bool> Succeed()
        => _invocationHelper.SetFunctionState(
            FunctionId, Status.Succeeded, Param, Scrapbook, PostponedUntil, FailedWithException, Epoch
        );
    
    public Task<bool> Postpone(DateTime until) 
        => _invocationHelper.SetFunctionState(
            FunctionId, Status.Postponed, Param, Scrapbook, until, FailedWithException, Epoch
        );
    public Task<bool> Postpone(TimeSpan delay) => Postpone(DateTime.UtcNow + delay);
    
    public Task<bool> Fail(Exception exception) 
        => _invocationHelper.SetFunctionState(
            FunctionId, Status.Failed, Param, Scrapbook, PostponedUntil, exception, Epoch
        );

    public Task<bool> SaveParameterAndScrapbook()
        => _invocationHelper.SetFunctionState(
            FunctionId, Status, Param, Scrapbook, PostponedUntil, FailedWithException, Epoch
        );
    
    public Task<bool> Delete() => _invocationHelper.Delete(FunctionId, Epoch);
}

public class ControlPanelFactory<TParam, TScrapbook, TReturn> where TParam : notnull where TScrapbook : RScrapbook, new()
{
    private readonly FunctionTypeId _functionTypeId;
    private readonly InvocationHelper<TParam, TScrapbook, TReturn> _invocationHelper;

    internal ControlPanelFactory(FunctionTypeId functionTypeId, InvocationHelper<TParam, TScrapbook, TReturn> invocationHelper)
    {
        _invocationHelper = invocationHelper;
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
            functionId,
            f.Status,
            f.Epoch,
            f.Version,
            f.CrashedCheckFrequency,
            f.Param,
            f.Scrapbook,
            f.Result,
            f.PostponedUntil,
            f.Error
        );
    }
}

public class ControlPanel<TParam, TScrapbook, TReturn> where TParam : notnull where TScrapbook : RScrapbook, new()
{
    private readonly InvocationHelper<TParam, TScrapbook, TReturn> _invocationHelper;

    internal ControlPanel(
        InvocationHelper<TParam, TScrapbook, TReturn> invocationHelper, 
        FunctionId functionId, 
        Status status, 
        int epoch, 
        int version,
        long crashedCheckFrequency,
        TParam param, 
        TScrapbook scrapbook, 
        TReturn? result,
        DateTime? postponedUntil, 
        Exception? failedWithException)
    {
        _invocationHelper = invocationHelper;
        FunctionId = functionId;
        Status = status;
        Epoch = epoch;
        Version = version;
        CrashedCheckFrequency = crashedCheckFrequency;
        Param = param;
        Scrapbook = scrapbook;
        Result = result;
        PostponedUntil = postponedUntil;
        FailedWithException = failedWithException;
    }

    public FunctionId FunctionId { get; }
    public Status Status { get; }
    
    public int Epoch { get; }
    public int Version { get; }
    public long CrashedCheckFrequency { get; }
    
    public TParam Param { get; set; }
    public TScrapbook Scrapbook { get; set; }
    public TReturn? Result { get; set; }
    
    public DateTime? PostponedUntil { get; }
    public Exception? FailedWithException { get; }

    public Task<bool> Succeed(TReturn result)
        => _invocationHelper.SetFunctionState(
            FunctionId, Status.Succeeded, Param, Scrapbook, result, PostponedUntil, FailedWithException, Epoch
        );
    
    public Task<bool> Postpone(DateTime until) 
        => _invocationHelper.SetFunctionState(
            FunctionId, Status.Postponed, Param, Scrapbook, result: default, until, FailedWithException, Epoch
        );
    public Task<bool> Postpone(TimeSpan delay) => Postpone(DateTime.UtcNow + delay);
    
    public Task<bool> Fail(Exception exception) 
        => _invocationHelper.SetFunctionState(
            FunctionId, Status.Failed, Param, Scrapbook, result: default, PostponedUntil, exception, Epoch
        );
    
    public Task<bool> SaveParameterAndScrapbook()
        => _invocationHelper.SetFunctionState(
            FunctionId, Status, Param, Scrapbook, PostponedUntil, FailedWithException, Epoch
        );
    
    public Task<bool> Delete() => _invocationHelper.Delete(FunctionId, Epoch);
}