﻿using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Cleipnir.ResilientFunctions.Domain.Exceptions;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.Domain;

public class ControlPanel : BaseControlPanel<Unit, Unit>  
{
    internal ControlPanel(
        Invoker<Unit, Unit> invoker, 
        InvocationHelper<Unit, Unit> invocationHelper, 
        FlowId flowId, StoredId storedId,
        Status status, int epoch, long expires,  
        ExistingEffects effects,
        ExistingStates states, ExistingMessages messages, ExistingRegisteredTimeouts registeredTimeouts, Correlations correlations,
        PreviouslyThrownException? previouslyThrownException
    ) : base(
        invoker, invocationHelper, 
        flowId, storedId, status, epoch, 
        expires, innerParam: Unit.Instance, innerResult: Unit.Instance, effects,
        states, messages, registeredTimeouts, correlations, previouslyThrownException
    ) { }
    
    public Task Succeed() => InnerSucceed(result: Unit.Instance);
}

public class ControlPanel<TParam> : BaseControlPanel<TParam, Unit> where TParam : notnull  
{
    internal ControlPanel(
        Invoker<TParam, Unit> invoker, 
        InvocationHelper<TParam, Unit> invocationHelper, 
        FlowId flowId, StoredId storedId,
        Status status, int epoch, long expires, TParam innerParam, 
        ExistingEffects effects,
        ExistingStates states, ExistingMessages messages, ExistingRegisteredTimeouts registeredTimeouts, Correlations correlations, 
        PreviouslyThrownException? previouslyThrownException
    ) : base(
        invoker, invocationHelper, 
        flowId, storedId, status, epoch, 
        expires, innerParam, innerResult: Unit.Instance, effects,
        states, messages, registeredTimeouts, correlations, previouslyThrownException
    ) { }
    
    public TParam Param
    {
        get => InnerParam;
        set => InnerParam = value;
    }
    
    public Task Succeed() => InnerSucceed(result: Unit.Instance);
}

public class ControlPanel<TParam, TReturn> : BaseControlPanel<TParam, TReturn> where TParam : notnull
{
    internal ControlPanel(
        Invoker<TParam, TReturn> invoker, 
        InvocationHelper<TParam, TReturn> invocationHelper, 
        FlowId flowId, StoredId storedId, Status status, int epoch, 
        long expires, TParam innerParam, 
        TReturn? innerResult, 
        ExistingEffects effects, ExistingStates states, ExistingMessages messages, 
        ExistingRegisteredTimeouts registeredTimeouts, Correlations correlations, PreviouslyThrownException? previouslyThrownException
    ) : base(
        invoker, invocationHelper, 
        flowId, storedId, status, epoch, expires, 
        innerParam, innerResult, effects, states, messages, 
        registeredTimeouts, correlations, previouslyThrownException
    ) { }

    public Task Succeed(TReturn result) => InnerSucceed(result);
    public TReturn? Result => InnerResult;

    public TParam Param
    {
        get => InnerParam;
        set => InnerParam = value;
    }
}

public abstract class BaseControlPanel<TParam, TReturn> 
{
    private readonly Invoker<TParam, TReturn> _invoker;
    private readonly InvocationHelper<TParam, TReturn> _invocationHelper;
    private bool _innerParamChanged;

    internal BaseControlPanel(
        Invoker<TParam, TReturn> invoker, 
        InvocationHelper<TParam, TReturn> invocationHelper,
        FlowId flowId, 
        StoredId storedId,
        Status status, 
        int epoch,
        long expires,
        TParam innerParam, 
        TReturn? innerResult,
        ExistingEffects effects,
        ExistingStates states,
        ExistingMessages messages,
        ExistingRegisteredTimeouts registeredTimeouts,
        Correlations correlations,
        PreviouslyThrownException? previouslyThrownException)
    {
        _invoker = invoker;
        _invocationHelper = invocationHelper;
        FlowId = flowId;
        StoredId = storedId;
        Status = status;
        Epoch = epoch;
        LeaseExpiration = expires == long.MaxValue
            ? DateTime.MaxValue 
            : new DateTime(expires, DateTimeKind.Utc);
        
        _innerParam = innerParam;
        InnerResult = innerResult;
        PostponedUntil = Status == Status.Postponed ? LeaseExpiration : null;
        Effects = effects;
        States = states;
        Messages = messages;
        RegisteredTimeouts = registeredTimeouts;
        Correlations = correlations;
        PreviouslyThrownException = previouslyThrownException;
    }

    public FlowId FlowId { get; }
    public StoredId StoredId { get; }
    public Status Status { get; private set; }
    
    public int Epoch { get; private set; }
    public DateTime LeaseExpiration { get; private set; }
    
    public ExistingMessages Messages { get; private set; }
    
    public ExistingEffects Effects { get; private set; }

    public ExistingStates States { get; private set; }
    public Correlations Correlations { get; private set; }

    public ExistingRegisteredTimeouts RegisteredTimeouts { get; private set; }

    private TParam _innerParam;
    protected TParam InnerParam
    {
        get
        {
            _innerParamChanged = true;
            return _innerParam;
        }
        set
        {
            _innerParamChanged = true;
            _innerParam = value;
        }
    }

    protected TReturn? InnerResult { get; set; }
    public DateTime? PostponedUntil { get; private set; }
    public PreviouslyThrownException? PreviouslyThrownException { get; private set; }

    protected async Task InnerSucceed(TReturn? result)
    {
        var success = await _invocationHelper.SetFunctionState(
            StoredId, Status.Succeeded, 
            InnerParam, 
            result, 
            expires: long.MaxValue, 
            exception: null, 
            Epoch
        );

        if (!success)
            throw UnexpectedStateException.ConcurrentModification(FlowId);
        
        Epoch++;
        Status = Status.Succeeded;
        _innerParamChanged = false;
    }
    
    public async Task Postpone(DateTime until)
    {
        var success = await _invocationHelper.SetFunctionState(
            StoredId, Status.Postponed, 
            InnerParam,  
            result: default, 
            expires: until.Ticks, 
            exception: null, 
            Epoch
        );

        if (!success)
            throw UnexpectedStateException.ConcurrentModification(FlowId);
        
        Epoch++;
        Status = Status.Postponed;
        PostponedUntil = until;
        _innerParamChanged = false;
    }
        
    public Task Postpone(TimeSpan delay) => Postpone(DateTime.UtcNow + delay);

    public async Task Fail(Exception exception)
    {
        var success = await _invocationHelper.SetFunctionState(
            StoredId, Status.Failed, 
            InnerParam,  
            result: default, expires: long.MaxValue, exception, 
            Epoch
        );

        if (!success)
            throw UnexpectedStateException.ConcurrentModification(FlowId);
        
        Epoch++;
        Status = Status.Failed;
        PreviouslyThrownException = new PreviouslyThrownException(exception.Message, exception.StackTrace, exception.GetType());
        _innerParamChanged = false;
    }

    public async Task SaveChanges()
    {
        var success = await _invocationHelper.SaveControlPanelChanges(StoredId, InnerParam, InnerResult, Epoch);
        if (!success)
            throw UnexpectedStateException.ConcurrentModification(FlowId);
        
        Epoch++;
        _innerParamChanged = false;
    }
    
    public Task Delete() => _invocationHelper.Delete(StoredId);

    public async Task<TReturn> Restart()
    {
        if (_innerParamChanged)
            await SaveChanges();

        return await _invoker.Restart(FlowId.Instance.ToStoredInstance(), Epoch);   
    }
    public async Task ScheduleRestart()
    {
        if (_innerParamChanged)
            await SaveChanges();

        await _invoker.ScheduleRestart(FlowId.Instance.ToStoredInstance(), Epoch);
    }
    
    public async Task Refresh()
    {
        var sf = await _invocationHelper.GetFunction(StoredId);
        if (sf == null)
            throw UnexpectedStateException.NotFound(FlowId);

        Status = sf.Status;
        Epoch = sf.Epoch;
        LeaseExpiration = sf.Expires == long.MaxValue
            ? DateTime.MaxValue 
            : new DateTime(sf.Expires, DateTimeKind.Utc);
        InnerParam = sf.Param!;
        InnerResult = sf.Result;
        PostponedUntil = Status == Status.Postponed ? LeaseExpiration : null;
        PreviouslyThrownException = sf.PreviouslyThrownException;
        Effects = _invocationHelper.CreateExistingEffects(FlowId);
        Messages = _invocationHelper.CreateExistingMessages(FlowId);
        States = _invocationHelper.CreateExistingStates(FlowId);
        RegisteredTimeouts = _invocationHelper.CreateExistingTimeouts(FlowId);
        Correlations = _invocationHelper.CreateCorrelations(FlowId);

        _innerParamChanged = false;
    }
    
    public async Task<TReturn> WaitForCompletion(bool allowPostponeAndSuspended = false, TimeSpan? maxWait = null) 
        => await _invocationHelper.WaitForFunctionResult(FlowId, StoredId, allowPostponeAndSuspended, maxWait);
}