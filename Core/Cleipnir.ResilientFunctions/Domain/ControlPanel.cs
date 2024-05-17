using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Cleipnir.ResilientFunctions.Domain.Exceptions;
using Cleipnir.ResilientFunctions.Helpers;

namespace Cleipnir.ResilientFunctions.Domain;

public class ControlPanel<TParam> : BaseControlPanel<TParam, Unit> where TParam : notnull  
{
    internal ControlPanel(
        Invoker<TParam, Unit> invoker, 
        InvocationHelper<TParam, Unit> invocationHelper, 
        FunctionId functionId, 
        Status status, int epoch, long leaseExpiration, TParam innerParam, 
        DateTime? postponedUntil, ExistingEffects effects, 
        ExistingStates states, ExistingMessages messages, ExistingTimeouts timeouts, 
        PreviouslyThrownException? previouslyThrownException
    ) : base(
        invoker, invocationHelper, functionId, status, epoch, 
        leaseExpiration, innerParam, innerResult: null, postponedUntil, 
        effects, states, messages, timeouts, previouslyThrownException
    ) { }
    
    public TParam Param
    {
        get => InnerParam;
        set => InnerParam = value;
    }
    
    public Task Succeed() => InnerSucceed(result: null);
}

public class ControlPanel<TParam, TReturn> : BaseControlPanel<TParam, TReturn> where TParam : notnull
{
    internal ControlPanel(
        Invoker<TParam, TReturn> invoker, 
        InvocationHelper<TParam, TReturn> invocationHelper, 
        FunctionId functionId, Status status, int epoch, 
        long leaseExpiration, TParam innerParam, 
        TReturn? innerResult, 
        DateTime? postponedUntil, ExistingEffects effects, ExistingStates states, ExistingMessages messages, 
        ExistingTimeouts timeouts, PreviouslyThrownException? previouslyThrownException
    ) : base(
        invoker, invocationHelper, functionId, status, epoch, leaseExpiration, 
        innerParam, innerResult, postponedUntil, effects, states, messages, 
        timeouts, previouslyThrownException
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
        FunctionId functionId, 
        Status status, 
        int epoch,
        long leaseExpiration,
        TParam innerParam, 
        TReturn? innerResult,
        DateTime? postponedUntil, 
        ExistingEffects effects,
        ExistingStates states,
        ExistingMessages messages,
        ExistingTimeouts timeouts,
        PreviouslyThrownException? previouslyThrownException)
    {
        _invoker = invoker;
        _invocationHelper = invocationHelper;
        FunctionId = functionId;
        Status = status;
        Epoch = epoch;
        LeaseExpiration = new DateTime(leaseExpiration, DateTimeKind.Utc);
        
        _innerParam = innerParam;
        InnerResult = innerResult;
        PostponedUntil = postponedUntil;
        Effects = effects;
        States = states;
        Messages = messages;
        Timeouts = timeouts;
        PreviouslyThrownException = previouslyThrownException;
    }

    public FunctionId FunctionId { get; }
    public Status Status { get; private set; }
    
    public int Epoch { get; private set; }
    public DateTime LeaseExpiration { get; private set; }
    
    public ExistingMessages Messages { get; private set; }
    public ExistingEffects Effects { get; private set; } 
    public ExistingStates States { get; private set; }

    public ExistingTimeouts Timeouts { get; private set; }

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
            FunctionId, Status.Succeeded, 
            InnerParam, 
            result, 
            PostponedUntil, exception: null, 
            Epoch
        );

        if (!success)
            throw new ConcurrentModificationException(FunctionId);
        
        Epoch++;
        Status = Status.Succeeded;
        _innerParamChanged = false;
    }
    
    public async Task Postpone(DateTime until)
    {
        var success = await _invocationHelper.SetFunctionState(
            FunctionId, Status.Postponed, 
            InnerParam,  
            result: default, until, 
            exception: null, 
            Epoch
        );

        if (!success)
            throw new ConcurrentModificationException(FunctionId);
        
        Epoch++;
        Status = Status.Postponed;
        PostponedUntil = until;
        _innerParamChanged = false;
    }
        
    public Task Postpone(TimeSpan delay) => Postpone(DateTime.UtcNow + delay);

    public async Task Fail(Exception exception)
    {
        var success = await _invocationHelper.SetFunctionState(
            FunctionId, Status.Failed, 
            InnerParam,  
            result: default, postponeUntil: null, exception, 
            Epoch
        );

        if (!success)
            throw new ConcurrentModificationException(FunctionId);
        
        Epoch++;
        Status = Status.Failed;
        PreviouslyThrownException = new PreviouslyThrownException(exception.Message, exception.StackTrace, exception.GetType());
        _innerParamChanged = false;
    }

    public async Task SaveChanges()
    {
        var success = await _invocationHelper.SaveControlPanelChanges(FunctionId, InnerParam, InnerResult, Epoch);
        if (!success)
            throw new ConcurrentModificationException(FunctionId);
        
        Epoch++;
        _innerParamChanged = false;
    }
    
    public Task Delete() => _invocationHelper.Delete(FunctionId);

    public async Task<TReturn> ReInvoke()
    {
        if (_innerParamChanged)
            await SaveChanges();

        return await _invoker.ReInvoke(FunctionId.InstanceId.Value, Epoch);   
    }
    public async Task ScheduleReInvoke()
    {
        if (_innerParamChanged)
            await SaveChanges();

        await _invoker.ScheduleReInvoke(FunctionId.InstanceId.Value, Epoch);
    }
    
    public async Task Refresh()
    {
        var sf = await _invocationHelper.GetFunction(FunctionId);
        if (sf == null)
            throw new UnexpectedFunctionState(FunctionId, $"Function '{FunctionId}' not found");

        Status = sf.Status;
        Epoch = sf.Epoch;
        LeaseExpiration = new DateTime(sf.LeaseExpiration, DateTimeKind.Utc);
        InnerParam = sf.Param!;
        InnerResult = sf.Result;
        PostponedUntil = sf.PostponedUntil;
        PreviouslyThrownException = sf.PreviouslyThrownException;
        Messages = await _invocationHelper.GetExistingMessages(FunctionId);
        Effects = await _invocationHelper.GetExistingEffects(FunctionId);
        States = await _invocationHelper.GetExistingStates(FunctionId, sf.DefaultState);
        Timeouts = await _invocationHelper.GetExistingTimeouts(FunctionId); 

        _innerParamChanged = false;
    }
    
    public async Task<TReturn> WaitForCompletion(bool allowPostponeAndSuspended = false) 
        => await _invocationHelper.WaitForFunctionResult(FunctionId, allowPostponeAndSuspended);
}