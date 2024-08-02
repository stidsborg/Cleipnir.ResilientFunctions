using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Cleipnir.ResilientFunctions.Domain.Exceptions;
using Cleipnir.ResilientFunctions.Helpers;

namespace Cleipnir.ResilientFunctions.Domain;

public class ControlPanel : BaseControlPanel<Unit, Unit>  
{
    internal ControlPanel(
        Invoker<Unit, Unit> invoker, 
        InvocationHelper<Unit, Unit> invocationHelper, 
        FlowId flowId, 
        Status status, int epoch, long leaseExpiration,  
        DateTime? postponedUntil, 
        ExistingStates states, ExistingMessages messages, ExistingTimeouts timeouts, Correlations correlations,
        PreviouslyThrownException? previouslyThrownException
    ) : base(
        invoker, invocationHelper, flowId, status, epoch, 
        leaseExpiration, innerParam: Unit.Instance, innerResult: Unit.Instance, postponedUntil, 
        states, messages, timeouts, correlations, previouslyThrownException
    ) { }
    
    public Task Succeed() => InnerSucceed(result: Unit.Instance);
}

public class ControlPanel<TParam> : BaseControlPanel<TParam, Unit> where TParam : notnull  
{
    internal ControlPanel(
        Invoker<TParam, Unit> invoker, 
        InvocationHelper<TParam, Unit> invocationHelper, 
        FlowId flowId, 
        Status status, int epoch, long leaseExpiration, TParam innerParam, 
        DateTime? postponedUntil, 
        ExistingStates states, ExistingMessages messages, ExistingTimeouts timeouts, Correlations correlations, 
        PreviouslyThrownException? previouslyThrownException
    ) : base(
        invoker, invocationHelper, flowId, status, epoch, 
        leaseExpiration, innerParam, innerResult: Unit.Instance, postponedUntil, 
        states, messages, timeouts, correlations, previouslyThrownException
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
        FlowId flowId, Status status, int epoch, 
        long leaseExpiration, TParam innerParam, 
        TReturn? innerResult, 
        DateTime? postponedUntil, ExistingStates states, ExistingMessages messages, 
        ExistingTimeouts timeouts, Correlations correlations, PreviouslyThrownException? previouslyThrownException
    ) : base(
        invoker, invocationHelper, flowId, status, epoch, leaseExpiration, 
        innerParam, innerResult, postponedUntil, states, messages, 
        timeouts, correlations, previouslyThrownException
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
        Status status, 
        int epoch,
        long leaseExpiration,
        TParam innerParam, 
        TReturn? innerResult,
        DateTime? postponedUntil, 
        ExistingStates states,
        ExistingMessages messages,
        ExistingTimeouts timeouts,
        Correlations correlations,
        PreviouslyThrownException? previouslyThrownException)
    {
        _invoker = invoker;
        _invocationHelper = invocationHelper;
        FlowId = flowId;
        Status = status;
        Epoch = epoch;
        LeaseExpiration = new DateTime(leaseExpiration, DateTimeKind.Utc);
        
        _innerParam = innerParam;
        InnerResult = innerResult;
        PostponedUntil = postponedUntil;
        States = states;
        Messages = messages;
        Timeouts = timeouts;
        Correlations = correlations;
        PreviouslyThrownException = previouslyThrownException;
    }

    public FlowId FlowId { get; }
    public Status Status { get; private set; }
    
    public int Epoch { get; private set; }
    public DateTime LeaseExpiration { get; private set; }
    
    public ExistingMessages Messages { get; private set; }
    
    private Task<ExistingEffects>? _effects;
    public Task<ExistingEffects> Effects => _effects ??= _invocationHelper.CreateExistingEffects(FlowId); 

    public ExistingStates States { get; private set; }
    public Correlations Correlations { get; private set; }

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
            FlowId, Status.Succeeded, 
            InnerParam, 
            result, 
            PostponedUntil, exception: null, 
            Epoch
        );

        if (!success)
            throw new ConcurrentModificationException(FlowId);
        
        Epoch++;
        Status = Status.Succeeded;
        _innerParamChanged = false;
    }
    
    public async Task Postpone(DateTime until)
    {
        var success = await _invocationHelper.SetFunctionState(
            FlowId, Status.Postponed, 
            InnerParam,  
            result: default, until, 
            exception: null, 
            Epoch
        );

        if (!success)
            throw new ConcurrentModificationException(FlowId);
        
        Epoch++;
        Status = Status.Postponed;
        PostponedUntil = until;
        _innerParamChanged = false;
    }
        
    public Task Postpone(TimeSpan delay) => Postpone(DateTime.UtcNow + delay);

    public async Task Fail(Exception exception)
    {
        var success = await _invocationHelper.SetFunctionState(
            FlowId, Status.Failed, 
            InnerParam,  
            result: default, postponeUntil: null, exception, 
            Epoch
        );

        if (!success)
            throw new ConcurrentModificationException(FlowId);
        
        Epoch++;
        Status = Status.Failed;
        PreviouslyThrownException = new PreviouslyThrownException(exception.Message, exception.StackTrace, exception.GetType());
        _innerParamChanged = false;
    }

    public async Task SaveChanges()
    {
        var success = await _invocationHelper.SaveControlPanelChanges(FlowId, InnerParam, InnerResult, Epoch);
        if (!success)
            throw new ConcurrentModificationException(FlowId);
        
        Epoch++;
        _innerParamChanged = false;
    }
    
    public Task Delete() => _invocationHelper.Delete(FlowId);

    public async Task<TReturn> Restart()
    {
        if (_innerParamChanged)
            await SaveChanges();

        return await _invoker.Restart(FlowId.Instance.Value, Epoch);   
    }
    public async Task ScheduleRestart()
    {
        if (_innerParamChanged)
            await SaveChanges();

        await _invoker.ScheduleRestart(FlowId.Instance.Value, Epoch);
    }
    
    public async Task Refresh()
    {
        var sf = await _invocationHelper.GetFunction(FlowId);
        if (sf == null)
            throw new UnexpectedFunctionState(FlowId, $"Function '{FlowId}' not found");

        Status = sf.Status;
        Epoch = sf.Epoch;
        LeaseExpiration = new DateTime(sf.LeaseExpiration, DateTimeKind.Utc);
        InnerParam = sf.Param!;
        InnerResult = sf.Result;
        PostponedUntil = sf.PostponedUntil;
        PreviouslyThrownException = sf.PreviouslyThrownException;
        Messages = await _invocationHelper.GetExistingMessages(FlowId);
        States = await _invocationHelper.GetExistingStates(FlowId, sf.DefaultState);
        Timeouts = await _invocationHelper.GetExistingTimeouts(FlowId);
        Correlations = _invocationHelper.CreateCorrelations(FlowId);

        _innerParamChanged = false;
    }
    
    public async Task<TReturn> WaitForCompletion(bool allowPostponeAndSuspended = false) 
        => await _invocationHelper.WaitForFunctionResult(FlowId, allowPostponeAndSuspended);
}