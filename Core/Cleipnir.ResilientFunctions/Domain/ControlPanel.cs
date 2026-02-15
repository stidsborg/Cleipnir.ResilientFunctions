using System;
using System.Diagnostics;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime;
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
        ReplicaId? ownerReplica,
        Status status, long expires,  
        ExistingEffects effects,
        ExistingMessages messages, ExistingSemaphores semaphores, 
        Correlations correlations,
        FatalWorkflowException? fatalWorkflowException,
        UtcNow utcNow
    ) : base(
        invoker, invocationHelper, 
        flowId, storedId, ownerReplica, status,
        expires, innerParam: Unit.Instance, innerResult: Unit.Instance, effects,
        messages, semaphores, correlations, fatalWorkflowException,
        utcNow
    ) { }

    public Task Succeed() => InnerSucceed(result: Unit.Instance);
    
    public async Task BusyWaitUntil(Func<ControlPanel, bool> predicate, TimeSpan? maxWait = null, TimeSpan? checkFrequency = null)
    {
        if (predicate(this))
            return;
        
        maxWait ??= TimeSpan.FromSeconds(10);
        checkFrequency ??= TimeSpan.FromMilliseconds(250);
        
        var stopWatch = Stopwatch.StartNew();
        do
        {
            await Task.Delay(checkFrequency.Value);
            await Refresh();
            if (predicate(this))
                return;
        } while (stopWatch.Elapsed < maxWait);
        
        throw new TimeoutException("Predicate was not meet before max wait for reached");
    }
}

public class ControlPanel<TParam> : BaseControlPanel<TParam, Unit> where TParam : notnull  
{
    internal ControlPanel(
        Invoker<TParam, Unit> invoker, 
        InvocationHelper<TParam, Unit> invocationHelper, 
        FlowId flowId, StoredId storedId,
        ReplicaId? ownerReplica,
        Status status, long expires, TParam innerParam, 
        ExistingEffects effects,
        ExistingMessages messages, ExistingSemaphores semaphores, 
        Correlations correlations, 
        FatalWorkflowException? fatalWorkflowException,
        UtcNow utcNow
    ) : base(
        invoker, invocationHelper, 
        flowId, storedId, ownerReplica, status,
        expires, innerParam, innerResult: Unit.Instance, effects,
        messages, semaphores, correlations, fatalWorkflowException,
        utcNow
    ) { }
    
    public TParam Param
    {
        get => InnerParam;
        set => InnerParam = value;
    }
    
    public Task Succeed() => InnerSucceed(result: Unit.Instance);
    
    public async Task BusyWaitUntil(Func<ControlPanel<TParam>, bool> predicate, TimeSpan? maxWait = null, TimeSpan? checkFrequency = null)
    {
        if (predicate(this))
            return;
        
        maxWait ??= TimeSpan.FromSeconds(10);
        checkFrequency ??= TimeSpan.FromMilliseconds(250);
        
        var stopWatch = Stopwatch.StartNew();
        do
        {
            await Task.Delay(checkFrequency.Value);
            await Refresh();
            if (predicate(this))
                return;
        } while (stopWatch.Elapsed < maxWait);
        
        throw new TimeoutException("Predicate was not meet before max wait for reached");
    }
}

public class ControlPanel<TParam, TReturn> : BaseControlPanel<TParam, TReturn> where TParam : notnull
{
    internal ControlPanel(
        Invoker<TParam, TReturn> invoker, 
        InvocationHelper<TParam, TReturn> invocationHelper, 
        FlowId flowId, StoredId storedId, ReplicaId? ownerReplica, Status status,
        long expires, TParam innerParam, 
        TReturn? innerResult, 
        ExistingEffects effects, ExistingMessages messages, ExistingSemaphores semaphores,
        Correlations correlations, FatalWorkflowException? fatalWorkflowException,
        UtcNow utcNow
    ) : base(
        invoker, invocationHelper,
        flowId, storedId, ownerReplica, status, expires,
        innerParam, innerResult, effects, messages,
        semaphores, correlations, fatalWorkflowException,
        utcNow
    ) { }

    public Task Succeed(TReturn result) => InnerSucceed(result);
    public TReturn? Result => InnerResult;

    public TParam Param
    {
        get => InnerParam;
        set => InnerParam = value;
    }
    
    public async Task BusyWaitUntil(Func<ControlPanel<TParam, TReturn>, bool> predicate, TimeSpan? maxWait = null, TimeSpan? checkFrequency = null)
    {
        if (predicate(this))
            return;
        
        maxWait ??= TimeSpan.FromSeconds(10);
        checkFrequency ??= TimeSpan.FromMilliseconds(250);
        
        var stopWatch = Stopwatch.StartNew();
        do
        {
            await Task.Delay(checkFrequency.Value);
            await Refresh();
            if (predicate(this))
                return;
        } while (stopWatch.Elapsed < maxWait);
        
        throw new TimeoutException("Predicate was not meet before max wait for reached");
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
        ReplicaId? ownerReplica,
        Status status, 
        long expires,
        TParam innerParam, 
        TReturn? innerResult,
        ExistingEffects effects,
        ExistingMessages messages,
        ExistingSemaphores semaphores,
        Correlations correlations,
        FatalWorkflowException? fatalWorkflowException,
        UtcNow utcNow)
    {
        _invoker = invoker;
        _invocationHelper = invocationHelper;
        FlowId = flowId;
        StoredId = storedId;
        OwnerReplica = ownerReplica;
        Status = status;
        _innerParam = innerParam;
        InnerResult = innerResult;
        PostponedUntil = Status == Status.Postponed ? 
            (expires == long.MaxValue
                ? DateTime.MaxValue 
                : new DateTime(expires, DateTimeKind.Utc)) : null;
        Effects = effects;
        Messages = messages;
        Semaphores = semaphores;
        Correlations = correlations;
        FatalWorkflowException = fatalWorkflowException;
        UtcNow = utcNow;
    }

    public FlowId FlowId { get; }
    public StoredId StoredId { get; }
    public ReplicaId? OwnerReplica { get; }
    public Status Status { get; private set; }
    protected UtcNow UtcNow { get; }
    
    public ExistingMessages Messages { get; private set; }
    
    public ExistingEffects Effects { get; private set; }
    
    public ExistingSemaphores Semaphores { get; private set; }
    public Correlations Correlations { get; private set; }

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
    public FatalWorkflowException? FatalWorkflowException { get; private set; }

    protected async Task InnerSucceed(TReturn? result)
    {
        var success = await _invocationHelper.SetFunctionState(
            StoredId, Status.Succeeded, 
            InnerParam, 
            result, 
            expires: long.MaxValue, 
            exception: null, 
            OwnerReplica
        );

        if (!success)
            throw UnexpectedStateException.ConcurrentModification(FlowId);
        
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
            OwnerReplica
        );

        if (!success)
            throw UnexpectedStateException.ConcurrentModification(FlowId);
        
        Status = Status.Postponed;
        PostponedUntil = until;
        _innerParamChanged = false;
    }
        
    public Task Postpone(TimeSpan delay) => Postpone(UtcNow() + delay);

    public async Task Fail(Exception exception)
    {
        var fatalWorkflowException = FatalWorkflowException.CreateNonGeneric(FlowId, exception);
        var success = await _invocationHelper.SetFunctionState(
            StoredId, Status.Failed, 
            InnerParam,  
            result: default, expires: long.MaxValue, FatalWorkflowException.CreateNonGeneric(FlowId, exception), 
            OwnerReplica
        );

        if (!success)
            throw UnexpectedStateException.ConcurrentModification(FlowId);
        
        Status = Status.Failed;
        FatalWorkflowException = fatalWorkflowException;
        _innerParamChanged = false;
    }

    public async Task SaveChanges()
    {
        var success = await _invocationHelper.SaveControlPanelChanges(StoredId, InnerParam, InnerResult, OwnerReplica);
        if (!success)
            throw UnexpectedStateException.ConcurrentModification(FlowId);
        
        _innerParamChanged = false;
    }
    
    public Task Delete() => _invocationHelper.Delete(StoredId);

    /// <summary>
    /// Clear existing failed effects and retry information.
    /// </summary>
    public async Task ClearFailures()
    {
        await Effects.RemoveFailed();

        await Refresh();
    }

    /// <summary>
    /// Schedule invocation immediately
    /// </summary>
    /// <param name="clearFailures">Clear existing failed effects and retry information</param>
    /// <param name="refresh">Refresh control panel after invocation</param>
    public async Task<Scheduled<TReturn>> ScheduleRestart(bool clearFailures = false, bool refresh = true)
    {
        if (clearFailures)
            await ClearFailures();
            
        if (_innerParamChanged)
            await SaveChanges();

        var innerScheduled = await _invoker.ScheduleRestart(StoredId);

        if (refresh)
            await Refresh();

        return innerScheduled.ToScheduledWithResult();
    }
    
    public async Task Refresh()
    {
        var sf = await _invocationHelper.GetFunction(StoredId, FlowId);
        if (sf == null)
            throw UnexpectedStateException.NotFound(FlowId);

        Status = sf.Status;
        InnerParam = sf.Param!;
        InnerResult = sf.Result;
        PostponedUntil = Status == Status.Postponed ? 
            (sf.Expires == long.MaxValue
                ? DateTime.MaxValue 
                : new DateTime(sf.Expires, DateTimeKind.Utc))
            : null;
        FatalWorkflowException = sf.FatalWorkflowException;
        Effects = await _invocationHelper.CreateExistingEffects(FlowId);
        Messages = _invocationHelper.CreateExistingMessages(FlowId);
        Correlations = _invocationHelper.CreateCorrelations(FlowId);
        Semaphores = await _invocationHelper.CreateExistingSemaphores(FlowId);

        _innerParamChanged = false;
    }

    public async Task<TReturn> WaitForCompletion(bool allowPostponeAndSuspended = false, TimeSpan? maxWait = null)
    {
        var result = await _invocationHelper.WaitForFunctionResult(FlowId, StoredId, allowPostponeAndSuspended, maxWait);
        await Refresh();
        return result;
    }
}