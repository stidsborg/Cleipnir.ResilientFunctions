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
        InvocationHelper<Unit, Unit> invocationHelper,
        FlowId flowId, StoredId storedId,
        ReplicaId? ownerReplica,
        Status status, long expires,
        ExistingEffects effects,
        ExistingMessages messages,
        FatalWorkflowException? fatalWorkflowException,
        UtcNow utcNow
    ) : base(
        invocationHelper,
        flowId, storedId, ownerReplica, status,
        expires, innerParam: Unit.Instance, innerResult: Unit.Instance, effects,
        messages, fatalWorkflowException,
        utcNow
    ) { }

    public Task Succeed() => InnerSucceed(result: Unit.Instance);
    
    public Task BusyWaitUntil(Func<ControlPanel, bool> predicate, TimeSpan? maxWait = null, TimeSpan? checkFrequency = null)
        => BusyWaitUntil(() => predicate(this), maxWait, checkFrequency);
}

public class ControlPanel<TParam> : BaseControlPanel<TParam, Unit> where TParam : notnull  
{
    internal ControlPanel(
        InvocationHelper<TParam, Unit> invocationHelper,
        FlowId flowId, StoredId storedId,
        ReplicaId? ownerReplica,
        Status status, long expires, TParam innerParam,
        ExistingEffects effects,
        ExistingMessages messages,
        FatalWorkflowException? fatalWorkflowException,
        UtcNow utcNow
    ) : base(
        invocationHelper,
        flowId, storedId, ownerReplica, status,
        expires, innerParam, innerResult: Unit.Instance, effects,
        messages, fatalWorkflowException,
        utcNow
    ) { }
    
    public TParam Param
    {
        get => InnerParam;
        set => InnerParam = value;
    }
    
    public Task Succeed() => InnerSucceed(result: Unit.Instance);
    
    public Task BusyWaitUntil(Func<ControlPanel<TParam>, bool> predicate, TimeSpan? maxWait = null, TimeSpan? checkFrequency = null)
        => BusyWaitUntil(() => predicate(this), maxWait, checkFrequency);
}

public class ControlPanel<TParam, TReturn> : BaseControlPanel<TParam, TReturn> where TParam : notnull
{
    internal ControlPanel(
        InvocationHelper<TParam, TReturn> invocationHelper,
        FlowId flowId, StoredId storedId, ReplicaId? ownerReplica, Status status,
        long expires, TParam innerParam,
        TReturn? innerResult,
        ExistingEffects effects, ExistingMessages messages,
        FatalWorkflowException? fatalWorkflowException,
        UtcNow utcNow
    ) : base(
        invocationHelper,
        flowId, storedId, ownerReplica, status, expires,
        innerParam, innerResult, effects, messages,
        fatalWorkflowException,
        utcNow
    ) { }

    public Task Succeed(TReturn result) => InnerSucceed(result);
    public TReturn? Result => InnerResult;

    public TParam Param
    {
        get => InnerParam;
        set => InnerParam = value;
    }
    
    public Task BusyWaitUntil(Func<ControlPanel<TParam, TReturn>, bool> predicate, TimeSpan? maxWait = null, TimeSpan? checkFrequency = null)
        => BusyWaitUntil(() => predicate(this), maxWait, checkFrequency);
}

public abstract class BaseControlPanel<TParam, TReturn>
{
    private readonly InvocationHelper<TParam, TReturn> _invocationHelper;
    private bool _innerParamChanged;

    internal BaseControlPanel(
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
        FatalWorkflowException? fatalWorkflowException,
        UtcNow utcNow)
    {
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

        // An explicit restart marks the flow immediately eligible for restart: set it to Postponed with expiry 0
        // (GetExpiredFunctions returns expires <= now && Postponed). A watchdog then claims it via RestartExecutions
        // and runs it - so restart requires a running watchdog. Guard on owner-is-null (expectedReplica: null) to
        // match the removed singular claim's WHERE owner IS NULL - a restartable flow is always unowned, and
        // OwnerReplica is not refreshed so it cannot be relied on here.
        var success = await _invocationHelper.SetFunctionState(
            StoredId, Status.Postponed,
            InnerParam,
            result: default,
            expires: 0,
            exception: null,
            expectedReplicaId: null
        );
        if (!success)
            throw UnexpectedStateException.ConcurrentModification(FlowId);

        Status = Status.Postponed;
        PostponedUntil = RestartMarker;
        _innerParamChanged = false;

        // Wait for a watchdog to pick up the restart (the flow leaves the Postponed/expiry-0 marker state) so callers
        // observe real progress rather than the transient postponed state - e.g. a subsequent WaitForCompletion.
        await BusyWaitUntil(() => PostponedUntil != RestartMarker);

        var innerScheduled = _invocationHelper.CreateInnerScheduled([FlowId], parentWorkflow: null, detach: null);

        if (refresh)
            await Refresh();

        return innerScheduled.ToScheduledWithResult();
    }

    // The Postponed/expiry-0 state ScheduleRestart writes to mark a flow for immediate watchdog restart.
    private static readonly DateTime RestartMarker = new(0, DateTimeKind.Utc);

    private protected async Task BusyWaitUntil(Func<bool> predicate, TimeSpan? maxWait = null, TimeSpan? checkFrequency = null)
    {
        if (predicate())
            return;

        maxWait ??= TimeSpan.FromSeconds(10);
        checkFrequency ??= TimeSpan.FromMilliseconds(250);

        var stopWatch = Stopwatch.StartNew();
        do
        {
            await Task.Delay(checkFrequency.Value);
            await Refresh();
            if (predicate())
                return;
        } while (stopWatch.Elapsed < maxWait);

        throw new TimeoutException("Predicate was not meet before max wait for reached");
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

        _innerParamChanged = false;
    }

    public async Task<TReturn> WaitForCompletion(bool allowPostponeAndSuspended = false, TimeSpan? maxWait = null)
    {
        var result = await _invocationHelper.WaitForFunctionResult(FlowId, StoredId, allowPostponeAndSuspended, maxWait);
        await Refresh();
        return result;
    }
}