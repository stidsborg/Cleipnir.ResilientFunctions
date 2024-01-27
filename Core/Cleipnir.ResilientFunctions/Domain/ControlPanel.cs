using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime;
using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Cleipnir.ResilientFunctions.Domain.Exceptions;
using Cleipnir.ResilientFunctions.Helpers;

namespace Cleipnir.ResilientFunctions.Domain;

public class ControlPanel<TParam, TScrapbook> where TParam : notnull where TScrapbook : RScrapbook, new()
{
    private readonly Invoker<TParam, TScrapbook, Unit> _invoker;
    private readonly InvocationHelper<TParam, TScrapbook, Unit> _invocationHelper;

    private bool _changed;
    
    internal ControlPanel(
        Invoker<TParam, TScrapbook, Unit> invoker,
        InvocationHelper<TParam, TScrapbook, Unit> invocationHelper,
        FunctionId functionId, 
        Status status, 
        int epoch,
        long leaseExpiration,
        TParam param, 
        TScrapbook scrapbook, 
        DateTime? postponedUntil, 
        ExistingActivities existingActivities,
        ExistingMessages existingMessages,
        PreviouslyThrownException? previouslyThrownException)
    {
        _invoker = invoker;
        _invocationHelper = invocationHelper;
        FunctionId = functionId;
        Status = status;
        Epoch = epoch;
        LeaseExpiration = new DateTime(leaseExpiration, DateTimeKind.Utc);
        _param = param;
        _scrapbook = scrapbook;
        PostponedUntil = postponedUntil;
        PreviouslyThrownException = previouslyThrownException;
        Activities = existingActivities;
        Messages = existingMessages;
    }

    public FunctionId FunctionId { get; }
    public Status Status { get; private set; }
    
    public int Epoch { get; private set; }
    public DateTime LeaseExpiration { get; private set; }
    public ExistingMessages Messages { get; private set; }
    public ExistingActivities Activities { get; private set; } 
    public ITimeoutProvider TimeoutProvider => _invocationHelper.CreateTimeoutProvider(FunctionId);
    
    private TParam _param;
    public TParam Param
    {
        get
        {
            _changed = true;
            return _param;
        }
        set
        {
            _changed = true;
            _param = value;
        }
    }

    private TScrapbook _scrapbook;
    public TScrapbook Scrapbook
    {
        get
        {
            _changed = true;
            return _scrapbook;
        }
        set
        {
            _changed = true;
            _scrapbook = value;
        }
    }

    public DateTime? PostponedUntil { get; private set; }
    public PreviouslyThrownException? PreviouslyThrownException { get; private set; }

    public async Task Succeed()
    {
        var success = await _invocationHelper.SetFunctionState(
            FunctionId, Status.Succeeded, Param, Scrapbook, postponeUntil: null, exception: null, 
            Epoch
        );

        if (!success)
            throw new ConcurrentModificationException(FunctionId);
        
        Epoch++;
        _changed = false;
    }
    
    public async Task Postpone(DateTime until)
    {
        var success = await _invocationHelper.SetFunctionState(
            FunctionId, Status.Postponed, Param, Scrapbook, until, exception: null,
            Epoch
        );

        if (!success)
            throw new ConcurrentModificationException(FunctionId);

        Epoch++;
        Status = Status.Postponed;
        PostponedUntil = until;
        _changed = false;
    }

    public Task Postpone(TimeSpan delay) => Postpone(DateTime.UtcNow + delay);

    public async Task Fail(Exception exception)
    {
        var success = await _invocationHelper.SetFunctionState(
            FunctionId, Status.Failed, Param, Scrapbook, postponeUntil: null, exception,
            Epoch
        );

        if (!success)
            throw new ConcurrentModificationException(FunctionId);
     
        Epoch++;
        Status = Status.Failed;
        PreviouslyThrownException = new PreviouslyThrownException(exception.Message, exception.StackTrace, exception.GetType());
        _changed = false;
    }
    
    public async Task SaveChanges()
    {
        var success = await _invocationHelper.SaveControlPanelChanges(FunctionId, Param, Scrapbook, @return: default, Epoch);
        if (!success)
            throw new ConcurrentModificationException(FunctionId);
        
        Epoch++;
        _changed = false;
    }

    public Task Delete() => _invocationHelper.Delete(FunctionId, Epoch);

    public async Task ReInvoke()
    {
        if (_changed)
            await SaveChanges();

        await _invoker.ReInvoke(FunctionId.InstanceId.Value, Epoch);
    }
    public async Task ScheduleReInvoke()
    {
        if (_changed)
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
        Param = sf.Param;
        Scrapbook = sf.Scrapbook;
        PostponedUntil = sf.PostponedUntil;
        PreviouslyThrownException = sf.PreviouslyThrownException;
        _changed = false;
        Messages = await _invocationHelper.GetExistingMessages(FunctionId);
        Activities = await _invocationHelper.GetExistingActivities(FunctionId);
    }

    public async Task WaitForCompletion(bool allowPostponedAndSuspended = false) => await _invocationHelper.WaitForFunctionResult(FunctionId, allowPostponedAndSuspended);
}

public class ControlPanel<TParam, TScrapbook, TReturn> where TParam : notnull where TScrapbook : RScrapbook, new()
{
    private readonly Invoker<TParam, TScrapbook, TReturn> _invoker;
    private readonly InvocationHelper<TParam, TScrapbook, TReturn> _invocationHelper;
    private bool _changed;

    internal ControlPanel(
        Invoker<TParam, TScrapbook, TReturn> invoker, 
        InvocationHelper<TParam, TScrapbook, TReturn> invocationHelper,
        FunctionId functionId, 
        Status status, 
        int epoch,
        long leaseExpiration,
        TParam param, 
        TScrapbook scrapbook, 
        TReturn? result,
        DateTime? postponedUntil, 
        ExistingActivities activities,
        ExistingMessages messages,
        PreviouslyThrownException? previouslyThrownException)
    {
        _invoker = invoker;
        _invocationHelper = invocationHelper;
        FunctionId = functionId;
        Status = status;
        Epoch = epoch;
        LeaseExpiration = new DateTime(leaseExpiration, DateTimeKind.Utc);
        
        _param = param;
        _scrapbook = scrapbook;
        Result = result;
        PostponedUntil = postponedUntil;
        Activities = activities;
        Messages = messages;
        PreviouslyThrownException = previouslyThrownException;
    }

    public FunctionId FunctionId { get; }
    public Status Status { get; private set; }
    
    public int Epoch { get; private set; }
    public DateTime LeaseExpiration { get; private set; }
    
    public ExistingMessages Messages { get; private set; }
    public ExistingActivities Activities { get; private set; } 

    public ITimeoutProvider TimeoutProvider => _invocationHelper.CreateTimeoutProvider(FunctionId);

    private TParam _param;
    public TParam Param
    {
        get
        {
            _changed = true;
            return _param;
        }
        set
        {
            _changed = true;
            _param = value;
        }
    }

    private TScrapbook _scrapbook;
    public TScrapbook Scrapbook
    {
        get
        {
            _changed = true;
            return _scrapbook;
        }
        set
        {
            _changed = true;
            _scrapbook = value;
        }
    }

    public TReturn? Result { get; set; }
    public DateTime? PostponedUntil { get; private set; }
    public PreviouslyThrownException? PreviouslyThrownException { get; private set; }

    public async Task Succeed(TReturn result)
    {
        var success = await _invocationHelper.SetFunctionState(
            FunctionId, Status.Succeeded, 
            Param, Scrapbook, 
            result, 
            PostponedUntil, exception: null, 
            Epoch
        );

        if (!success)
            throw new ConcurrentModificationException(FunctionId);
        
        Epoch++;
        Status = Status.Succeeded;
        _changed = false;
    }
    
    public async Task Postpone(DateTime until)
    {
        var success = await _invocationHelper.SetFunctionState(
            FunctionId, Status.Postponed, 
            Param, Scrapbook, 
            result: default, until, 
            exception: null, 
            Epoch
        );

        if (!success)
            throw new ConcurrentModificationException(FunctionId);
        
        Epoch++;
        Status = Status.Postponed;
        PostponedUntil = until;
        _changed = false;
    }
        
    public Task Postpone(TimeSpan delay) => Postpone(DateTime.UtcNow + delay);

    public async Task Fail(Exception exception)
    {
        var success = await _invocationHelper.SetFunctionState(
            FunctionId, Status.Failed, 
            Param, Scrapbook, 
            result: default, postponeUntil: null, exception, 
            Epoch
        );

        if (!success)
            throw new ConcurrentModificationException(FunctionId);
        
        Epoch++;
        Status = Status.Failed;
        PreviouslyThrownException = new PreviouslyThrownException(exception.Message, exception.StackTrace, exception.GetType());
        _changed = false;
    }

    public async Task SaveChanges()
    {
        var success = await _invocationHelper.SaveControlPanelChanges(FunctionId, Param, Scrapbook, Result, Epoch);
        if (!success)
            throw new ConcurrentModificationException(FunctionId);
        
        Epoch++;
        _changed = false;
    }
    
    public Task Delete() => _invocationHelper.Delete(FunctionId, Epoch);

    public async Task<TReturn> ReInvoke()
    {
        if (_changed)
            await SaveChanges();

        return await _invoker.ReInvoke(FunctionId.InstanceId.Value, Epoch);   
    }
    public async Task ScheduleReInvoke()
    {
        if (_changed)
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
        Param = sf.Param;
        Scrapbook = sf.Scrapbook;
        Result = sf.Result;
        PostponedUntil = sf.PostponedUntil;
        PreviouslyThrownException = sf.PreviouslyThrownException;
        Activities = await _invocationHelper.GetExistingActivities(FunctionId);

        _changed = false;
    }
    
    public async Task<TReturn> WaitForCompletion(bool allowPostponeAndSuspended = false) 
        => await _invocationHelper.WaitForFunctionResult(FunctionId, allowPostponeAndSuspended);
}