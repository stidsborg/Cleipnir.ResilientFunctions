using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Cleipnir.ResilientFunctions.Domain.Exceptions;
using Cleipnir.ResilientFunctions.Helpers;

namespace Cleipnir.ResilientFunctions.Domain;

public class ControlPanel<TParam, TScrapbook> where TParam : notnull where TScrapbook : RScrapbook, new()
{
    private readonly InvocationHelper<TParam, TScrapbook, Unit> _invocationHelper;
    private readonly RAction.ReInvoke _reInvoke;
    private readonly RAction.ScheduleReInvoke _scheduleReInvoke;

    private bool _changed;
    
    internal ControlPanel(
        InvocationHelper<TParam, TScrapbook, Unit> invocationHelper,
        RAction.ReInvoke reInvoke,
        RAction.ScheduleReInvoke scheduleReInvoke,
        FunctionId functionId, 
        Status status, 
        int epoch,
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
        CrashedCheckFrequency = crashedCheckFrequency;
        _param = param;
        _scrapbook = scrapbook;
        PostponedUntil = postponedUntil;
        PreviouslyThrownException = previouslyThrownException;
    }

    public FunctionId FunctionId { get; }
    public Status Status { get; private set; }
    
    public int Epoch { get; private set; }
    public long CrashedCheckFrequency { get; private set; }

    private Task<ExistingEvents>? _events;
    public Task<ExistingEvents> Events => _events ??= _invocationHelper.GetExistingEvents(FunctionId);

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
            existingEvents: _events == null ? null : await _events,
            Epoch
        );

        if (!success)
            throw new ConcurrentModificationException(FunctionId);
        
        Epoch++;
        _changed = false;
        _events = null;
    }
    
    public async Task Postpone(DateTime until)
    {
        var success = await _invocationHelper.SetFunctionState(
            FunctionId, Status.Postponed, Param, Scrapbook, until, exception: null,
            existingEvents: _events == null ? null : await _events,
            Epoch
        );

        if (!success)
            throw new ConcurrentModificationException(FunctionId);

        Epoch++;
        Status = Status.Postponed;
        PostponedUntil = until;
        _changed = false;
        _events = null;
    }

    public Task Postpone(TimeSpan delay) => Postpone(DateTime.UtcNow + delay);

    public async Task Fail(Exception exception)
    {
        var success = await _invocationHelper.SetFunctionState(
            FunctionId, Status.Failed, Param, Scrapbook, postponeUntil: null, exception,
            existingEvents: _events == null ? null : await _events,
            Epoch
        );

        if (!success)
            throw new ConcurrentModificationException(FunctionId);
     
        Epoch++;
        Status = Status.Failed;
        PreviouslyThrownException = new PreviouslyThrownException(exception.Message, exception.StackTrace, exception.GetType());
        _changed = false;
        _events = null;
    }
    
    public async Task SaveChanges()
    {
        var success = await _invocationHelper.SaveControlPanelChanges(
            FunctionId, Param, Scrapbook,
            existingEvents: _events == null ? null : await _events,
            suspended: Status == Status.Suspended,
            Epoch
        );
        
        if (!success)
            throw new ConcurrentModificationException(FunctionId);
        
        Epoch++;
        _changed = false;
        _events = null;
    }

    public Task Delete() => _invocationHelper.Delete(FunctionId, Epoch);

    public async Task ReInvoke()
    {
        if (_changed)
            await SaveChanges();

        await _reInvoke(FunctionId.InstanceId.Value, Epoch);   
    }
    public async Task ScheduleReInvoke()
    {
        if (_changed)
            await SaveChanges();
        
        await _scheduleReInvoke(FunctionId.InstanceId.Value, Epoch);
    }

    public async Task Refresh()
    {
        var sf = await _invocationHelper.GetFunction(FunctionId);
        if (sf == null)
            throw new UnexpectedFunctionState(FunctionId, $"Function '{FunctionId}' not found");

        Status = sf.Status;
        Epoch = sf.Epoch;
        CrashedCheckFrequency = sf.CrashedCheckFrequency;
        Param = sf.Param;
        Scrapbook = sf.Scrapbook;
        PostponedUntil = sf.PostponedUntil;
        PreviouslyThrownException = sf.PreviouslyThrownException;
        _changed = false;
        _events = null;
    }

    public async Task WaitForCompletion() => await _invocationHelper.WaitForFunctionResult(FunctionId);
}

public class ControlPanel<TParam, TScrapbook, TReturn> where TParam : notnull where TScrapbook : RScrapbook, new()
{
    private readonly InvocationHelper<TParam, TScrapbook, TReturn> _invocationHelper;
    private readonly RFunc.ReInvoke<TReturn> _reInvoke;
    private readonly RFunc.ScheduleReInvoke _scheduleReInvoke;

    private bool _changed;

    internal ControlPanel(
        InvocationHelper<TParam, TScrapbook, TReturn> invocationHelper,
        RFunc.ReInvoke<TReturn> reInvoke,
        RFunc.ScheduleReInvoke scheduleReInvoke,
        FunctionId functionId, 
        Status status, 
        int epoch,
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
        CrashedCheckFrequency = crashedCheckFrequency;
        _param = param;
        _scrapbook = scrapbook;
        Result = result;
        PostponedUntil = postponedUntil;
        PreviouslyThrownException = previouslyThrownException;
    }

    public FunctionId FunctionId { get; }
    public Status Status { get; private set; }
    
    public int Epoch { get; private set; }
    public long CrashedCheckFrequency { get; private set; }
    
    private Task<ExistingEvents>? _events;
    public Task<ExistingEvents> Events => _events ??= _invocationHelper.GetExistingEvents(FunctionId);

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
            FunctionId, Status.Succeeded, Param, Scrapbook, result, PostponedUntil, exception: null, 
            existingEvents: _events == null ? null : await _events, 
            Epoch
        );

        if (!success)
            throw new ConcurrentModificationException(FunctionId);
        
        Epoch++;
        Status = Status.Succeeded;
        _changed = false;
        _events = null;
    }
    
    public async Task Postpone(DateTime until)
    {
        var success = await _invocationHelper.SetFunctionState(
            FunctionId, Status.Postponed, Param, Scrapbook, result: default, until, exception: null, 
            existingEvents: _events == null ? null : await _events,
            Epoch
        );

        if (!success)
            throw new ConcurrentModificationException(FunctionId);
        
        Epoch++;
        Status = Status.Postponed;
        PostponedUntil = until;
        _changed = false;
        _events = null;
    }
        
    public Task Postpone(TimeSpan delay) => Postpone(DateTime.UtcNow + delay);

    public async Task Fail(Exception exception)
    {
        var success = await _invocationHelper.SetFunctionState(
            FunctionId, Status.Failed, Param, Scrapbook, result: default, postponeUntil: null, exception, 
            existingEvents: _events == null ? null : await _events,
            Epoch
        );

        if (!success)
            throw new ConcurrentModificationException(FunctionId);
        
        Epoch++;
        Status = Status.Failed;
        PreviouslyThrownException = new PreviouslyThrownException(exception.Message, exception.StackTrace, exception.GetType());
        _changed = false;
        _events = null;
    }

    public async Task SaveChanges()
    {
        var success = await _invocationHelper.SaveControlPanelChanges(
            FunctionId, Param, Scrapbook,
            existingEvents: _events == null ? null : await _events,
            suspended: Status == Status.Suspended,
            Epoch
        );
        
        if (!success)
            throw new ConcurrentModificationException(FunctionId);
        
        Epoch++;
        _changed = false;
        _events = null;
    }
    
    public Task Delete() => _invocationHelper.Delete(FunctionId, Epoch);

    public async Task<TReturn> ReInvoke()
    {
        if (_changed)
            await SaveChanges();

        return await _reInvoke(FunctionId.InstanceId.Value, Epoch);   
    }
    public async Task ScheduleReInvoke()
    {
        if (_changed)
            await SaveChanges();

        await _scheduleReInvoke(FunctionId.InstanceId.Value, Epoch);
    }
    
    public async Task Refresh()
    {
        var sf = await _invocationHelper.GetFunction(FunctionId);
        if (sf == null)
            throw new UnexpectedFunctionState(FunctionId, $"Function '{FunctionId}' not found");

        Status = sf.Status;
        Epoch = sf.Epoch;
        CrashedCheckFrequency = sf.CrashedCheckFrequency;
        Param = sf.Param;
        Scrapbook = sf.Scrapbook;
        Result = sf.Result;
        PostponedUntil = sf.PostponedUntil;
        PreviouslyThrownException = sf.PreviouslyThrownException;

        _changed = false;
        _events = null;
    }
    
    public async Task<TReturn> WaitForCompletion() => await _invocationHelper.WaitForFunctionResult(FunctionId);
}