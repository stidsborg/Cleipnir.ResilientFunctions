using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Cleipnir.ResilientFunctions.Domain.Exceptions;
using Cleipnir.ResilientFunctions.Helpers;

namespace Cleipnir.ResilientFunctions.Domain;

public class ControlPanel<TParam> where TParam : notnull 
{
    private readonly Invoker<TParam, Unit> _invoker;
    private readonly InvocationHelper<TParam, Unit> _invocationHelper;

    private bool _changed;
    
    internal ControlPanel(
        Invoker<TParam, Unit> invoker,
        InvocationHelper<TParam, Unit> invocationHelper,
        FunctionId functionId, 
        Status status, 
        int epoch,
        long leaseExpiration,
        TParam param, 
        DateTime? postponedUntil, 
        ExistingEffects existingEffects,
        ExistingStates existingStates,
        ExistingMessages existingMessages,
        ExistingTimeouts existingTimeouts,
        PreviouslyThrownException? previouslyThrownException)
    {
        _invoker = invoker;
        _invocationHelper = invocationHelper;
        FunctionId = functionId;
        Status = status;
        Epoch = epoch;
        LeaseExpiration = new DateTime(leaseExpiration, DateTimeKind.Utc);
        _param = param;
        PostponedUntil = postponedUntil;
        PreviouslyThrownException = previouslyThrownException;
        Effects = existingEffects;
        States = existingStates;
        Messages = existingMessages;
        Timeouts = existingTimeouts;
    }

    public FunctionId FunctionId { get; }
    public Status Status { get; private set; }
    
    public int Epoch { get; private set; }
    public DateTime LeaseExpiration { get; private set; }
    public ExistingMessages Messages { get; private set; }
    public ExistingEffects Effects { get; private set; } 
    public ExistingStates States { get; private set; }
    public ExistingTimeouts Timeouts { get; private set; }
    
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

    public DateTime? PostponedUntil { get; private set; }
    public PreviouslyThrownException? PreviouslyThrownException { get; private set; }

    public async Task Succeed()
    {
        var success = await _invocationHelper.SetFunctionState(
            FunctionId, Status.Succeeded, Param, postponeUntil: null, exception: null, 
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
            FunctionId, Status.Postponed, Param, until, exception: null,
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
            FunctionId, Status.Failed, Param, postponeUntil: null, exception,
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
        var success = await _invocationHelper.SaveControlPanelChanges(FunctionId, Param, @return: default, Epoch);
        if (!success)
            throw new ConcurrentModificationException(FunctionId);
        
        Epoch++;
        _changed = false;
    }

    public Task Delete() => _invocationHelper.Delete(FunctionId);

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
        PostponedUntil = sf.PostponedUntil;
        PreviouslyThrownException = sf.PreviouslyThrownException;
        _changed = false;
        Messages = await _invocationHelper.GetExistingMessages(FunctionId);
        Effects = await _invocationHelper.GetExistingEffects(FunctionId);
        States = await _invocationHelper.GetExistingStates(FunctionId, sf.DefaultState);
        Timeouts = await _invocationHelper.GetExistingTimeouts(FunctionId);
    }

    public async Task WaitForCompletion(bool allowPostponedAndSuspended = false) => await _invocationHelper.WaitForFunctionResult(FunctionId, allowPostponedAndSuspended);
}

public class ControlPanel<TParam, TReturn> where TParam : notnull
{
    private readonly Invoker<TParam, TReturn> _invoker;
    private readonly InvocationHelper<TParam, TReturn> _invocationHelper;
    private bool _changed;

    internal ControlPanel(
        Invoker<TParam, TReturn> invoker, 
        InvocationHelper<TParam, TReturn> invocationHelper,
        FunctionId functionId, 
        Status status, 
        int epoch,
        long leaseExpiration,
        TParam param, 
        TReturn? result,
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
        
        _param = param;
        Result = result;
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

    public TReturn? Result { get; set; }
    public DateTime? PostponedUntil { get; private set; }
    public PreviouslyThrownException? PreviouslyThrownException { get; private set; }

    public async Task Succeed(TReturn result)
    {
        var success = await _invocationHelper.SetFunctionState(
            FunctionId, Status.Succeeded, 
            Param, 
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
            Param,  
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
            Param,  
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
        var success = await _invocationHelper.SaveControlPanelChanges(FunctionId, Param, Result, Epoch);
        if (!success)
            throw new ConcurrentModificationException(FunctionId);
        
        Epoch++;
        _changed = false;
    }
    
    public Task Delete() => _invocationHelper.Delete(FunctionId);

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
        Result = sf.Result;
        PostponedUntil = sf.PostponedUntil;
        PreviouslyThrownException = sf.PreviouslyThrownException;
        Effects = await _invocationHelper.GetExistingEffects(FunctionId);
        States = await _invocationHelper.GetExistingStates(FunctionId, sf.DefaultState);
        Timeouts = await _invocationHelper.GetExistingTimeouts(FunctionId); 

        _changed = false;
    }
    
    public async Task<TReturn> WaitForCompletion(bool allowPostponeAndSuspended = false) 
        => await _invocationHelper.WaitForFunctionResult(FunctionId, allowPostponeAndSuspended);
}