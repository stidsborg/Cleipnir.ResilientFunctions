using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Cleipnir.ResilientFunctions.Helpers;

namespace Cleipnir.ResilientFunctions.Domain;

public class ControlPanels<TParam, TState> where TParam : notnull where TState : WorkflowState, new()
{
    private readonly FunctionTypeId _functionTypeId;
    private readonly Invoker<TParam, TState, Unit> _invoker;
    private readonly InvocationHelper<TParam, TState, Unit> _invocationHelper;

    internal ControlPanels(FunctionTypeId functionTypeId, Invoker<TParam, TState, Unit> invoker, InvocationHelper<TParam, TState, Unit> invocationHelper)
    {
        _invoker = invoker;
        _invocationHelper = invocationHelper;
        _functionTypeId = functionTypeId;
    }
    
    public async Task<ControlPanel<TParam, TState>?> For(FunctionInstanceId functionInstanceId)
    {
        var functionId = new FunctionId(_functionTypeId, functionInstanceId);
        var functionState = await _invocationHelper.GetFunction(functionId);
        if (functionState == null)
            return null;
        
        return new ControlPanel<TParam, TState>(
            _invoker,
            _invocationHelper,
            functionId,
            functionState.Status,
            functionState.Epoch,
            functionState.LeaseExpiration,
            functionState.Param,
            functionState.State,
            functionState.PostponedUntil,
            await _invocationHelper.GetExistingActivities(functionId),
            await _invocationHelper.GetExistingMessages(functionId),
            await _invocationHelper.GetExistingTimeouts(functionId),
            functionState.PreviouslyThrownException
        );
    }
}

public class ControlPanels<TParam, TState, TReturn> where TParam : notnull where TState : WorkflowState, new()
{
    public delegate Task<TReturn> ReInvoke(string functionInstanceId, int expectedEpoch);
    public delegate Task ScheduleReInvoke(string functionInstanceId, int expectedEpoch);
    
    private readonly FunctionTypeId _functionTypeId;
    private readonly Invoker<TParam, TState, TReturn> _invoker;
    private readonly InvocationHelper<TParam, TState, TReturn> _invocationHelper;

    internal ControlPanels(FunctionTypeId functionTypeId, Invoker<TParam, TState, TReturn> invoker, InvocationHelper<TParam, TState, TReturn> invocationHelper)
    {
        _invoker = invoker;
        _invocationHelper = invocationHelper;
        _functionTypeId = functionTypeId;
    }

    public async Task<ControlPanel<TParam, TState, TReturn>?> For(FunctionInstanceId functionInstanceId)
    {
        var functionId = new FunctionId(_functionTypeId, functionInstanceId);
        var f = await _invocationHelper.GetFunction(functionId);
        if (f == null)
            return null;
        
        return new ControlPanel<TParam, TState, TReturn>(
            _invoker,
            _invocationHelper,
            functionId,
            f.Status,
            f.Epoch,
            f.LeaseExpiration,
            f.Param,
            f.State,
            f.Result,
            f.PostponedUntil,
            await _invocationHelper.GetExistingActivities(functionId),
            await _invocationHelper.GetExistingMessages(functionId),
            await _invocationHelper.GetExistingTimeouts(functionId),
            f.PreviouslyThrownException
        );
    }
}