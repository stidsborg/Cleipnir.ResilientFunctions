using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Cleipnir.ResilientFunctions.Helpers;

namespace Cleipnir.ResilientFunctions.Domain;

public class ControlPanels<TParam> where TParam : notnull 
{
    private readonly FunctionTypeId _functionTypeId;
    private readonly Invoker<TParam, Unit> _invoker;
    private readonly InvocationHelper<TParam, Unit> _invocationHelper;

    internal ControlPanels(FunctionTypeId functionTypeId, Invoker<TParam, Unit> invoker, InvocationHelper<TParam, Unit> invocationHelper)
    {
        _invoker = invoker;
        _invocationHelper = invocationHelper;
        _functionTypeId = functionTypeId;
    }
    
    public async Task<ControlPanel<TParam>?> For(FunctionInstanceId functionInstanceId)
    {
        var functionId = new FunctionId(_functionTypeId, functionInstanceId);
        var functionState = await _invocationHelper.GetFunction(functionId);
        if (functionState == null)
            return null;
        
        return new ControlPanel<TParam>(
            _invoker,
            _invocationHelper,
            functionId,
            functionState.Status,
            functionState.Epoch,
            functionState.LeaseExpiration,
            functionState.Param!,
            functionState.PostponedUntil,
            await _invocationHelper.GetExistingEffects(functionId),
            await _invocationHelper.GetExistingStates(functionId, functionState.DefaultState),
            await _invocationHelper.GetExistingMessages(functionId),
            await _invocationHelper.GetExistingTimeouts(functionId),
            functionState.PreviouslyThrownException
        );
    }
}

public class ControlPanels<TParam, TReturn> where TParam : notnull
{
    public delegate Task<TReturn> ReInvoke(string functionInstanceId, int expectedEpoch);
    public delegate Task ScheduleReInvoke(string functionInstanceId, int expectedEpoch);
    
    private readonly FunctionTypeId _functionTypeId;
    private readonly Invoker<TParam, TReturn> _invoker;
    private readonly InvocationHelper<TParam, TReturn> _invocationHelper;

    internal ControlPanels(FunctionTypeId functionTypeId, Invoker<TParam, TReturn> invoker, InvocationHelper<TParam, TReturn> invocationHelper)
    {
        _invoker = invoker;
        _invocationHelper = invocationHelper;
        _functionTypeId = functionTypeId;
    }

    public async Task<ControlPanel<TParam, TReturn>?> For(FunctionInstanceId functionInstanceId)
    {
        var functionId = new FunctionId(_functionTypeId, functionInstanceId);
        var f = await _invocationHelper.GetFunction(functionId);
        if (f == null)
            return null;
        
        return new ControlPanel<TParam, TReturn>(
            _invoker,
            _invocationHelper,
            functionId,
            f.Status,
            f.Epoch,
            f.LeaseExpiration,
            f.Param!,
            f.Result,
            f.PostponedUntil,
            await _invocationHelper.GetExistingEffects(functionId),
            await _invocationHelper.GetExistingStates(functionId, f.DefaultState),
            await _invocationHelper.GetExistingMessages(functionId),
            await _invocationHelper.GetExistingTimeouts(functionId),
            f.PreviouslyThrownException
        );
    }
}