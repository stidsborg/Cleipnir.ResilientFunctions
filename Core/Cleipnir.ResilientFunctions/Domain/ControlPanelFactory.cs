using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Cleipnir.ResilientFunctions.Helpers;

namespace Cleipnir.ResilientFunctions.Domain;

public class ControlPanelFactory 
{
    private readonly FlowType _flowType;
    private readonly Invoker<Unit, Unit> _invoker;
    private readonly InvocationHelper<Unit, Unit> _invocationHelper;

    internal ControlPanelFactory(FlowType flowType, Invoker<Unit, Unit> invoker, InvocationHelper<Unit, Unit> invocationHelper)
    {
        _invoker = invoker;
        _invocationHelper = invocationHelper;
        _flowType = flowType;
    }
    
    public async Task<ControlPanel?> Create(FlowInstance flowInstance)
    {
        var functionId = new FlowId(_flowType, flowInstance);
        var functionState = await _invocationHelper.GetFunction(functionId);
        if (functionState == null)
            return null;
        
        return new ControlPanel(
            _invoker,
            _invocationHelper,
            functionId,
            functionState.Status,
            functionState.Epoch,
            functionState.LeaseExpiration,
            functionState.PostponedUntil,
            await _invocationHelper.GetExistingEffects(functionId),
            await _invocationHelper.GetExistingStates(functionId, functionState.DefaultState),
            await _invocationHelper.GetExistingMessages(functionId),
            await _invocationHelper.GetExistingTimeouts(functionId),
            await _invocationHelper.CreateCorrelations(functionId, sync: true),
            functionState.PreviouslyThrownException
        );
    }
}


public class ControlPanelFactory<TParam> where TParam : notnull 
{
    private readonly FlowType _flowType;
    private readonly Invoker<TParam, Unit> _invoker;
    private readonly InvocationHelper<TParam, Unit> _invocationHelper;

    internal ControlPanelFactory(FlowType flowType, Invoker<TParam, Unit> invoker, InvocationHelper<TParam, Unit> invocationHelper)
    {
        _invoker = invoker;
        _invocationHelper = invocationHelper;
        _flowType = flowType;
    }
    
    public async Task<ControlPanel<TParam>?> Create(FlowInstance flowInstance)
    {
        var functionId = new FlowId(_flowType, flowInstance);
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
            await _invocationHelper.CreateCorrelations(functionId, sync: true),
            functionState.PreviouslyThrownException
        );
    }
}

public class ControlPanelFactory<TParam, TReturn> where TParam : notnull
{
    private readonly FlowType _flowType;
    private readonly Invoker<TParam, TReturn> _invoker;
    private readonly InvocationHelper<TParam, TReturn> _invocationHelper;

    internal ControlPanelFactory(FlowType flowType, Invoker<TParam, TReturn> invoker, InvocationHelper<TParam, TReturn> invocationHelper)
    {
        _invoker = invoker;
        _invocationHelper = invocationHelper;
        _flowType = flowType;
    }

    public async Task<ControlPanel<TParam, TReturn>?> Create(FlowInstance flowInstance)
    {
        var functionId = new FlowId(_flowType, flowInstance);
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
            await _invocationHelper.CreateCorrelations(functionId, sync: true),
            f.PreviouslyThrownException
        );
    }
}