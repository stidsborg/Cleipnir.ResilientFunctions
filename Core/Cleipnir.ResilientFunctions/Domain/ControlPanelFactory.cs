using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.Domain;

public class ControlPanelFactory 
{
    private readonly FlowType _flowType;
    private readonly StoredType _storedType;
    private readonly Invoker<Unit, Unit> _invoker;
    private readonly InvocationHelper<Unit, Unit> _invocationHelper;

    internal ControlPanelFactory(FlowType flowType, StoredType storedType, Invoker<Unit, Unit> invoker, InvocationHelper<Unit, Unit> invocationHelper)
    {
        _invoker = invoker;
        _invocationHelper = invocationHelper;
        _flowType = flowType;
        _storedType = storedType;
    }
    
    public async Task<ControlPanel?> Create(FlowInstance flowInstance)
    {
        var flowId = new FlowId(_flowType, flowInstance);
        var storedId = new StoredId(_storedType, flowInstance.Value.ToStoredInstance());
        var functionState = await _invocationHelper.GetFunction(storedId);
        if (functionState == null)
            return null;
        
        return new ControlPanel(
            _invoker,
            _invocationHelper,
            flowId,
            storedId,
            functionState.Status,
            functionState.Epoch,
            functionState.Expires,
            _invocationHelper.CreateExistingEffects(flowId),
            _invocationHelper.CreateExistingStates(flowId),
            _invocationHelper.CreateExistingMessages(flowId),
            _invocationHelper.CreateExistingTimeouts(flowId),
            _invocationHelper.CreateCorrelations(flowId),
            functionState.PreviouslyThrownException
        );
    }
}


public class ControlPanelFactory<TParam> where TParam : notnull 
{
    private readonly FlowType _flowType;
    private readonly StoredType _storedType;
    private readonly Invoker<TParam, Unit> _invoker;
    private readonly InvocationHelper<TParam, Unit> _invocationHelper;

    internal ControlPanelFactory(FlowType flowType, StoredType storedType, Invoker<TParam, Unit> invoker, InvocationHelper<TParam, Unit> invocationHelper)
    {
        _invoker = invoker;
        _invocationHelper = invocationHelper;
        _flowType = flowType;
        _storedType = storedType;
    }
    
    public async Task<ControlPanel<TParam>?> Create(FlowInstance flowInstance)
    {
        var flowId = new FlowId(_flowType, flowInstance);
        var storedId = new StoredId(_storedType, flowInstance.Value.ToStoredInstance());
        var functionState = await _invocationHelper.GetFunction(storedId);
        if (functionState == null)
            return null;
        
        return new ControlPanel<TParam>(
            _invoker,
            _invocationHelper,
            flowId,
            storedId,
            functionState.Status,
            functionState.Epoch,
            functionState.Expires,
            functionState.Param!,
            _invocationHelper.CreateExistingEffects(flowId),
            _invocationHelper.CreateExistingStates(flowId),
            _invocationHelper.CreateExistingMessages(flowId),
            _invocationHelper.CreateExistingTimeouts(flowId),
            _invocationHelper.CreateCorrelations(flowId),
            functionState.PreviouslyThrownException
        );
    }
}

public class ControlPanelFactory<TParam, TReturn> where TParam : notnull
{
    private readonly FlowType _flowType;
    private readonly StoredType _storedType;
    private readonly Invoker<TParam, TReturn> _invoker;
    private readonly InvocationHelper<TParam, TReturn> _invocationHelper;

    internal ControlPanelFactory(FlowType flowType, StoredType storedType, Invoker<TParam, TReturn> invoker, InvocationHelper<TParam, TReturn> invocationHelper)
    {
        _invoker = invoker;
        _invocationHelper = invocationHelper;
        _flowType = flowType;
        _storedType = storedType;
    }

    public async Task<ControlPanel<TParam, TReturn>?> Create(FlowInstance flowInstance)
    {
        var flowId = new FlowId(_flowType, flowInstance);
        var storedId = new StoredId(_storedType, flowInstance.Value.ToStoredInstance());
        var f = await _invocationHelper.GetFunction(storedId);
        if (f == null)
            return null;
        
        return new ControlPanel<TParam, TReturn>(
            _invoker,
            _invocationHelper,
            flowId,
            storedId,
            f.Status,
            f.Epoch,
            f.Expires,
            f.Param!,
            f.Result,
            _invocationHelper.CreateExistingEffects(flowId),
            _invocationHelper.CreateExistingStates(flowId),
            _invocationHelper.CreateExistingMessages(flowId),
            _invocationHelper.CreateExistingTimeouts(flowId),
            _invocationHelper.CreateCorrelations(flowId),
            f.PreviouslyThrownException
        );
    }
}