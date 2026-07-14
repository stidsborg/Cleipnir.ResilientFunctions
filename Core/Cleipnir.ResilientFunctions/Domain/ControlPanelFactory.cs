using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime;
using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.Domain;

public class ControlPanelFactory 
{
    private readonly FlowType _flowType;
    private readonly StoredType _storedType;
    private readonly InvocationHelper<Unit, Unit> _invocationHelper;
    private readonly UtcNow _utcNow;

    internal ControlPanelFactory(FlowType flowType, StoredType storedType, InvocationHelper<Unit, Unit> invocationHelper, UtcNow utcNow)
    {
        _invocationHelper = invocationHelper;
        _flowType = flowType;
        _storedType = storedType;
        _utcNow = utcNow;
    }

    public async Task<ControlPanel?> Create(FlowInstance flowInstance)
    {
        var flowId = new FlowId(_flowType, flowInstance);
        var storedId = StoredId.Create(_storedType, flowInstance.Value);
        var functionState = await _invocationHelper.GetFunction(storedId, flowId);
        if (functionState == null)
            return null;

        var existingEffects = await _invocationHelper.CreateExistingEffects(flowId);
        return new ControlPanel(
            _invocationHelper,
            flowId,
            storedId,
            functionState.Owner,
            functionState.Status,
            functionState.Expires,
            existingEffects,
            _invocationHelper.CreateExistingMessages(flowId),
            functionState.FatalWorkflowException,
            _utcNow
        );
    }
}


public class ControlPanelFactory<TParam> where TParam : notnull
{
    private readonly FlowType _flowType;
    private readonly StoredType _storedType;
    private readonly InvocationHelper<TParam, Unit> _invocationHelper;
    private readonly UtcNow _utcNow;

    internal ControlPanelFactory(FlowType flowType, StoredType storedType, InvocationHelper<TParam, Unit> invocationHelper, UtcNow utcNow)
    {
        _invocationHelper = invocationHelper;
        _flowType = flowType;
        _storedType = storedType;
        _utcNow = utcNow;
    }

    public async Task<ControlPanel<TParam>?> Create(FlowInstance flowInstance)
    {
        var flowId = new FlowId(_flowType, flowInstance);
        var storedId = StoredId.Create(_storedType, flowInstance.Value);
        var functionState = await _invocationHelper.GetFunction(storedId, flowId);
        if (functionState == null)
            return null;

        var existingEffects = await _invocationHelper.CreateExistingEffects(flowId);
        return new ControlPanel<TParam>(
            _invocationHelper,
            flowId,
            storedId,
            functionState.Owner,
            functionState.Status,
            functionState.Expires,
            functionState.Param!,
            existingEffects,
            _invocationHelper.CreateExistingMessages(flowId),
            functionState.FatalWorkflowException,
            _utcNow
        );
    }
}

public class ControlPanelFactory<TParam, TReturn> where TParam : notnull
{
    private readonly FlowType _flowType;
    private readonly StoredType _storedType;
    private readonly InvocationHelper<TParam, TReturn> _invocationHelper;
    private readonly UtcNow _utcNow;

    internal ControlPanelFactory(FlowType flowType, StoredType storedType, InvocationHelper<TParam, TReturn> invocationHelper, UtcNow utcNow)
    {
        _invocationHelper = invocationHelper;
        _flowType = flowType;
        _storedType = storedType;
        _utcNow = utcNow;
    }

    public async Task<ControlPanel<TParam, TReturn>?> Create(FlowInstance flowInstance)
    {
        var flowId = new FlowId(_flowType, flowInstance);
        var storedId = StoredId.Create(_storedType, flowInstance.Value);
        var functionState = await _invocationHelper.GetFunction(storedId, flowId);
        if (functionState == null)
            return null;

        var existingEffects = await _invocationHelper.CreateExistingEffects(flowId);
        return new ControlPanel<TParam, TReturn>(
            _invocationHelper,
            flowId,
            storedId,
            functionState.Owner,
            functionState.Status,
            functionState.Expires,
            functionState.Param!,
            functionState.Result,
            existingEffects,
            _invocationHelper.CreateExistingMessages(flowId),
            functionState.FatalWorkflowException,
            _utcNow
        );
    }
}