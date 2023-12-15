using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Cleipnir.ResilientFunctions.Helpers;

namespace Cleipnir.ResilientFunctions.Domain;

public class ControlPanels<TParam, TScrapbook> where TParam : notnull where TScrapbook : RScrapbook, new()
{
    private readonly FunctionTypeId _functionTypeId;
    private readonly Invoker<TParam, TScrapbook, Unit> _invoker;
    private readonly InvocationHelper<TParam, TScrapbook, Unit> _invocationHelper;

    internal ControlPanels(FunctionTypeId functionTypeId, Invoker<TParam, TScrapbook, Unit> invoker, InvocationHelper<TParam, TScrapbook, Unit> invocationHelper)
    {
        _invoker = invoker;
        _invocationHelper = invocationHelper;
        _functionTypeId = functionTypeId;
    }
    
    public async Task<ControlPanel<TParam, TScrapbook>?> For(FunctionInstanceId functionInstanceId)
    {
        var functionId = new FunctionId(_functionTypeId, functionInstanceId);
        var functionState = await _invocationHelper.GetFunction(functionId);
        if (functionState == null)
            return null;
        
        return new ControlPanel<TParam, TScrapbook>(
            _invoker,
            _invocationHelper,
            functionId,
            functionState.Status,
            functionState.Epoch,
            functionState.LeaseExpiration,
            functionState.Param,
            functionState.Scrapbook,
            functionState.PostponedUntil,
            await _invocationHelper.GetExistingActivities(functionId),
            await _invocationHelper.GetExistingEvents(functionId),
            functionState.PreviouslyThrownException
        );
    }
}

public class ControlPanels<TParam, TScrapbook, TReturn> where TParam : notnull where TScrapbook : RScrapbook, new()
{
    public delegate Task<TReturn> ReInvoke(string functionInstanceId, int expectedEpoch);
    public delegate Task ScheduleReInvoke(string functionInstanceId, int expectedEpoch);
    
    private readonly FunctionTypeId _functionTypeId;
    private readonly Invoker<TParam, TScrapbook, TReturn> _invoker;
    private readonly InvocationHelper<TParam, TScrapbook, TReturn> _invocationHelper;

    internal ControlPanels(FunctionTypeId functionTypeId, Invoker<TParam, TScrapbook, TReturn> invoker, InvocationHelper<TParam, TScrapbook, TReturn> invocationHelper)
    {
        _invoker = invoker;
        _invocationHelper = invocationHelper;
        _functionTypeId = functionTypeId;
    }

    public async Task<ControlPanel<TParam, TScrapbook, TReturn>?> For(FunctionInstanceId functionInstanceId)
    {
        var functionId = new FunctionId(_functionTypeId, functionInstanceId);
        var f = await _invocationHelper.GetFunction(functionId);
        if (f == null)
            return null;
        
        return new ControlPanel<TParam, TScrapbook, TReturn>(
            _invoker,
            _invocationHelper,
            functionId,
            f.Status,
            f.Epoch,
            f.LeaseExpiration,
            f.Param,
            f.Scrapbook,
            f.Result,
            f.PostponedUntil,
            await _invocationHelper.GetExistingActivities(functionId),
            await _invocationHelper.GetExistingEvents(functionId),
            f.PreviouslyThrownException
        );
    }
}