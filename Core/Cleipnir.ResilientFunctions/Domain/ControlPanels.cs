using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Cleipnir.ResilientFunctions.Helpers;

namespace Cleipnir.ResilientFunctions.Domain;

public class ControlPanels<TParam, TScrapbook> where TParam : notnull where TScrapbook : RScrapbook, new()
{
    private readonly FunctionTypeId _functionTypeId;
    private readonly InvocationHelper<TParam, TScrapbook, Unit> _invocationHelper;
    private readonly RAction.ReInvoke _reInvoke;
    private readonly RAction.ScheduleReInvoke _scheduleReInvoke;

    internal ControlPanels(
        FunctionTypeId functionTypeId, 
        InvocationHelper<TParam, TScrapbook, Unit> invocationHelper, 
        RAction.ReInvoke reInvoke, 
        RAction.ScheduleReInvoke scheduleReInvoke)
    {
        _invocationHelper = invocationHelper;
        _reInvoke = reInvoke;
        _scheduleReInvoke = scheduleReInvoke;
        _functionTypeId = functionTypeId;
    }
    
    public async Task<ControlPanel<TParam, TScrapbook>?> For(FunctionInstanceId functionInstanceId)
    {
        var functionId = new FunctionId(_functionTypeId, functionInstanceId);
        var f = await _invocationHelper.GetFunction(functionId);
        if (f == null)
            return null;
        
        return new ControlPanel<TParam, TScrapbook>(
            _invocationHelper,
            _reInvoke,
            _scheduleReInvoke,
            functionId,
            f.Status,
            f.Epoch,
            f.CrashedCheckFrequency,
            f.Param,
            f.Scrapbook,
            f.PostponedUntil,
            f.PreviouslyThrownException
        );
    }
}

public class ControlPanels<TParam, TScrapbook, TReturn> where TParam : notnull where TScrapbook : RScrapbook, new()
{
    private readonly FunctionTypeId _functionTypeId;
    private readonly InvocationHelper<TParam, TScrapbook, TReturn> _invocationHelper;
    private readonly RFunc.ReInvoke<TReturn> _reInvoke;
    private readonly RFunc.ScheduleReInvoke _scheduleReInvoke;

    internal ControlPanels(
        FunctionTypeId functionTypeId, 
        InvocationHelper<TParam, TScrapbook, TReturn> invocationHelper, 
        RFunc.ReInvoke<TReturn> reInvoke, 
        RFunc.ScheduleReInvoke scheduleReInvoke)
    {
        _invocationHelper = invocationHelper;
        _reInvoke = reInvoke;
        _scheduleReInvoke = scheduleReInvoke;
        _functionTypeId = functionTypeId;
    }

    public async Task<ControlPanel<TParam, TScrapbook, TReturn>?> For(FunctionInstanceId functionInstanceId)
    {
        var functionId = new FunctionId(_functionTypeId, functionInstanceId);
        var f = await _invocationHelper.GetFunction(functionId);
        if (f == null)
            return null;
        
        return new ControlPanel<TParam, TScrapbook, TReturn>(
            _invocationHelper,
            _reInvoke,
            _scheduleReInvoke,
            functionId,
            f.Status,
            f.Epoch,
            f.CrashedCheckFrequency,
            f.Param,
            f.Scrapbook,
            f.Result,
            f.PostponedUntil,
            f.PreviouslyThrownException
        );
    }
}