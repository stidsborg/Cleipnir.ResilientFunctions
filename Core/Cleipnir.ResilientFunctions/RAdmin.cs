using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Cleipnir.ResilientFunctions.Domain;

namespace Cleipnir.ResilientFunctions;

public class RAdmin<TParam, TScrapbook, TReturn> where TParam : notnull where TScrapbook : RScrapbook, new()
{
    private readonly InvocationHelper<TParam, TScrapbook, TReturn> _invocationHelper;
    private readonly FunctionTypeId _functionTypeId;

    internal RAdmin(FunctionTypeId functionTypeId, InvocationHelper<TParam, TScrapbook, TReturn> invocationHelper)
    {
        _functionTypeId = functionTypeId;
        _invocationHelper = invocationHelper;
    }

    public Task UpdateParameter(FunctionInstanceId id, Func<TParam, TParam> updater)
        => UpdateParameter(id, param => Task.FromResult(updater(param)));

    public Task UpdateParameter(FunctionInstanceId id, Func<TParam, Task<TParam>> updater)
        => _invocationHelper.UpdateParameter(new FunctionId(_functionTypeId, id), updater);
        
    public Task UpdateScrapbook(FunctionInstanceId id, Func<TScrapbook, TScrapbook> updater)
        => UpdateScrapbook(id, scrapbook => Task.FromResult(updater(scrapbook)));

    public Task UpdateScrapbook(FunctionInstanceId id, Func<TScrapbook, Task<TScrapbook>> updater)
        => _invocationHelper.UpdateScrapbook(new FunctionId(_functionTypeId, id), updater);
}