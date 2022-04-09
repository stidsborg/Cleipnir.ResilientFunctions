using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;

namespace Cleipnir.ResilientFunctions.Builder.RAction;

public class Builder
{
    private readonly RFunctions _rFunctions;
    private readonly FunctionTypeId _functionTypeId;

    public Builder(RFunctions rFunctions, FunctionTypeId functionTypeId)
    {
        _rFunctions = rFunctions;
        _functionTypeId = functionTypeId;
    }

    public BuilderWithInner<TParam, TScrapbook> WithInner<TParam, TScrapbook>(Action<TParam, TScrapbook> inner) 
        where TParam : notnull where TScrapbook : RScrapbook, new()
        => WithInner(CommonAdapters.ToInnerAction(inner));

    public BuilderWithInner<TParam, TScrapbook> WithInner<TParam, TScrapbook>(Func<TParam, TScrapbook, Task> inner) 
        where TParam : notnull where TScrapbook : RScrapbook, new()
        => WithInner(CommonAdapters.ToInnerAction(inner));

    public BuilderWithInner<TParam, TScrapbook> WithInner<TParam, TScrapbook>(Func<TParam, TScrapbook, Task<Result>> inner) 
        where TParam : notnull where TScrapbook : RScrapbook, new() 
        => new(_rFunctions, _functionTypeId, inner);
}