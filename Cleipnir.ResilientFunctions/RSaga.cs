using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;

namespace Cleipnir.ResilientFunctions;

public abstract class RSaga<TParam, TResult> where TParam : notnull where TResult : notnull
{
    public RFunc<TParam, TResult> Invoke { get; }
    
    protected RSaga(
        FunctionTypeId functionTypeId,
        Func<TParam, object> idFunc,
        RFunctions rFunctions)
    {
        Invoke = rFunctions.Register(
            functionTypeId,
            Func,
            idFunc
        );
    }
    
    protected abstract Task<RResult<TResult>> Func(TParam param);
}

public abstract class RSaga<TParam, TScrapbook, TResult> 
    where TParam : notnull 
    where TScrapbook : RScrapbook, new()
    where TResult : notnull
{
    public RFunc<TParam, TResult> Invoke { get; }
    
    protected RSaga(
        FunctionTypeId functionTypeId,
        Func<TParam, object> idFunc,
        RFunctions rFunctions)
    {
        Invoke = rFunctions.Register<TParam, TScrapbook, TResult>(
            functionTypeId,
            Func,
            idFunc
        );
    }

    protected abstract Task<RResult<TResult>> Func(TParam param, TScrapbook scrapbook);
}

public abstract class RSaga<TParam> where TParam : notnull
{
    public RAction<TParam> Invoke { get; }
    
    protected RSaga(
        FunctionTypeId functionTypeId,
        Func<TParam, object> idFunc,
        RFunctions rFunctions)
    {
        Invoke = rFunctions.Register(
            functionTypeId,
            Func,
            idFunc
        );
    }
    
    protected abstract Task<RResult> Func(TParam param);
}