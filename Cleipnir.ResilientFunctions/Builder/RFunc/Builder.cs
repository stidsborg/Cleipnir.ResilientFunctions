using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.ParameterSerialization;

namespace Cleipnir.ResilientFunctions.Builder.RFunc;

public class Builder
{
    private readonly RFunctions _rFunctions;
    private readonly FunctionTypeId _functionTypeId;

    public Builder(RFunctions rFunctions, FunctionTypeId functionTypeId)
    {
        _rFunctions = rFunctions;
        _functionTypeId = functionTypeId;
    }

    public BuilderWithInner<TParam, TScrapbook, TReturn> WithInner<TParam, TScrapbook, TReturn>(Func<TParam, TScrapbook, TReturn> inner) 
        where TParam : notnull where TScrapbook : RScrapbook, new()
        => WithInner(CommonAdapters.ToInnerFunc(inner));

    public BuilderWithInner<TParam, TScrapbook, TReturn> WithInner<TParam, TScrapbook, TReturn>(Func<TParam, TScrapbook, Result<TReturn>> inner) 
        where TParam : notnull where TScrapbook : RScrapbook, new()
        => WithInner(CommonAdapters.ToInnerFunc(inner));

    public BuilderWithInner<TParam, TScrapbook, TReturn> WithInner<TParam, TScrapbook, TReturn>(Func<TParam, TScrapbook, Task<TReturn>> inner) 
        where TParam : notnull where TScrapbook : RScrapbook, new() 
        => WithInner(CommonAdapters.ToInnerFunc(inner));
    
    public BuilderWithInner<TParam, TScrapbook, TReturn> WithInner<TParam, TScrapbook, TReturn>(Func<TParam, TScrapbook, Task<Result<TReturn>>> inner) 
        where TParam : notnull where TScrapbook : RScrapbook, new() 
        => new(_rFunctions, _functionTypeId, inner);
}

public class BuilderWithInnerWithPreAndPostInvokeAndSerializer<TParam, TReturn> where TParam : notnull
{
    private readonly RFunctions _rFunctions;
    private readonly FunctionTypeId _functionTypeId;
    private readonly Func<TParam, Task<Result<TReturn>>> _inner;
    private readonly Func<Metadata<TParam>, Task>? _preInvoke;
    private readonly Func<Result<TReturn>, Metadata<TParam>, Task<Result<TReturn>>>? _postInvoke;
    private readonly ISerializer? _serializer;

    public BuilderWithInnerWithPreAndPostInvokeAndSerializer(
        RFunctions rFunctions, 
        FunctionTypeId functionTypeId, 
        Func<TParam, Task<Result<TReturn>>> inner, 
        Func<Metadata<TParam>, Task>? preInvoke, 
        Func<Result<TReturn>, Metadata<TParam>, Task<Result<TReturn>>>? postInvoke, 
        ISerializer? serializer)
    {
        _rFunctions = rFunctions;
        _functionTypeId = functionTypeId;
        _inner = inner;
        _preInvoke = preInvoke;
        _postInvoke = postInvoke;
        _serializer = serializer;
    }
}