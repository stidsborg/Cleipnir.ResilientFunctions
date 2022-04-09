﻿using System;
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

    public BuilderWithInner<TParam, TReturn> WithInner<TParam, TReturn>(Func<TParam, TReturn> inner) where TParam : notnull
        => WithInner(CommonAdapters.ToInnerFunc(inner));

    public BuilderWithInner<TParam, TReturn> WithInner<TParam, TReturn>(Func<TParam, Result<TReturn>> inner) where TParam : notnull
        => WithInner(CommonAdapters.ToInnerFunc(inner));

    public BuilderWithInner<TParam, TReturn> WithInner<TParam, TReturn>(Func<TParam, Task<TReturn>> inner) where TParam : notnull 
        => WithInner(CommonAdapters.ToInnerFunc(inner));
    
    public BuilderWithInner<TParam, TReturn> WithInner<TParam, TReturn>(Func<TParam, Task<Result<TReturn>>> inner) where TParam : notnull 
        => new(_rFunctions, _functionTypeId, inner);
    
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

public class BuilderWithInner<TParam, TReturn> where TParam : notnull
{
    private readonly RFunctions _rFunctions;
    private readonly FunctionTypeId _functionTypeId;
    private readonly Func<TParam, Task<Result<TReturn>>> _inner;

    public BuilderWithInner(RFunctions rFunctions, FunctionTypeId functionTypeId, Func<TParam, Task<Result<TReturn>>> inner)
    {
        _functionTypeId = functionTypeId;
        _inner = inner;
        _rFunctions = rFunctions;
    }

    public RFunc<TParam, TReturn> Register() => _rFunctions.Register(_functionTypeId, _inner);

    public BuilderWithInnerWithPreInvoke<TParam, TReturn> WithPreInvoke(Func<Metadata<TParam>, Task> preInvoke)
        => new(_rFunctions, _functionTypeId, _inner, preInvoke);

    public BuilderWithInnerWithPreInvoke<TParam, TReturn> WithPreInvoke(Action<Metadata<TParam>> preInvoke)
        => new(
            _rFunctions,
            _functionTypeId,
            _inner,
            CommonAdapters.ToAsyncPreInvoke(preInvoke)
        );
    
    public BuilderWithInnerWithPreAndPostInvoke<TParam, TReturn> WithPostInvoke(Func<Result<TReturn>, Metadata<TParam>, Task<Result<TReturn>>> postInvoke)
        => new(_rFunctions, _functionTypeId, _inner, preInvoke: null, postInvoke);

    public BuilderWithInnerWithPreAndPostInvoke<TParam, TReturn> WithPostInvoke(Func<Result<TReturn>, Metadata<TParam>, Result<TReturn>> postInvoke)
        => new(
            _rFunctions,
            _functionTypeId,
            _inner,
            preInvoke: null,
            CommonAdapters.ToAsyncPostInvoke(postInvoke)
        );

    public BuilderWithInnerWithPreAndPostInvokeAndSerializer<TParam, TReturn> WithSerializer(ISerializer serializer)
        => new(
            _rFunctions,
            _functionTypeId,
            _inner,
            preInvoke: null,
            postInvoke: null,
            serializer
        );
}

public class BuilderWithInnerWithPreInvoke<TParam, TReturn> where TParam : notnull
{
    private readonly RFunctions _rFunctions;
    private readonly FunctionTypeId _functionTypeId;
    private readonly Func<TParam, Task<Result<TReturn>>> _inner;
    private readonly Func<Metadata<TParam>, Task> _preInvoke;

    public BuilderWithInnerWithPreInvoke(
        RFunctions rFunctions, 
        FunctionTypeId functionTypeId, 
        Func<TParam, Task<Result<TReturn>>> inner, 
        Func<Metadata<TParam>, Task> preInvoke)
    {
        _functionTypeId = functionTypeId;
        _inner = inner;
        _preInvoke = preInvoke;
        _rFunctions = rFunctions;
    }
    
    public BuilderWithInnerWithPreAndPostInvoke<TParam, TReturn> WithPostInvoke(Func<Result<TReturn>, Metadata<TParam>, Task<Result<TReturn>>> postInvoke)
        => new(_rFunctions, _functionTypeId, _inner, _preInvoke, postInvoke);

    public BuilderWithInnerWithPreAndPostInvoke<TParam, TReturn> WithPostInvoke(Func<Result<TReturn>, Metadata<TParam>, Result<TReturn>> postInvoke)
        => new(
            _rFunctions,
            _functionTypeId,
            _inner,
            _preInvoke,
            CommonAdapters.ToAsyncPostInvoke(postInvoke)
        );

    public BuilderWithInnerWithPreAndPostInvokeAndSerializer<TParam, TReturn> WithSerializer(ISerializer serializer)
        => new(
            _rFunctions,
            _functionTypeId,
            _inner,
            _preInvoke,
            postInvoke: null,
            serializer
        );

    public RFunc<TParam, TReturn> Register() => _rFunctions.Register(
        _functionTypeId,
        _inner,
        _preInvoke
    );
}

public class BuilderWithInnerWithPreAndPostInvoke<TParam, TReturn> where TParam : notnull
{
    private readonly RFunctions _rFunctions;
    private readonly FunctionTypeId _functionTypeId;
    private readonly Func<TParam, Task<Result<TReturn>>> _inner;
    private readonly Func<Metadata<TParam>, Task>? _preInvoke;
    private readonly Func<Result<TReturn>, Metadata<TParam>, Task<Result<TReturn>>>? _postInvoke;

    public BuilderWithInnerWithPreAndPostInvoke(
        RFunctions rFunctions, 
        FunctionTypeId functionTypeId, 
        Func<TParam, Task<Result<TReturn>>> inner, 
        Func<Metadata<TParam>, Task>? preInvoke, 
        Func<Result<TReturn>, Metadata<TParam>, Task<Result<TReturn>>>? postInvoke)
    {
        _functionTypeId = functionTypeId;
        _inner = inner;
        _preInvoke = preInvoke;
        _postInvoke = postInvoke;
        _rFunctions = rFunctions;
    }

    public BuilderWithInnerWithPreAndPostInvokeAndSerializer<TParam, TReturn> WithSerializer(ISerializer serializer)
        => new(
            _rFunctions,
            _functionTypeId,
            _inner,
            _preInvoke,
            _postInvoke,
            serializer
        );
    
    public RFunc<TParam, TReturn> Register() => _rFunctions.Register(
        _functionTypeId,
        _inner,
        _preInvoke,
        _postInvoke
    );
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
    
    public RFunc<TParam, TReturn> Register() => _rFunctions.Register(
        _functionTypeId,
        _inner,
        _preInvoke,
        _postInvoke,
        _serializer
    );
}