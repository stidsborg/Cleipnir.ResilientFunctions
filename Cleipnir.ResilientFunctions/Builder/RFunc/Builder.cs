﻿using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Invocation;
using Cleipnir.ResilientFunctions.ParameterSerialization;

namespace Cleipnir.ResilientFunctions.Builder.RFunc;

public class BuilderWithInner<TParam, TReturn> where TParam : notnull
{
    private readonly RFunctions _rFunctions;
    private readonly FunctionTypeId _functionTypeId;
    private readonly InnerFunc<TParam, TReturn> _inner;

    public BuilderWithInner(RFunctions rFunctions, FunctionTypeId functionTypeId, InnerFunc<TParam, TReturn> inner)
    {
        _functionTypeId = functionTypeId;
        _inner = inner;
        _rFunctions = rFunctions;
    }

    public RFunc<TParam, TReturn> Register() => _rFunctions.Register(_functionTypeId, _inner);

    public BuilderWithInnerWithPreInvoke<TParam, TReturn> WithPreInvoke(ResilientFunctions.RFunc.PreInvoke<TParam> preInvoke)
        => new(_rFunctions, _functionTypeId, _inner, preInvoke);

    public BuilderWithInnerWithPreInvoke<TParam, TReturn> WithPreInvoke(ResilientFunctions.RFunc.SyncPreInvoke<TParam> preInvoke)
        => new(
            _rFunctions,
            _functionTypeId,
            _inner,
            CommonInvoker.AsyncFuncPreInvoke(preInvoke)!
        );
    
    public BuilderWithInnerWithPreAndPostInvoke<TParam, TReturn> WithPostInvoke(ResilientFunctions.RFunc.PostInvoke<TParam, TReturn> postInvoke)
        => new(_rFunctions, _functionTypeId, _inner, preInvoke: null, postInvoke);

    public BuilderWithInnerWithPreAndPostInvoke<TParam, TReturn> WithPostInvoke(ResilientFunctions.RFunc.SyncPostInvoke<TParam, TReturn> postInvoke)
        => new(
            _rFunctions,
            _functionTypeId,
            _inner,
            preInvoke: null,
            CommonInvoker.AsyncFuncPostInvoke(postInvoke)
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
    private readonly InnerFunc<TParam, TReturn> _inner;
    private readonly ResilientFunctions.RFunc.PreInvoke<TParam> _preInvoke;

    public BuilderWithInnerWithPreInvoke(
        RFunctions rFunctions, 
        FunctionTypeId functionTypeId, 
        InnerFunc<TParam, TReturn> inner, 
        ResilientFunctions.RFunc.PreInvoke<TParam> preInvoke)
    {
        _functionTypeId = functionTypeId;
        _inner = inner;
        _preInvoke = preInvoke;
        _rFunctions = rFunctions;
    }
    
    public BuilderWithInnerWithPreAndPostInvoke<TParam, TReturn> WithPostInvoke(ResilientFunctions.RFunc.PostInvoke<TParam, TReturn> postInvoke)
        => new(_rFunctions, _functionTypeId, _inner, _preInvoke, postInvoke);

    public BuilderWithInnerWithPreAndPostInvoke<TParam, TReturn> WithPostInvoke(ResilientFunctions.RFunc.SyncPostInvoke<TParam, TReturn> postInvoke)
        => new(
            _rFunctions,
            _functionTypeId,
            _inner,
            _preInvoke,
            CommonInvoker.AsyncFuncPostInvoke(postInvoke)!
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
    private readonly InnerFunc<TParam, TReturn> _inner;
    private readonly ResilientFunctions.RFunc.PreInvoke<TParam>? _preInvoke;
    private readonly ResilientFunctions.RFunc.PostInvoke<TParam, TReturn>? _postInvoke;

    public BuilderWithInnerWithPreAndPostInvoke(
        RFunctions rFunctions, 
        FunctionTypeId functionTypeId, 
        InnerFunc<TParam, TReturn> inner, 
        ResilientFunctions.RFunc.PreInvoke<TParam>? preInvoke, 
        ResilientFunctions.RFunc.PostInvoke<TParam, TReturn>? postInvoke)
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
    private readonly InnerFunc<TParam, TReturn> _inner;
    private readonly ResilientFunctions.RFunc.PreInvoke<TParam>? _preInvoke;
    private readonly ResilientFunctions.RFunc.PostInvoke<TParam, TReturn>? _postInvoke;
    private readonly ISerializer? _serializer;

    public BuilderWithInnerWithPreAndPostInvokeAndSerializer(
        RFunctions rFunctions, 
        FunctionTypeId functionTypeId, 
        InnerFunc<TParam, TReturn> inner, 
        ResilientFunctions.RFunc.PreInvoke<TParam>? preInvoke, 
        ResilientFunctions.RFunc.PostInvoke<TParam, TReturn>? postInvoke, 
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