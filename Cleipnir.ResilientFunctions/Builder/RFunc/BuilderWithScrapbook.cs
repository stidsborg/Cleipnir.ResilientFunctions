using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Invocation;
using Cleipnir.ResilientFunctions.ParameterSerialization;

namespace Cleipnir.ResilientFunctions.Builder.RFunc;

public class BuilderWithInner<TParam, TScrapbook, TReturn> where TParam : notnull where TScrapbook : RScrapbook, new()
{
    private readonly RFunctions _rFunctions;
    private readonly FunctionTypeId _functionTypeId;
    private readonly Func<TParam, TScrapbook, Task<Result<TReturn>>> _inner;

    public BuilderWithInner(RFunctions rFunctions, FunctionTypeId functionTypeId, Func<TParam, TScrapbook, Task<Result<TReturn>>> inner)
    {
        _functionTypeId = functionTypeId;
        _inner = inner;
        _rFunctions = rFunctions;
    }

    public RFunc<TParam, TReturn> Register() => _rFunctions.Register(_functionTypeId, _inner);

    public BuilderWithInnerWithPreInvoke<TParam, TScrapbook, TReturn> WithPreInvoke(Func<TScrapbook, Metadata<TParam>, Task> preInvoke)
        => new(_rFunctions, _functionTypeId, _inner, preInvoke);

    public BuilderWithInnerWithPreInvoke<TParam, TScrapbook, TReturn> WithPreInvoke(Action<TScrapbook, Metadata<TParam>> preInvoke)
        => new(
            _rFunctions,
            _functionTypeId,
            _inner,
            CommonAdapters.ToAsyncPreInvoke(preInvoke)
        );
    
    public BuilderWithInnerWithPreAndPostInvoke<TParam, TScrapbook, TReturn> WithPostInvoke(Func<Result<TReturn>, TScrapbook, Metadata<TParam>, Task<Result<TReturn>>> postInvoke)
        => new(_rFunctions, _functionTypeId, _inner, preInvoke: null, postInvoke);

    public BuilderWithInnerWithPreAndPostInvoke<TParam, TScrapbook, TReturn> WithPostInvoke(Func<Result<TReturn>, TScrapbook, Metadata<TParam>, Result<TReturn>> postInvoke)
        => new(
            _rFunctions,
            _functionTypeId,
            _inner,
            preInvoke: null,
            CommonAdapters.ToAsyncPostInvoke(postInvoke)
        );

    public BuilderWithInnerWithPreAndPostInvokeAndSerializer<TParam, TScrapbook, TReturn> WithSerializer(ISerializer serializer)
        => new(
            _rFunctions,
            _functionTypeId,
            _inner,
            preInvoke: null,
            postInvoke: null,
            serializer
        );
}

public class BuilderWithInnerWithPreInvoke<TParam, TScrapbook, TReturn> where TParam : notnull where TScrapbook : RScrapbook, new()
{
    private readonly RFunctions _rFunctions;
    private readonly FunctionTypeId _functionTypeId;
    private readonly Func<TParam, TScrapbook, Task<Result<TReturn>>> _inner;
    private readonly Func<TScrapbook, Metadata<TParam>, Task>? _preInvoke;

    public BuilderWithInnerWithPreInvoke(
        RFunctions rFunctions, 
        FunctionTypeId functionTypeId, 
        Func<TParam, TScrapbook, Task<Result<TReturn>>> inner, 
        Func<TScrapbook, Metadata<TParam>, Task>? preInvoke)
    {
        _functionTypeId = functionTypeId;
        _inner = inner;
        _preInvoke = preInvoke;
        _rFunctions = rFunctions;
    }
    
    public BuilderWithInnerWithPreAndPostInvoke<TParam, TScrapbook, TReturn> WithPostInvoke(Func<Result<TReturn>, TScrapbook, Metadata<TParam>, Task<Result<TReturn>>> postInvoke)
        => new(_rFunctions, _functionTypeId, _inner, _preInvoke, postInvoke);

    public BuilderWithInnerWithPreAndPostInvoke<TParam, TScrapbook, TReturn> WithPostInvoke(Func<Result<TReturn>, TScrapbook, Metadata<TParam>, Result<TReturn>> postInvoke)
        => new(
            _rFunctions,
            _functionTypeId,
            _inner,
            _preInvoke,
            CommonAdapters.ToAsyncPostInvoke(postInvoke)
        );

    public BuilderWithInnerWithPreAndPostInvokeAndSerializer<TParam, TScrapbook, TReturn> WithSerializer(ISerializer serializer)
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

public class BuilderWithInnerWithPreAndPostInvoke<TParam, TScrapbook, TReturn> where TParam : notnull where TScrapbook : RScrapbook, new()
{
    private readonly RFunctions _rFunctions;
    private readonly FunctionTypeId _functionTypeId;
    private readonly Func<TParam, TScrapbook, Task<Result<TReturn>>> _inner;
    private readonly Func<TScrapbook, Metadata<TParam>, Task>? _preInvoke;
    private readonly Func<Result<TReturn>, TScrapbook, Metadata<TParam>, Task<Result<TReturn>>>? _postInvoke;

    public BuilderWithInnerWithPreAndPostInvoke(
        RFunctions rFunctions, 
        FunctionTypeId functionTypeId, 
        Func<TParam, TScrapbook, Task<Result<TReturn>>> inner, 
        Func<TScrapbook, Metadata<TParam>, Task>? preInvoke, 
        Func<Result<TReturn>, TScrapbook, Metadata<TParam>, Task<Result<TReturn>>>? postInvoke)
    {
        _functionTypeId = functionTypeId;
        _inner = inner;
        _preInvoke = preInvoke;
        _postInvoke = postInvoke;
        _rFunctions = rFunctions;
    }

    public BuilderWithInnerWithPreAndPostInvokeAndSerializer<TParam, TScrapbook, TReturn> WithSerializer(ISerializer serializer)
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

public class BuilderWithInnerWithPreAndPostInvokeAndSerializer<TParam, TScrapbook, TReturn> where TParam : notnull where TScrapbook : RScrapbook, new()
{
    private readonly RFunctions _rFunctions;
    private readonly FunctionTypeId _functionTypeId;
    private readonly Func<TParam, TScrapbook, Task<Result<TReturn>>> _inner;
    private readonly Func<TScrapbook, Metadata<TParam>, Task>? _preInvoke;
    private readonly Func<Result<TReturn>, TScrapbook, Metadata<TParam>, Task<Result<TReturn>>>? _postInvoke;
    private readonly ISerializer? _serializer;

    public BuilderWithInnerWithPreAndPostInvokeAndSerializer(
        RFunctions rFunctions, 
        FunctionTypeId functionTypeId, 
        Func<TParam, TScrapbook, Task<Result<TReturn>>> inner, 
        Func<TScrapbook, Metadata<TParam>, Task>? preInvoke, 
        Func<Result<TReturn>, TScrapbook, Metadata<TParam>, Task<Result<TReturn>>>? postInvoke, 
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