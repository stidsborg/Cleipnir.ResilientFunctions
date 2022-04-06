using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.ParameterSerialization;

namespace Cleipnir.ResilientFunctions.Builder.RAction;

public class BuilderWithInner<TParam, TScrapbook> where TParam : notnull where TScrapbook : RScrapbook, new()
{
    private readonly RFunctions _rFunctions;
    private readonly FunctionTypeId _functionTypeId;
    private readonly Func<TParam, TScrapbook, Task<Result>> _inner;

    public BuilderWithInner(RFunctions rFunctions, FunctionTypeId functionTypeId, Func<TParam, TScrapbook, Task<Result>> inner)
    {
        _functionTypeId = functionTypeId;
        _inner = inner;
        _rFunctions = rFunctions;
    }

    public RAction<TParam> Register() => _rFunctions.Register(_functionTypeId, _inner);

    public BuilderWithInnerWithPreInvoke<TParam, TScrapbook> WithPreInvoke(Func<TScrapbook, Metadata<TParam>, Task> preInvoke)
        => new(_rFunctions, _functionTypeId, _inner, preInvoke);

    public BuilderWithInnerWithPreInvoke<TParam, TScrapbook> WithPreInvoke(Action<TScrapbook, Metadata<TParam>> preInvoke)
        => new(
            _rFunctions,
            _functionTypeId,
            _inner,
            CommonAdapters.ToPreInvoke(preInvoke)
        );
    
    public BuilderWithInnerWithPreAndPostInvoke<TParam, TScrapbook> WithPostInvoke(Func<Result, TScrapbook, Metadata<TParam>, Task<Result>> postInvoke)
        => new(_rFunctions, _functionTypeId, _inner, preInvoke: CommonAdapters.NoOpPreInvoke<TParam, TScrapbook>(), postInvoke);

    public BuilderWithInnerWithPreAndPostInvoke<TParam, TScrapbook> WithPostInvoke(Func<Result, TScrapbook, Metadata<TParam>, Result> postInvoke)
        => new(
            _rFunctions,
            _functionTypeId,
            _inner,
            preInvoke: CommonAdapters.NoOpPreInvoke<TParam, TScrapbook>(),
            CommonAdapters.ToPostInvoke(postInvoke)
        );

    public BuilderWithInnerWithPreAndPostInvokeAndSerializer<TParam, TScrapbook> WithSerializer(ISerializer serializer)
        => new(
            _rFunctions,
            _functionTypeId,
            _inner,
            preInvoke: CommonAdapters.NoOpPreInvoke<TParam, TScrapbook>(),
            postInvoke: CommonAdapters.NoOpPostInvoke<TParam, TScrapbook>(),
            serializer
        );
}

public class BuilderWithInnerWithPreInvoke<TParam, TScrapbook> where TParam : notnull where TScrapbook : RScrapbook, new()
{
    private readonly RFunctions _rFunctions;
    private readonly FunctionTypeId _functionTypeId;
    private readonly Func<TParam, TScrapbook, Task<Result>> _inner;
    private readonly Func<TScrapbook, Metadata<TParam>, Task> _preInvoke;

    public BuilderWithInnerWithPreInvoke(
        RFunctions rFunctions, 
        FunctionTypeId functionTypeId, 
        Func<TParam, TScrapbook, Task<Result>> inner, 
        Func<TScrapbook, Metadata<TParam>, Task> preInvoke)
    {
        _functionTypeId = functionTypeId;
        _inner = inner;
        _preInvoke = preInvoke;
        _rFunctions = rFunctions;
    }
    
    public BuilderWithInnerWithPreAndPostInvoke<TParam, TScrapbook> WithPostInvoke(Func<Result, TScrapbook, Metadata<TParam>, Task<Result>> postInvoke)
        => new(_rFunctions, _functionTypeId, _inner, _preInvoke, postInvoke);

    public BuilderWithInnerWithPreAndPostInvoke<TParam, TScrapbook> WithPostInvoke(Func<Result, TScrapbook, Metadata<TParam>, Result> postInvoke)
        => new(
            _rFunctions,
            _functionTypeId,
            _inner,
            _preInvoke,
            CommonAdapters.ToPostInvoke(postInvoke)
        );

    public BuilderWithInnerWithPreAndPostInvokeAndSerializer<TParam, TScrapbook> WithSerializer(ISerializer serializer)
        => new(
            _rFunctions,
            _functionTypeId,
            _inner,
            _preInvoke,
            postInvoke: CommonAdapters.NoOpPostInvoke<TParam, TScrapbook>(),
            serializer
        );

    public RAction<TParam> Register() => _rFunctions.Register(
        _functionTypeId,
        _inner,
        _preInvoke
    );
}

public class BuilderWithInnerWithPreAndPostInvoke<TParam, TScrapbook> where TParam : notnull where TScrapbook : RScrapbook, new()
{
    private readonly RFunctions _rFunctions;
    private readonly FunctionTypeId _functionTypeId;
    private readonly Func<TParam, TScrapbook, Task<Result>> _inner;
    private readonly Func<TScrapbook, Metadata<TParam>, Task> _preInvoke;
    private readonly Func<Result, TScrapbook, Metadata<TParam>, Task<Result>> _postInvoke;

    public BuilderWithInnerWithPreAndPostInvoke(
        RFunctions rFunctions, 
        FunctionTypeId functionTypeId, 
        Func<TParam, TScrapbook, Task<Result>> inner, 
        Func<TScrapbook, Metadata<TParam>, Task> preInvoke, 
        Func<Result, TScrapbook, Metadata<TParam>, Task<Result>> postInvoke)
    {
        _functionTypeId = functionTypeId;
        _inner = inner;
        _preInvoke = preInvoke;
        _postInvoke = postInvoke;
        _rFunctions = rFunctions;
    }

    public BuilderWithInnerWithPreAndPostInvokeAndSerializer<TParam, TScrapbook> WithSerializer(ISerializer serializer)
        => new(
            _rFunctions,
            _functionTypeId,
            _inner,
            _preInvoke,
            _postInvoke,
            serializer
        );
    
    public RAction<TParam> Register() => _rFunctions.Register(
        _functionTypeId,
        _inner,
        _preInvoke,
        _postInvoke
    );
}

public class BuilderWithInnerWithPreAndPostInvokeAndSerializer<TParam, TScrapbook> where TParam : notnull where TScrapbook : RScrapbook, new()
{
    private readonly RFunctions _rFunctions;
    private readonly FunctionTypeId _functionTypeId;
    private readonly Func<TParam, TScrapbook, Task<Result>> _inner;
    private readonly Func<TScrapbook, Metadata<TParam>, Task> _preInvoke;
    private readonly Func<Result, TScrapbook, Metadata<TParam>, Task<Result>> _postInvoke;
    private readonly ISerializer _serializer;

    public BuilderWithInnerWithPreAndPostInvokeAndSerializer(
        RFunctions rFunctions, 
        FunctionTypeId functionTypeId, 
        Func<TParam, TScrapbook, Task<Result>> inner, 
        Func<TScrapbook, Metadata<TParam>, Task> preInvoke, 
        Func<Result, TScrapbook, Metadata<TParam>, Task<Result>> postInvoke, 
        ISerializer serializer)
    {
        _rFunctions = rFunctions;
        _functionTypeId = functionTypeId;
        _inner = inner;
        _preInvoke = preInvoke;
        _postInvoke = postInvoke;
        _serializer = serializer;
    }
    
    public RAction<TParam> Register() => _rFunctions.Register(
        _functionTypeId,
        _inner,
        _preInvoke,
        _postInvoke,
        _serializer
    );
}