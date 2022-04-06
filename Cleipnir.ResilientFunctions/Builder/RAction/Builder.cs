using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.ParameterSerialization;

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

    public BuilderWithInner<TParam> WithInner<TParam>(Action<TParam> inner) where TParam : notnull
        => WithInner(CommonAdapters.ToInnerAction(inner));

    public BuilderWithInner<TParam> WithInner<TParam>(Func<TParam, Task> inner) where TParam : notnull
        => WithInner(CommonAdapters.ToInnerAction(inner));

    public BuilderWithInner<TParam> WithInner<TParam>(Func<TParam, Task<Result>> inner) where TParam : notnull 
        => new(_rFunctions, _functionTypeId, inner);
    
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

public class BuilderWithInner<TParam> where TParam : notnull
{
    private readonly RFunctions _rFunctions;
    private readonly FunctionTypeId _functionTypeId;
    private readonly Func<TParam, Task<Result>> _inner;

    public BuilderWithInner(RFunctions rFunctions, FunctionTypeId functionTypeId, Func<TParam, Task<Result>> inner)
    {
        _functionTypeId = functionTypeId;
        _inner = inner;
        _rFunctions = rFunctions;
    }

    public RAction<TParam> Register() => _rFunctions.Register(
        _functionTypeId,
        _inner,
        CommonAdapters.NoOpPreInvoke<TParam>(),
        CommonAdapters.NoOpPostInvoke<TParam>(),
        DefaultSerializer.Instance
    );

    public BuilderWithInnerWithPreInvoke<TParam> WithPreInvoke(Func<Metadata<TParam>, Task> preInvoke)
        => new(_rFunctions, _functionTypeId, _inner, preInvoke);

    public BuilderWithInnerWithPreInvoke<TParam> WithPreInvoke(Action<Metadata<TParam>> preInvoke)
        => WithPreInvoke(CommonAdapters.ToPreInvoke(preInvoke));
    
    public BuilderWithInnerWithPreAndPostInvoke<TParam> WithPostInvoke(Func<Result, Metadata<TParam>, Task<Result>> postInvoke)
        => new(_rFunctions, _functionTypeId, _inner, CommonAdapters.NoOpPreInvoke<TParam>(), postInvoke);

    public BuilderWithInnerWithPreAndPostInvoke<TParam> WithPostInvoke(Func<Result, Metadata<TParam>, Result> postInvoke)
        => WithPostInvoke(CommonAdapters.ToPostInvoke(postInvoke));

    public BuilderWithInnerWithPreAndPostInvokeAndSerializer<TParam> WithSerializer(ISerializer serializer)
        => new(
            _rFunctions,
            _functionTypeId,
            _inner,
            preInvoke: CommonAdapters.NoOpPreInvoke<TParam>(),
            postInvoke: CommonAdapters.NoOpPostInvoke<TParam>(),
            serializer
        );
}

public class BuilderWithInnerWithPreInvoke<TParam> where TParam : notnull
{
    private readonly RFunctions _rFunctions;
    private readonly FunctionTypeId _functionTypeId;
    private readonly Func<TParam, Task<Result>> _inner;
    private readonly Func<Metadata<TParam>, Task> _preInvoke;

    public BuilderWithInnerWithPreInvoke(
        RFunctions rFunctions, 
        FunctionTypeId functionTypeId, 
        Func<TParam, Task<Result>> inner, 
        Func<Metadata<TParam>, Task> preInvoke)
    {
        _functionTypeId = functionTypeId;
        _inner = inner;
        _preInvoke = preInvoke;
        _rFunctions = rFunctions;
    }
    
    public BuilderWithInnerWithPreAndPostInvoke<TParam> WithPostInvoke(Func<Result, Metadata<TParam>, Task<Result>> postInvoke)
        => new(_rFunctions, _functionTypeId, _inner, _preInvoke, postInvoke);

    public BuilderWithInnerWithPreAndPostInvoke<TParam> WithPostInvoke(Func<Result, Metadata<TParam>, Result> postInvoke)
        => WithPostInvoke(CommonAdapters.ToPostInvoke(postInvoke));

    public BuilderWithInnerWithPreAndPostInvokeAndSerializer<TParam> WithSerializer(ISerializer serializer)
        => new(
            _rFunctions,
            _functionTypeId,
            _inner,
            _preInvoke,
            postInvoke: CommonAdapters.NoOpPostInvoke<TParam>(),
            serializer
        );

    public RAction<TParam> Register() => _rFunctions.Register(
        _functionTypeId,
        _inner,
        _preInvoke,
        CommonAdapters.NoOpPostInvoke<TParam>(),
        DefaultSerializer.Instance
    );
}

public class BuilderWithInnerWithPreAndPostInvoke<TParam> where TParam : notnull
{
    private readonly RFunctions _rFunctions;
    private readonly FunctionTypeId _functionTypeId;
    private readonly Func<TParam, Task<Result>> _inner;
    private readonly Func<Metadata<TParam>, Task> _preInvoke;
    private readonly Func<Result, Metadata<TParam>, Task<Result>> _postInvoke;

    public BuilderWithInnerWithPreAndPostInvoke(
        RFunctions rFunctions, 
        FunctionTypeId functionTypeId, 
        Func<TParam, Task<Result>> inner, 
        Func<Metadata<TParam>, Task> preInvoke, 
        Func<Result, Metadata<TParam>, Task<Result>> postInvoke)
    {
        _functionTypeId = functionTypeId;
        _inner = inner;
        _preInvoke = preInvoke;
        _postInvoke = postInvoke;
        _rFunctions = rFunctions;
    }

    public BuilderWithInnerWithPreAndPostInvokeAndSerializer<TParam> WithSerializer(ISerializer serializer)
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
        _postInvoke,
        DefaultSerializer.Instance
    );
}

public class BuilderWithInnerWithPreAndPostInvokeAndSerializer<TParam> where TParam : notnull
{
    private readonly RFunctions _rFunctions;
    private readonly FunctionTypeId _functionTypeId;
    private readonly Func<TParam, Task<Result>> _inner;
    private readonly Func<Metadata<TParam>, Task> _preInvoke;
    private readonly Func<Result, Metadata<TParam>, Task<Result>> _postInvoke;
    private readonly ISerializer _serializer;

    public BuilderWithInnerWithPreAndPostInvokeAndSerializer(
        RFunctions rFunctions, 
        FunctionTypeId functionTypeId, 
        Func<TParam, Task<Result>> inner, 
        Func<Metadata<TParam>, Task> preInvoke, 
        Func<Result, Metadata<TParam>, Task<Result>> postInvoke, 
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