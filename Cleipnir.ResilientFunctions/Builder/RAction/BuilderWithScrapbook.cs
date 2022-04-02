using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Invocation;
using Cleipnir.ResilientFunctions.ParameterSerialization;

namespace Cleipnir.ResilientFunctions.Builder.RAction;

public class BuilderWithInner<TParam, TScrapbook> where TParam : notnull where TScrapbook : RScrapbook, new()
{
    private readonly RFunctions _rFunctions;
    private readonly FunctionTypeId _functionTypeId;
    private readonly InnerAction<TParam, TScrapbook> _inner;

    public BuilderWithInner(RFunctions rFunctions, FunctionTypeId functionTypeId, InnerAction<TParam, TScrapbook> inner)
    {
        _functionTypeId = functionTypeId;
        _inner = inner;
        _rFunctions = rFunctions;
    }

    public RAction<TParam> Register() => _rFunctions.Register(_functionTypeId, _inner);

    public BuilderWithInnerWithPreInvoke<TParam, TScrapbook> WithPreInvoke(ResilientFunctions.RAction.PreInvoke<TParam, TScrapbook> preInvoke)
        => new(_rFunctions, _functionTypeId, _inner, preInvoke);

    public BuilderWithInnerWithPreInvoke<TParam, TScrapbook> WithPreInvoke(ResilientFunctions.RAction.SyncPreInvoke<TParam, TScrapbook> preInvoke)
        => new(
            _rFunctions,
            _functionTypeId,
            _inner,
            CommonInvoker.SyncActionPreInvoke(preInvoke)
        );
    
    public BuilderWithInnerWithPreAndPostInvoke<TParam, TScrapbook> WithPostInvoke(ResilientFunctions.RAction.PostInvoke<TParam, TScrapbook> postInvoke)
        => new(_rFunctions, _functionTypeId, _inner, preInvoke: null, postInvoke);

    public BuilderWithInnerWithPreAndPostInvoke<TParam, TScrapbook> WithPostInvoke(ResilientFunctions.RAction.SyncPostInvoke<TParam, TScrapbook> postInvoke)
        => new(
            _rFunctions,
            _functionTypeId,
            _inner,
            preInvoke: null,
            CommonInvoker.AsyncActionPostInvoke(postInvoke)
        );

    public BuilderWithInnerWithPreAndPostInvokeAndSerializer<TParam, TScrapbook> WithSerializer(ISerializer serializer)
        => new(
            _rFunctions,
            _functionTypeId,
            _inner,
            preInvoke: null,
            postInvoke: null,
            serializer
        );
}

public class BuilderWithInnerWithPreInvoke<TParam, TScrapbook> where TParam : notnull where TScrapbook : RScrapbook, new()
{
    private readonly RFunctions _rFunctions;
    private readonly FunctionTypeId _functionTypeId;
    private readonly InnerAction<TParam, TScrapbook> _inner;
    private readonly ResilientFunctions.RAction.PreInvoke<TParam, TScrapbook>? _preInvoke;

    public BuilderWithInnerWithPreInvoke(
        RFunctions rFunctions, 
        FunctionTypeId functionTypeId, 
        InnerAction<TParam, TScrapbook> inner, 
        ResilientFunctions.RAction.PreInvoke<TParam, TScrapbook>? preInvoke)
    {
        _functionTypeId = functionTypeId;
        _inner = inner;
        _preInvoke = preInvoke;
        _rFunctions = rFunctions;
    }
    
    public BuilderWithInnerWithPreAndPostInvoke<TParam, TScrapbook> WithPostInvoke(ResilientFunctions.RAction.PostInvoke<TParam, TScrapbook> postInvoke)
        => new(_rFunctions, _functionTypeId, _inner, _preInvoke, postInvoke);

    public BuilderWithInnerWithPreAndPostInvoke<TParam, TScrapbook> WithPostInvoke(ResilientFunctions.RAction.SyncPostInvoke<TParam, TScrapbook> postInvoke)
        => new(
            _rFunctions,
            _functionTypeId,
            _inner,
            _preInvoke,
            CommonInvoker.AsyncActionPostInvoke(postInvoke)
        );

    public BuilderWithInnerWithPreAndPostInvokeAndSerializer<TParam, TScrapbook> WithSerializer(ISerializer serializer)
        => new(
            _rFunctions,
            _functionTypeId,
            _inner,
            _preInvoke,
            postInvoke: null,
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
    private readonly InnerAction<TParam, TScrapbook> _inner;
    private readonly ResilientFunctions.RAction.PreInvoke<TParam, TScrapbook>? _preInvoke;
    private readonly ResilientFunctions.RAction.PostInvoke<TParam, TScrapbook>? _postInvoke;

    public BuilderWithInnerWithPreAndPostInvoke(
        RFunctions rFunctions, 
        FunctionTypeId functionTypeId, 
        InnerAction<TParam, TScrapbook> inner, 
        ResilientFunctions.RAction.PreInvoke<TParam, TScrapbook>? preInvoke, 
        ResilientFunctions.RAction.PostInvoke<TParam, TScrapbook>? postInvoke)
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
    private readonly InnerAction<TParam, TScrapbook> _inner;
    private readonly ResilientFunctions.RAction.PreInvoke<TParam, TScrapbook>? _preInvoke;
    private readonly ResilientFunctions.RAction.PostInvoke<TParam, TScrapbook>? _postInvoke;
    private readonly ISerializer? _serializer;

    public BuilderWithInnerWithPreAndPostInvokeAndSerializer(
        RFunctions rFunctions, 
        FunctionTypeId functionTypeId, 
        InnerAction<TParam, TScrapbook> inner, 
        ResilientFunctions.RAction.PreInvoke<TParam, TScrapbook>? preInvoke, 
        ResilientFunctions.RAction.PostInvoke<TParam, TScrapbook>? postInvoke, 
        ISerializer? serializer)
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