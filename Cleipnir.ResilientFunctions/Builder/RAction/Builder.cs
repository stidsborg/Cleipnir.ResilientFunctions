using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Invocation;
using Cleipnir.ResilientFunctions.ParameterSerialization;

namespace Cleipnir.ResilientFunctions.Builder.RAction;

public class BuilderWithInner<TParam> where TParam : notnull
{
    private readonly RFunctions _rFunctions;
    private readonly FunctionTypeId _functionTypeId;
    private readonly InnerAction<TParam> _inner;

    public BuilderWithInner(RFunctions rFunctions, FunctionTypeId functionTypeId, InnerAction<TParam> inner)
    {
        _functionTypeId = functionTypeId;
        _inner = inner;
        _rFunctions = rFunctions;
    }

    public RAction<TParam> Register() => _rFunctions.Register(_functionTypeId, _inner);

    public BuilderWithInnerWithPreInvoke<TParam> WithPreInvoke(ResilientFunctions.RAction.PreInvoke<TParam> preInvoke)
        => new(_rFunctions, _functionTypeId, _inner, preInvoke);

    public BuilderWithInnerWithPreInvoke<TParam> WithPreInvoke(ResilientFunctions.RAction.SyncPreInvoke<TParam> preInvoke)
        => new(
            _rFunctions,
            _functionTypeId,
            _inner,
            CommonInvoker.SyncActionPreInvoke(preInvoke)
        );
    
    public BuilderWithInnerWithPreAndPostInvoke<TParam> WithPostInvoke(ResilientFunctions.RAction.PostInvoke<TParam> postInvoke)
        => new(_rFunctions, _functionTypeId, _inner, preInvoke: null, postInvoke);

    public BuilderWithInnerWithPreAndPostInvoke<TParam> WithPostInvoke(ResilientFunctions.RAction.SyncPostInvoke<TParam> postInvoke)
        => new(
            _rFunctions,
            _functionTypeId,
            _inner,
            preInvoke: null,
            CommonInvoker.AsyncActionPostInvoke(postInvoke)
        );

    public BuilderWithInnerWithPreAndPostInvokeAndSerializer<TParam> WithSerializer(ISerializer serializer)
        => new(
            _rFunctions,
            _functionTypeId,
            _inner,
            preInvoke: null,
            postInvoke: null,
            serializer
        );
}

public class BuilderWithInnerWithPreInvoke<TParam> where TParam : notnull
{
    private readonly RFunctions _rFunctions;
    private readonly FunctionTypeId _functionTypeId;
    private readonly InnerAction<TParam> _inner;
    private readonly ResilientFunctions.RAction.PreInvoke<TParam>? _preInvoke;

    public BuilderWithInnerWithPreInvoke(
        RFunctions rFunctions, 
        FunctionTypeId functionTypeId, 
        InnerAction<TParam> inner, 
        ResilientFunctions.RAction.PreInvoke<TParam>? preInvoke)
    {
        _functionTypeId = functionTypeId;
        _inner = inner;
        _preInvoke = preInvoke;
        _rFunctions = rFunctions;
    }
    
    public BuilderWithInnerWithPreAndPostInvoke<TParam> WithPostInvoke(ResilientFunctions.RAction.PostInvoke<TParam> postInvoke)
        => new(_rFunctions, _functionTypeId, _inner, _preInvoke, postInvoke);

    public BuilderWithInnerWithPreAndPostInvoke<TParam> WithPostInvoke(ResilientFunctions.RAction.SyncPostInvoke<TParam> postInvoke)
        => new(
            _rFunctions,
            _functionTypeId,
            _inner,
            _preInvoke,
            CommonInvoker.AsyncActionPostInvoke(postInvoke)
        );

    public BuilderWithInnerWithPreAndPostInvokeAndSerializer<TParam> WithSerializer(ISerializer serializer)
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

public class BuilderWithInnerWithPreAndPostInvoke<TParam> where TParam : notnull
{
    private readonly RFunctions _rFunctions;
    private readonly FunctionTypeId _functionTypeId;
    private readonly InnerAction<TParam> _inner;
    private readonly ResilientFunctions.RAction.PreInvoke<TParam>? _preInvoke;
    private readonly ResilientFunctions.RAction.PostInvoke<TParam>? _postInvoke;

    public BuilderWithInnerWithPreAndPostInvoke(
        RFunctions rFunctions, 
        FunctionTypeId functionTypeId, 
        InnerAction<TParam> inner, 
        ResilientFunctions.RAction.PreInvoke<TParam>? preInvoke, 
        ResilientFunctions.RAction.PostInvoke<TParam>? postInvoke)
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
        _postInvoke
    );
}

public class BuilderWithInnerWithPreAndPostInvokeAndSerializer<TParam> where TParam : notnull
{
    private readonly RFunctions _rFunctions;
    private readonly FunctionTypeId _functionTypeId;
    private readonly InnerAction<TParam> _inner;
    private readonly ResilientFunctions.RAction.PreInvoke<TParam>? _preInvoke;
    private readonly ResilientFunctions.RAction.PostInvoke<TParam>? _postInvoke;
    private readonly ISerializer? _serializer;

    public BuilderWithInnerWithPreAndPostInvokeAndSerializer(
        RFunctions rFunctions, 
        FunctionTypeId functionTypeId, 
        InnerAction<TParam> inner, 
        ResilientFunctions.RAction.PreInvoke<TParam>? preInvoke, 
        ResilientFunctions.RAction.PostInvoke<TParam>? postInvoke, 
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