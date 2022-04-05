using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Invocation;
using Cleipnir.ResilientFunctions.ParameterSerialization;

namespace Cleipnir.ResilientFunctions.Builder.RJob;

public class Builder<TScrapbook> where TScrapbook : RScrapbook, new()
{
    private readonly RFunctions _rFunctions;
    private readonly string _jobId;

    public Builder(RFunctions rFunctions, string jobId)
    {
        _rFunctions = rFunctions;
        _jobId = jobId;
    }

    public BuilderWithInner<TScrapbook> WithInner(Action<TScrapbook> inner)
    {
        Task<Return> AsyncInner(TScrapbook scrapbook)
        {
            inner(scrapbook);
            return Return.Succeed.ToTask();
        }

        return WithInner(AsyncInner);
    }

    public BuilderWithInner<TScrapbook> WithInner(Func<TScrapbook, Return> inner)
    {
        Task<Return> AsyncInner(TScrapbook scrapbook)
        {
            var returned = inner(scrapbook);
            return returned.ToTask();
        }

        return WithInner(AsyncInner);
    }

    public BuilderWithInner<TScrapbook> WithInner(Func<TScrapbook, Task> inner)
    {
        async Task<Return> AsyncInner(TScrapbook scrapbook)
        {
            await inner(scrapbook);
            return Return.Succeed;
        }

        return WithInner(AsyncInner);
    }
    
    public BuilderWithInner<TScrapbook> WithInner(Func<TScrapbook, Task<Return>> inner) 
        => new(_rFunctions, _jobId, inner);
}

public class BuilderWithInner<TScrapbook> where TScrapbook : RScrapbook, new()
{
    private readonly RFunctions _rFunctions;
    private readonly string _jobId;
    private readonly Func<TScrapbook, Task<Return>> _inner;

    public BuilderWithInner(RFunctions rFunctions, string jobId, Func<TScrapbook, Task<Return>> inner)
    {
        _inner = inner;
        _rFunctions = rFunctions;
        _jobId = jobId;
    }

    public ResilientFunctions.RJob Create() => _rFunctions.RegisterJob(_jobId, _inner);

    public BuilderWithInnerWithPreInvoke<TScrapbook> WithPreInvoke(Func<TScrapbook, Task> preInvoke)
        => new(_rFunctions, _jobId, _inner, preInvoke);

    public BuilderWithInnerWithPreInvoke<TScrapbook> WithPreInvoke(Action<TScrapbook> preInvoke)
        => new(
            _rFunctions,
            _jobId,
            _inner,
            CommonInvoker.AsyncJobPreInvoke(preInvoke)
        );
    
    public BuilderWithInnerWithPreAndPostInvoke<TScrapbook> WithPostInvoke(Func<Return, TScrapbook, Task<Return>> postInvoke)
        => new(_rFunctions, _jobId, _inner, preInvoke: null, postInvoke);

    public BuilderWithInnerWithPreAndPostInvoke<TScrapbook> WithPostInvoke(Func<Return, TScrapbook, Return> postInvoke)
        => new(
            _rFunctions,
            _jobId,
            _inner,
            preInvoke: null,
            CommonInvoker.AsyncJobPostInvoke(postInvoke)
        );

    public BuilderWithInnerWithPreAndPostInvokeAndSerializer<TScrapbook> WithSerializer(ISerializer serializer)
        => new(
            _rFunctions,
            _jobId,
            _inner,
            preInvoke: null,
            postInvoke: null,
            serializer
        );
}

public class BuilderWithInnerWithPreInvoke<TScrapbook> where TScrapbook : RScrapbook, new()
{
    private readonly RFunctions _rFunctions;
    private readonly string _jobId;
    private readonly Func<TScrapbook, Task<Return>> _inner;
    private readonly Func<TScrapbook, Task>? _preInvoke;

    public BuilderWithInnerWithPreInvoke(RFunctions rFunctions, string jobId, Func<TScrapbook, Task<Return>> inner, Func<TScrapbook, Task>? preInvoke)
    {
        _rFunctions = rFunctions;
        _jobId = jobId;
        _inner = inner;
        _preInvoke = preInvoke;
    }

    public BuilderWithInnerWithPreAndPostInvoke<TScrapbook> WithPostInvoke(Func<Return, TScrapbook, Task<Return>> postInvoke)
        => new(_rFunctions, _jobId, _inner, _preInvoke, postInvoke);

    public BuilderWithInnerWithPreAndPostInvoke<TScrapbook> WithPostInvoke(Func<Return, TScrapbook, Return> postInvoke)
        => new(
            _rFunctions,
            _jobId,
            _inner,
            _preInvoke,
            CommonInvoker.AsyncJobPostInvoke(postInvoke)
        );

    public BuilderWithInnerWithPreAndPostInvokeAndSerializer<TScrapbook> WithSerializer(ISerializer serializer)
        => new(
            _rFunctions,
            _jobId,
            _inner,
            _preInvoke,
            postInvoke: null,
            serializer
        );

    public ResilientFunctions.RJob Register()
        => _rFunctions.RegisterJob(
            _jobId,
            _inner,
            _preInvoke
        );
}

public class BuilderWithInnerWithPreAndPostInvoke<TScrapbook> where TScrapbook : RScrapbook, new()
{
    private readonly RFunctions _rFunctions;
    private readonly string _jobId;
    private readonly Func<TScrapbook, Task<Return>> _inner;
    private readonly Func<TScrapbook, Task>? _preInvoke;
    private readonly Func<Return, TScrapbook, Task<Return>>? _postInvoke;

    public BuilderWithInnerWithPreAndPostInvoke(
        RFunctions rFunctions, 
        string jobId, 
        Func<TScrapbook, Task<Return>> inner, 
        Func<TScrapbook, Task>? preInvoke, 
        Func<Return, TScrapbook, Task<Return>>? postInvoke)
    {
        _rFunctions = rFunctions;
        _jobId = jobId;
        _inner = inner;
        _preInvoke = preInvoke;
        _postInvoke = postInvoke;
    }

    public BuilderWithInnerWithPreAndPostInvokeAndSerializer<TScrapbook> WithSerializer(ISerializer serializer)
        => new(_rFunctions, _jobId, _inner, _preInvoke, _postInvoke, serializer);

    public ResilientFunctions.RJob Register()
        => _rFunctions.RegisterJob(
            _jobId,
            _inner,
            _preInvoke,
            _postInvoke
        );
}

public class BuilderWithInnerWithPreAndPostInvokeAndSerializer<TScrapbook> where TScrapbook : RScrapbook, new()
{
    private readonly RFunctions _rFunctions;
    private readonly string _jobId;
    private readonly Func<TScrapbook, Task<Return>> _inner;
    private readonly Func<TScrapbook, Task>? _preInvoke;
    private readonly Func<Return, TScrapbook, Task<Return>>? _postInvoke;
    private readonly ISerializer? _serializer;

    public BuilderWithInnerWithPreAndPostInvokeAndSerializer(
        RFunctions rFunctions, 
        string jobId, 
        Func<TScrapbook, Task<Return>> inner, 
        Func<TScrapbook, Task>? preInvoke, 
        Func<Return, TScrapbook, Task<Return>>? postInvoke, 
        ISerializer? serializer)
    {
        _rFunctions = rFunctions;
        _jobId = jobId;
        _inner = inner;
        _preInvoke = preInvoke;
        _postInvoke = postInvoke;
        _serializer = serializer;
    }

    public ResilientFunctions.RJob Register()
        => _rFunctions.RegisterJob(
            _jobId,
            _inner,
            _preInvoke,
            _postInvoke,
            _serializer
        );
}