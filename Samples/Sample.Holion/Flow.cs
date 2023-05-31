using System.Linq.Expressions;
using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Domain.Exceptions;
using Cleipnir.ResilientFunctions.Messaging;

namespace Sample.Holion;

public abstract class Flow<TParam, TScrapbook> where TScrapbook : RScrapbook
{
    public Context Context { get; init; } = null!;
    public Utilities Utilities { get; init; } = null!;
    public EventSource EventSource { get; init; } = null!;
    public TScrapbook Scrapbook { get; init; } = default!;

    public abstract Task Invoke(TParam param);
    
    public void Suspend(int whileEventCount) => throw new SuspendInvocationException(whileEventCount);
    public void Postpone(TimeSpan delay) => throw new PostponeInvocationException(delay);
    public void Postpone(DateTime until) => throw new PostponeInvocationException(until);
    
    public Task<string> DoAtMostOnce(string workId, Func<Task<string>> work) => Scrapbook.DoAtMostOnce(workId, work);
    public Task DoAtMostOnce(string workId, Func<Task> work) => Scrapbook.DoAtMostOnce(workId, work);
    public Task DoAtMostOnce(Expression<Func<TScrapbook, WorkStatus>> workStatus, Func<Task> work) 
        => Scrapbook.DoAtMostOnce(workStatus: workStatus, work);
    public Task DoAtMostOnce<TResult>(Expression<Func<TScrapbook, WorkStatusAndResult<TResult>>> workStatus, Func<Task<TResult>> work) =>
        Scrapbook.DoAtMostOnce(workStatus, work);

    public Task DoAtLeastOnce(string workId, Func<Task> work) => Scrapbook.DoAtLeastOnce(workId, work);
    public Task DoAtLeastOnce(Expression<Func<TScrapbook, WorkStatus>> workStatus,  Func<Task> work) => Scrapbook.DoAtLeastOnce(workStatus, work);
    public Task<string> DoAtLeastOnce(string workId, Func<Task<string>> work) => Scrapbook.DoAtLeastOnce(workId, work);
    public Task<TResult> DoAtLeastOnce<TResult>(Expression<Func<TScrapbook, WorkStatusAndResult<TResult>>> workStatus, Func<Task<TResult>> work)
        => Scrapbook.DoAtLeastOnce(workStatus, work);
}