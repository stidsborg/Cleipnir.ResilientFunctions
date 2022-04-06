﻿using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Domain.Exceptions;
using Cleipnir.ResilientFunctions.ExceptionHandling;
using Cleipnir.ResilientFunctions.Helpers;

namespace Cleipnir.ResilientFunctions.Invocation;

public class RJobInvoker<TScrapbook> where TScrapbook : RScrapbook, new()
{
    private readonly CommonInvoker _commonInvoker;
    private readonly UnhandledExceptionHandler _unhandledExceptionHandler;
    
    private readonly Func<TScrapbook, Task<Return>> _inner;
    private readonly Func<TScrapbook, Task> _preInvoke;
    private readonly Func<Return, TScrapbook, Task<Return>> _postInvoke;
    
    private readonly FunctionId _functionId;
    private readonly string _jobId;

    internal RJobInvoker(
        string jobId,
        Func<TScrapbook, Task<Return>> inner,
        Func<TScrapbook, Task>? preInvoke,
        Func<Return, TScrapbook, Task<Return>>? postInvoke,
        CommonInvoker commonInvoker,
        UnhandledExceptionHandler unhandledExceptionHandler)
    {
        _functionId = new FunctionId("Job", jobId);
        _jobId = jobId;
        
        _inner = inner;
        _preInvoke = preInvoke ?? NoOpPreInvoke;
        _postInvoke = postInvoke ?? NoOpPostInvoke;
        
        _commonInvoker = commonInvoker;
        _unhandledExceptionHandler = unhandledExceptionHandler;
    }

    public async Task Start()
    {
        var created = await _commonInvoker.PersistFunctionInStore(
            _functionId,
            "",
            typeof(TScrapbook)
        );
        
        if (created) 
            _ = Task.Run(() => PerformInvocation(scrapbook: null, epoch: 0));
    }

    public async Task ForceContinuation(IEnumerable<Status> expectedStatuses, int? expectedEpoch = null)
    {
        var (scrapbook, epoch) = await PrepareForReInvocation(_functionId, expectedStatuses, expectedEpoch);
        _ = Task.Run(() => PerformInvocation(scrapbook, epoch));
    }

    private async Task PerformInvocation(TScrapbook? scrapbook, int epoch)
    {
        using var _ = CreateSignOfLifeAndRegisterRunningFunction(_functionId, epoch);
        try
        {
            scrapbook ??= new TScrapbook();

            while (true)
            {
                Return postInvoked;
                try
                {
                    await _preInvoke(scrapbook);
                    // *** USER FUNCTION INVOCATION *** 
                    var returned = await _inner(scrapbook);
                    postInvoked = await _postInvoke(returned, scrapbook);
                }
                catch (Exception exception)
                {
                    postInvoked = await _postInvoke(new Fail(exception), scrapbook);
                    if (postInvoked.Fail == exception)
                    {
                        await PersistPostInvoked(_functionId, postInvoked, scrapbook, epoch);
                        throw;
                    }
                }

                await PersistPostInvoked(_functionId, postInvoked, scrapbook, epoch);
                if (postInvoked.Postpone?.InProcessWait != true) return;
                await Task.Delay(CommonInvoker.CalculateDelay(postInvoked.Postpone));
            }
        }
        catch (Exception exception)
        {
            _unhandledExceptionHandler.Invoke(new RJobException(_jobId, "Job invocation failed", exception));
        }
    }

    private static Task NoOpPreInvoke(TScrapbook scrapbook) => Task.CompletedTask;
    private static Task<Return> NoOpPostInvoke(Return returned, TScrapbook scrapbook) => returned.ToTask();

    private async Task<Tuple<TScrapbook, int>> PrepareForReInvocation(
        FunctionId functionId,
        IEnumerable<Status> expectedStatuses,
        int? expectedEpoch)
    {
        var (_, scrapbook, epoch) = await _commonInvoker.PrepareForReInvocation<string, TScrapbook>(functionId, expectedStatuses, expectedEpoch);
        return Tuple.Create(scrapbook, epoch);
    }

    private async Task PersistPostInvoked(FunctionId functionId, Return returned, TScrapbook scrapbook, int expectedEpoch)
        => await _commonInvoker.PersistPostInvoked(functionId, returned, scrapbook, expectedEpoch);

    private IDisposable CreateSignOfLifeAndRegisterRunningFunction(FunctionId functionId, int expectedEpoch)
        => _commonInvoker.CreateSignOfLifeAndRegisterRunningFunction(functionId, expectedEpoch);
}