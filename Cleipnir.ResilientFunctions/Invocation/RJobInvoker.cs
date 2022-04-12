using System;
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
    
    private readonly Func<TScrapbook, Task<Result>> _inner;

    private readonly FunctionId _functionId;
    private readonly string _jobId;

    internal RJobInvoker(
        string jobId,
        Func<TScrapbook, Task<Result>> inner,
        CommonInvoker commonInvoker,
        UnhandledExceptionHandler unhandledExceptionHandler)
    {
        _functionId = new FunctionId("Job", jobId);
        _jobId = jobId;
        
        _inner = inner;
        
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
                Result result;
                try
                {
                    // *** USER FUNCTION INVOCATION *** 
                    result = await _inner(scrapbook);
                }
                catch (Exception exception)
                {
                    await PersistFailure(_functionId, exception, scrapbook, epoch);
                    throw;
                }

                await _commonInvoker.PersistResult(_functionId, result, scrapbook, epoch);
                if (result.Postpone?.InProcessWait != true) return;
                await Task.Delay(CommonInvoker.CalculateDelay(result.Postpone));
            }
        }
        catch (Exception exception)
        {
            _unhandledExceptionHandler.Invoke(new RJobException(_jobId, "Job invocation failed", exception));
        }
    }
    
    private async Task<Tuple<TScrapbook, int>> PrepareForReInvocation(
        FunctionId functionId,
        IEnumerable<Status> expectedStatuses,
        int? expectedEpoch)
    {
        var (_, scrapbook, epoch) = await _commonInvoker.PrepareForReInvocation<string, TScrapbook>(
            functionId,
            expectedStatuses,
            expectedEpoch
        );
        return Tuple.Create(scrapbook, epoch);
    }

    private async Task PersistFailure(FunctionId functionId, Exception exception, TScrapbook scrapbook, int expectedEpoch)
        => await _commonInvoker.PersistFailure(functionId, exception, scrapbook, expectedEpoch);

    private IDisposable CreateSignOfLifeAndRegisterRunningFunction(FunctionId functionId, int expectedEpoch)
        => _commonInvoker.CreateSignOfLifeAndRegisterRunningFunction(functionId, expectedEpoch);
}
