using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Tests.Utils;
using Cleipnir.ResilientFunctions.Utils;
using Shouldly;
using static Cleipnir.ResilientFunctions.Tests.Utils.FlagPosition;

namespace Cleipnir.ResilientFunctions.Tests.TestTemplates.RFunctionTests;

public abstract class FailedTests
{
    private const string PARAM = "test";
    
    public abstract Task ExceptionThrowingFuncIsNotCompletedByWatchDog();
    protected Task ExceptionThrowingFuncIsNotCompletedByWatchDog(IFunctionStore store)
        => ExceptionThrowingFuncIsNotCompletedByWatchDog(store, throwUnhandledException: false);
    public abstract Task UnhandledExceptionThrowingFuncIsNotCompletedByWatchDog();
    protected Task UnhandledExceptionThrowingFuncIsNotCompletedByWatchDog(IFunctionStore store)
        => ExceptionThrowingFuncIsNotCompletedByWatchDog(store, throwUnhandledException: true);
    
    private async Task ExceptionThrowingFuncIsNotCompletedByWatchDog(
        IFunctionStore store, 
        bool throwUnhandledException,
        [System.Runtime.CompilerServices.CallerMemberName] string callerMemberName = ""
    )
    {
        var functionTypeId = callerMemberName.ToFunctionTypeId();
        var unhandledExceptionHandler = new UnhandledExceptionCatcher();
        {
            var nonCompletingRFunctions = RFunctions
                .Create(
                    store, 
                    unhandledExceptionHandler.Catch, 
                    crashedCheckFrequency: TimeSpan.Zero,
                    postponedCheckFrequency: TimeSpan.Zero
                )
                .Register(
                    functionTypeId,
                    (string _) =>
                        throwUnhandledException 
                            ? throw new Exception() 
                            : new Exception().ToFailedRResult<string>().ToTask(),
                    _ => _
                );

            var result = await nonCompletingRFunctions(PARAM);
            result.Failed.ShouldBeTrue();
        }
        {
            var flag = new SyncedFlag();
            using var rFunctions = RFunctions.Create(
                store, 
                unhandledExceptionHandler.Catch,
                crashedCheckFrequency: TimeSpan.FromMilliseconds(2),
                postponedCheckFrequency: TimeSpan.FromMilliseconds(2)
            );
            var rFunc = rFunctions.Register(
                functionTypeId,
                (string s) =>
                {
                    flag.Raise();
                    return s.ToUpper().ToSucceededRResult().ToTask();
                },
                _ => _
            );
            await Task.Delay(100);
            
            flag.Position.ShouldBe(Lowered);
            
            var functionId = new FunctionId(functionTypeId, PARAM.ToFunctionInstanceId());
            var status = await store.GetFunction(functionId).Map(t => t?.Status);
            status.ShouldNotBeNull();
            status.ShouldBe(Status.Failed);
            (await rFunc(PARAM)).Failed.ShouldBeTrue();
        }
            
        unhandledExceptionHandler.ThrownExceptions.Count.ShouldBe(throwUnhandledException ? 1 : 0);
    }
    
    public abstract Task ExceptionThrowingFuncWithScrapbookIsNotCompletedByWatchDog();
    protected Task ExceptionThrowingFuncWithScrapbookIsNotCompletedByWatchDog(IFunctionStore store)
        => ExceptionThrowingFuncWithScrapbookIsNotCompletedByWatchDog(store, throwUnhandledException: false);
    public abstract Task UnhandledExceptionThrowingFuncWithScrapbookIsNotCompletedByWatchDog();
    protected Task UnhandledExceptionThrowingFuncWithScrapbookIsNotCompletedByWatchDog(IFunctionStore store) 
        => ExceptionThrowingFuncWithScrapbookIsNotCompletedByWatchDog(store, throwUnhandledException: true);
    
    private async Task ExceptionThrowingFuncWithScrapbookIsNotCompletedByWatchDog(
        IFunctionStore store,
        bool throwUnhandledException, 
        [System.Runtime.CompilerServices.CallerMemberName] string callerMemberName = ""
    )
    {
        var functionTypeId = callerMemberName.ToFunctionTypeId();
        var unhandledExceptionHandler = new UnhandledExceptionCatcher();
        {
            var nonCompletingRFunctions = RFunctions
                .Create(
                    store, 
                    unhandledExceptionHandler.Catch, 
                    crashedCheckFrequency: TimeSpan.Zero,
                    postponedCheckFrequency: TimeSpan.Zero
                )
                .Register(
                    functionTypeId,
                    (string _, Scrapbook _) =>
                        throwUnhandledException 
                            ? throw new Exception()
                            : new Exception().ToFailedRResult<string>().ToTask(),
                    _ => _
                );

            var result = await nonCompletingRFunctions(PARAM);
            result.Failed.ShouldBeTrue();
        }
        {
            var flag = new SyncedFlag();
            using var rFunctions = 
                RFunctions.Create(
                    store,
                    unhandledExceptionHandler.Catch,
                    crashedCheckFrequency: TimeSpan.FromMilliseconds(2),
                    postponedCheckFrequency: TimeSpan.FromMilliseconds(2)
                );
            var rFunc = rFunctions.Register(functionTypeId,
                (string _, Scrapbook _) =>
                {
                    flag.Raise();
                    return RResult.Success.ToTask();
                },
                _ => _
            );
                
            await Task.Delay(100);
            flag.Position.ShouldBe(Lowered);
            
            var functionId = new FunctionId(functionTypeId, PARAM.ToFunctionInstanceId());
            var storedFunction = await store.GetFunction(functionId);
            storedFunction.ShouldNotBeNull();
            storedFunction.Status.ShouldBe(Status.Failed);

            storedFunction.Scrapbook.ShouldNotBeNull();
            storedFunction.Scrapbook.Deserialize().ShouldBeOfType<Scrapbook>();

            (await rFunc(PARAM)).Failed.ShouldBeTrue();
        }
            
        unhandledExceptionHandler.ThrownExceptions.Count.ShouldBe(throwUnhandledException ? 1 : 0);
    }
    
    public abstract Task ExceptionThrowingActionIsNotCompletedByWatchDog();
    protected Task ExceptionThrowingActionIsNotCompletedByWatchDog(IFunctionStore store)
        => ExceptionThrowingActionIsNotCompletedByWatchDog(store, throwUnhandledException: false);
    public abstract Task UnhandledExceptionThrowingActionIsNotCompletedByWatchDog();
    protected Task UnhandledExceptionThrowingActionIsNotCompletedByWatchDog(IFunctionStore store)
        => ExceptionThrowingActionIsNotCompletedByWatchDog(store, throwUnhandledException: true);
    private async Task ExceptionThrowingActionIsNotCompletedByWatchDog(
        IFunctionStore store,
        bool throwUnhandledException,
        [System.Runtime.CompilerServices.CallerMemberName] string callerMemberName = ""
    )
    {
        var functionTypeId = callerMemberName.ToFunctionTypeId();
        var unhandledExceptionHandler = new UnhandledExceptionCatcher();
        {
            var nonCompletingRFunctions = RFunctions
                .Create(
                    store, 
                    unhandledExceptionHandler.Catch, 
                    crashedCheckFrequency: TimeSpan.Zero,
                    postponedCheckFrequency: TimeSpan.Zero
                )
                .Register(
                    functionTypeId,
                    (string _) =>
                        throwUnhandledException 
                            ? throw new Exception()
                            : new Exception().ToFailedRResult().ToTask(),
                    _ => _
                );

            var result = await nonCompletingRFunctions(PARAM);
            result.Failed.ShouldBe(true);
        }
        {
            var flag = new SyncedFlag();
            using var rFunctions = RFunctions.Create(
                store, 
                unhandledExceptionHandler.Catch,
                crashedCheckFrequency: TimeSpan.FromMilliseconds(2),
                postponedCheckFrequency: TimeSpan.FromMilliseconds(2)
            );
            var rFunc = rFunctions.Register(
                functionTypeId,
                (string _) =>
                {
                    flag.Raise();
                    return RResult.Success.ToTask();
                },
                _ => _
            );
            await Task.Delay(100);
            flag.Position.ShouldBe(Lowered);
            
            var functionId = new FunctionId(functionTypeId, PARAM.ToFunctionInstanceId());
            var status = await store.GetFunction(functionId).Map(t => t?.Status);
            status.ShouldNotBeNull();
            status.ShouldBe(Status.Failed);
            (await rFunc(PARAM)).Failed.ShouldBeTrue();
        }
            
        unhandledExceptionHandler.ThrownExceptions.Count.ShouldBe(throwUnhandledException ? 1 : 0);
    }
    
    public abstract Task ExceptionThrowingActionWithScrapbookIsNotCompletedByWatchDog();
    protected Task ExceptionThrowingActionWithScrapbookIsNotCompletedByWatchDog(IFunctionStore store)
        => ExceptionThrowingActionWithScrapbookIsNotCompletedByWatchDog(store, throwUnhandledException: false);
    public abstract Task UnhandledExceptionThrowingActionWithScrapbookIsNotCompletedByWatchDog();
    protected Task UnhandledExceptionThrowingActionWithScrapbookIsNotCompletedByWatchDog(IFunctionStore store)
        => ExceptionThrowingActionWithScrapbookIsNotCompletedByWatchDog(store, throwUnhandledException: true);
    
    private async Task ExceptionThrowingActionWithScrapbookIsNotCompletedByWatchDog(
        IFunctionStore store,
        bool throwUnhandledException,
        [System.Runtime.CompilerServices.CallerMemberName] string callerMemberName = ""
    )
    {
        var functionTypeId = callerMemberName.ToFunctionTypeId();
        var unhandledExceptionHandler = new UnhandledExceptionCatcher();
        const string param = "test";
        {
            var nonCompletingRFunctions = RFunctions
                .Create(store, unhandledExceptionHandler.Catch, crashedCheckFrequency: TimeSpan.Zero)
                .Register(
                    functionTypeId,
                    (string _, Scrapbook _) => 
                        throwUnhandledException
                            ? throw new Exception()
                            : new Exception().ToFailedRResult().ToTask(),
                    _ => _
                );

            var result = await nonCompletingRFunctions(param);
            result.Failed.ShouldBe(true);
        }
        {
            var flag = new SyncedFlag();
            using var rFunctions = RFunctions.Create(
                store, 
                unhandledExceptionHandler.Catch,
                crashedCheckFrequency: TimeSpan.FromMilliseconds(2)
            );
            var rFunc = rFunctions.Register(
                functionTypeId,
                (string _, Scrapbook _) =>
                {
                    flag.Raise();
                    return RResult.Success.ToTask();
                }, 
                _ => _
            );
                
            await Task.Delay(100);
            flag.Position.ShouldBe(Lowered);
            
            var functionId = new FunctionId(functionTypeId, param.ToFunctionInstanceId());
            var storedFunction = await store.GetFunction(functionId);
            storedFunction.ShouldNotBeNull();
            storedFunction.Status.ShouldBe(Status.Failed);

            storedFunction.Scrapbook.ShouldNotBeNull();
            storedFunction.Scrapbook.Deserialize().ShouldBeOfType<Scrapbook>();
            (await rFunc(param)).Failed.ShouldBeTrue();
        }
            
        unhandledExceptionHandler.ThrownExceptions.Count.ShouldBe(throwUnhandledException ? 1 : 0);
    }

    private class Scrapbook : RScrapbook { }
}