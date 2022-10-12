using System;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.ParameterSerialization;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Tests.Utils;
using Shouldly;
using static Cleipnir.ResilientFunctions.Tests.Utils.FlagPosition;

namespace Cleipnir.ResilientFunctions.Tests.TestTemplates.RFunctionTests;

public abstract class FailedTests
{
    private const string PARAM = "test";
    
    public abstract Task ExceptionThrowingFuncIsNotCompletedByWatchDog();
    protected Task ExceptionThrowingFuncIsNotCompletedByWatchDog(Task<IFunctionStore> storeTask)
        => ExceptionThrowingFuncIsNotCompletedByWatchDog(storeTask, throwUnhandledException: false);
    public abstract Task UnhandledExceptionThrowingFuncIsNotCompletedByWatchDog();
    protected Task UnhandledExceptionThrowingFuncIsNotCompletedByWatchDog(Task<IFunctionStore> storeTask)
        => ExceptionThrowingFuncIsNotCompletedByWatchDog(storeTask, throwUnhandledException: true);
    
    private async Task ExceptionThrowingFuncIsNotCompletedByWatchDog(
        Task<IFunctionStore> storeTask, 
        bool throwUnhandledException,
        [CallerMemberName] string callerMemberName = ""
    )
    {
        var store = await storeTask;
        var functionTypeId = callerMemberName.ToFunctionTypeId();
        var functionId = new FunctionId(functionTypeId, PARAM);
        var unhandledExceptionHandler = new UnhandledExceptionCatcher();
        {
            var rFunc = new RFunctions
                (
                    store,
                    new Settings(
                        unhandledExceptionHandler.Catch,
                        CrashedCheckFrequency: TimeSpan.Zero,
                        PostponedCheckFrequency: TimeSpan.Zero
                    )
                )
                .RegisterAction(
                    functionTypeId,
                    (string _) =>
                        throwUnhandledException
                            ? throw new Exception()
                            : Task.FromException(new Exception())
                )
                .Invoke;

            await Should.ThrowAsync<Exception>(async () => await rFunc(PARAM, PARAM));
            (await store.GetFunction(functionId)).ShouldNotBeNull();
        }
        {
            var flag = new SyncedFlag();
            using var rFunctions = new RFunctions(
                store, 
                new Settings(
                    unhandledExceptionHandler.Catch,
                    CrashedCheckFrequency: TimeSpan.FromMilliseconds(2),
                    PostponedCheckFrequency: TimeSpan.FromMilliseconds(2)
                )
            );
            var rFunc = rFunctions.RegisterFunc(
                functionTypeId,
                (string s) =>
                {
                    flag.Raise();
                    return s.ToUpper().ToTask();
                }
            ).Invoke;
            await Task.Delay(100);
            
            flag.Position.ShouldBe(Lowered);
            
            var sf = await store.GetFunction(functionId);
            sf.ShouldNotBeNull();
            sf.Status.ShouldBe(Status.Failed);
            await Should.ThrowAsync<Exception>(async () => await rFunc(PARAM, PARAM));
        }
            
        unhandledExceptionHandler.ThrownExceptions.Count.ShouldBe(0);
    }
    
    public abstract Task ExceptionThrowingFuncWithScrapbookIsNotCompletedByWatchDog();
    protected Task ExceptionThrowingFuncWithScrapbookIsNotCompletedByWatchDog(Task<IFunctionStore> storeTask)
        => ExceptionThrowingFuncWithScrapbookIsNotCompletedByWatchDog(storeTask, throwUnhandledException: false);
    public abstract Task UnhandledExceptionThrowingFuncWithScrapbookIsNotCompletedByWatchDog();
    protected Task UnhandledExceptionThrowingFuncWithScrapbookIsNotCompletedByWatchDog(Task<IFunctionStore> storeTask) 
        => ExceptionThrowingFuncWithScrapbookIsNotCompletedByWatchDog(storeTask, throwUnhandledException: true);
    
    private async Task ExceptionThrowingFuncWithScrapbookIsNotCompletedByWatchDog(
        Task<IFunctionStore> storeTask,
        bool throwUnhandledException, 
        [CallerMemberName] string callerMemberName = ""
    )
    {
        var store = await storeTask;
        var functionTypeId = callerMemberName.ToFunctionTypeId();
        var unhandledExceptionHandler = new UnhandledExceptionCatcher();
        {
            var nonCompletingRFunctions = new RFunctions
                (
                    store, 
                    new Settings(
                        unhandledExceptionHandler.Catch,
                        CrashedCheckFrequency: TimeSpan.FromMilliseconds(0),
                        PostponedCheckFrequency: TimeSpan.FromMilliseconds(0)
                    )
                )
                .RegisterAction(
                    functionTypeId,
                    (string _, Scrapbook _) =>
                        throwUnhandledException 
                            ? throw new Exception()
                            : Task.FromException(new Exception())
                )
                .Invoke;

            await Should.ThrowAsync<Exception>(() => nonCompletingRFunctions(PARAM, PARAM));
        }
        {
            var flag = new SyncedFlag();
            using var rFunctions = new RFunctions(
                    store,
                    new Settings(
                        unhandledExceptionHandler.Catch,
                        CrashedCheckFrequency: TimeSpan.FromMilliseconds(2),
                        PostponedCheckFrequency: TimeSpan.FromMilliseconds(2)
                    )
                );
            var rAction = rFunctions.RegisterAction(functionTypeId,
                (string _, Scrapbook _) =>
                {
                    flag.Raise();
                    return Task.CompletedTask;
                }
            ).Invoke;
                
            await Task.Delay(100);
            flag.Position.ShouldBe(Lowered);
            
            var functionId = new FunctionId(functionTypeId, PARAM.ToFunctionInstanceId());
            var storedFunction = await store.GetFunction(functionId);
            storedFunction.ShouldNotBeNull();
            storedFunction.Status.ShouldBe(Status.Failed);

            storedFunction.Scrapbook.ShouldNotBeNull();
            storedFunction.Scrapbook.DefaultDeserialize().ShouldBeOfType<Scrapbook>();

            await Should.ThrowAsync<Exception>(() => rAction(PARAM, PARAM));
        }
            
        unhandledExceptionHandler.ThrownExceptions.Count.ShouldBe(0);
    }
    
    public abstract Task ExceptionThrowingActionIsNotCompletedByWatchDog();
    public async Task ExceptionThrowingActionIsNotCompletedByWatchDog(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var functionTypeId = nameof(ExceptionThrowingActionIsNotCompletedByWatchDog).ToFunctionTypeId();
        var unhandledExceptionHandler = new UnhandledExceptionCatcher();
        {
            var nonCompletingRFunctions = new RFunctions
                (
                    store, 
                    new Settings(
                        unhandledExceptionHandler.Catch,
                        CrashedCheckFrequency: TimeSpan.FromMilliseconds(0),
                        PostponedCheckFrequency: TimeSpan.FromMilliseconds(0)
                    )
                )
                .RegisterAction(
                    functionTypeId,
                    (string _) => Task.FromException(new Exception())
                )
                .Invoke;

            await Should.ThrowAsync<Exception>(nonCompletingRFunctions(PARAM, PARAM));
        }
        {
            var flag = new SyncedFlag();
            using var rFunctions = new RFunctions(
                store, 
                new Settings(
                    unhandledExceptionHandler.Catch,
                    CrashedCheckFrequency: TimeSpan.FromMilliseconds(2),
                    PostponedCheckFrequency: TimeSpan.FromMilliseconds(2)
                )
            );
            var rFunc = rFunctions
                .RegisterAction(
                    functionTypeId,
                    inner: (string _) => flag.Raise()
                )
                .Invoke;
            await Task.Delay(100);
            flag.Position.ShouldBe(Lowered);
            
            var functionId = new FunctionId(functionTypeId, PARAM.ToFunctionInstanceId());
            var status = await store.GetFunction(functionId).Map(t => t?.Status);
            status.ShouldNotBeNull();
            status.ShouldBe(Status.Failed);
            await Should.ThrowAsync<Exception>(rFunc(PARAM, PARAM));
        }
            
        unhandledExceptionHandler.ThrownExceptions.Count.ShouldBe(0);
    }
    
    public abstract Task PassingInNullParameterResultsInArgumentNullException();
    public async Task PassingInNullParameterResultsInException(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var functionTypeId = nameof(ExceptionThrowingActionIsNotCompletedByWatchDog).ToFunctionTypeId();
        var unhandledExceptionHandler = new UnhandledExceptionCatcher();
        var flag = new SyncedFlag();
        using var rFunctions = new RFunctions(
            store,
            new Settings(
                unhandledExceptionHandler.Catch,
                CrashedCheckFrequency: TimeSpan.FromMilliseconds(2),
                PostponedCheckFrequency: TimeSpan.FromMilliseconds(2)
            )
        );
        var rFunc = rFunctions
            .RegisterAction(
                functionTypeId,
                inner: (string _) => flag.Raise()
            )
            .Invoke;
        await Task.Delay(100);
        flag.Position.ShouldBe(Lowered);

        await Should.ThrowAsync<ArgumentNullException>(
            () => rFunc("someFunctionInstanceId", null!)
        );
        unhandledExceptionHandler.ThrownExceptions.Count.ShouldBe(0);
    }

    public abstract Task ExceptionThrowingActionWithScrapbookIsNotCompletedByWatchDog();
    protected Task ExceptionThrowingActionWithScrapbookIsNotCompletedByWatchDog(Task<IFunctionStore> storeTask)
        => ExceptionThrowingActionWithScrapbookIsNotCompletedByWatchDog(storeTask, throwUnhandledException: false);
    public abstract Task UnhandledExceptionThrowingActionWithScrapbookIsNotCompletedByWatchDog();
    protected Task UnhandledExceptionThrowingActionWithScrapbookIsNotCompletedByWatchDog(Task<IFunctionStore> storeTask)
        => ExceptionThrowingActionWithScrapbookIsNotCompletedByWatchDog(storeTask, throwUnhandledException: true);
    
    private async Task ExceptionThrowingActionWithScrapbookIsNotCompletedByWatchDog(
        Task<IFunctionStore> storeTask,
        bool throwUnhandledException,
        [CallerMemberName] string callerMemberName = ""
    )
    {
        var store = await storeTask;
        var functionTypeId = callerMemberName.ToFunctionTypeId();
        var unhandledExceptionHandler = new UnhandledExceptionCatcher();
        const string param = "test";
        {
            var nonCompletingRFunctions = new RFunctions
                (store, new Settings(unhandledExceptionHandler.Catch, CrashedCheckFrequency: TimeSpan.Zero))
                .RegisterAction(
                    functionTypeId,
                    (string _, Scrapbook _) => 
                        throwUnhandledException
                            ? throw new Exception()
                            : Task.FromException(new Exception())
                ).Invoke;

            await Should.ThrowAsync<Exception>(() => nonCompletingRFunctions(param, param));
        }
        {
            var flag = new SyncedFlag();
            using var rFunctions = new RFunctions(
                store,
                new Settings(unhandledExceptionHandler.Catch, CrashedCheckFrequency: TimeSpan.FromMilliseconds(2))
            );
            var rFunc = rFunctions.RegisterAction(
                functionTypeId,
                (string _, Scrapbook _) =>
                {
                    flag.Raise();
                    return Succeed.WithoutValue.ToTask();
                }
            ).Invoke;
                
            await Task.Delay(100);
            flag.Position.ShouldBe(Lowered);
            
            var functionId = new FunctionId(functionTypeId, param.ToFunctionInstanceId());
            var storedFunction = await store.GetFunction(functionId);
            storedFunction.ShouldNotBeNull();
            storedFunction.Status.ShouldBe(Status.Failed);

            storedFunction.Scrapbook.ShouldNotBeNull();
            storedFunction.Scrapbook.DefaultDeserialize().ShouldBeOfType<Scrapbook>();

            await Should.ThrowAsync<Exception>(() => rFunc(param, param));
        }
        
        unhandledExceptionHandler.ThrownExceptions.Count.ShouldBe(0);
        if (throwUnhandledException)
        {
            var errorJson = await store
                .GetFunction(new FunctionId(functionTypeId, param))
                .Map(f => f?.ErrorJson)
                .ShouldNotBeNullAsync();
            var exception = DefaultSerializer.Instance.DeserializeError(errorJson);
            exception.ShouldNotBeNull();
        }
    }

    private class Scrapbook : RScrapbook { }
}