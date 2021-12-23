using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Tests.Utils;
using Cleipnir.ResilientFunctions.Utils;
using Shouldly;

namespace Cleipnir.ResilientFunctions.Tests.TestTemplates.RFunctionTests;

public abstract class CrashedTests
{
    private static Task<RResult<string>> NeverCompletingTask => new(() => default!);
    
    public abstract Task NonCompletedFuncIsCompletedByWatchDog();
    protected async Task NonCompletedFuncIsCompletedByWatchDog(IFunctionStore store)
    {
        var functionTypeId = nameof(NonCompletedFuncIsCompletedByWatchDog).ToFunctionTypeId();
        const string param = "test";
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
                    (string _) => NeverCompletingTask,
                    _ => _
                );

            _ = nonCompletingRFunctions(param);
        }
        {
            using var rFunctions = RFunctions.Create(
                store,
                unhandledExceptionHandler.Catch,
                TimeSpan.FromMilliseconds(2)
            );

            var rFunc = rFunctions
                .Register(
                    functionTypeId,
                    (string s) => Funcs.ToUpper(s),
                    _ => _
                );

            var functionId = new FunctionId(functionTypeId, param.ToFunctionInstanceId());
            await BusyWait.Until(
                async () => await store
                    .GetFunction(functionId)
                    .Map(f => f?.Status ?? Status.Failed) != Status.Executing
            );

            var status = await store.GetFunction(functionId).Map(f => f?.Status);
            status.ShouldBe(Status.Succeeded);
            await rFunc(param).EnsureSuccess().ShouldBeAsync("TEST");
        }

        unhandledExceptionHandler.ThrownExceptions.Count.ShouldBe(0);
    }

    public abstract Task NonCompletedFuncWithScrapbookIsCompletedByWatchDog();
    protected async Task NonCompletedFuncWithScrapbookIsCompletedByWatchDog(IFunctionStore store)
    {
        var functionTypeId = nameof(NonCompletedFuncWithScrapbookIsCompletedByWatchDog).ToFunctionTypeId();
        const string param = "test";
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
                    (string _, Scrapbook _) => NeverCompletingTask,
                    _ => _
                );

            _ = nonCompletingRFunctions(param);
        }
        {
            using var rFunctions = RFunctions.Create(
                store,
                unhandledExceptionHandler.Catch,
                TimeSpan.FromMilliseconds(2)
            );

            var rFunc = rFunctions
                .Register(
                    functionTypeId,
                    async (string s, Scrapbook scrapbook) =>
                    {
                        scrapbook.Value = 1;
                        await scrapbook.Save();
                        return Succeed.WithResult(s.ToUpper());
                    },
                    _ => _
                );

            var functionId = new FunctionId(functionTypeId, param.ToFunctionInstanceId());
            await BusyWait.Until(
                async () => await store
                    .GetFunction(functionId)
                    .Map(f => f?.Status ?? Status.Failed) != Status.Executing
            );

            var storedFunction = await store.GetFunction(functionId);
            storedFunction.ShouldNotBeNull();
            storedFunction.Status.ShouldBe(Status.Succeeded);
            storedFunction.Scrapbook.ShouldNotBeNull();
            storedFunction.Scrapbook.Deserialize().CastTo<Scrapbook>().Value.ShouldBe(1);
            await rFunc(param).EnsureSuccess().ShouldBeAsync("TEST");
        }

        unhandledExceptionHandler.ThrownExceptions.Count.ShouldBe(0);
    }
    
    public abstract Task NonCompletedActionIsCompletedByWatchDog();
    protected async Task NonCompletedActionIsCompletedByWatchDog(IFunctionStore store)
    {
        var functionTypeId = nameof(NonCompletedActionIsCompletedByWatchDog).ToFunctionTypeId();
        const string param = "test";
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
                    (string _) => NeverCompletingTask,
                    _ => _
                );

            _ = nonCompletingRFunctions(param);
        }
        {
            using var rFunctions = RFunctions.Create(
                store,
                unhandledExceptionHandler.Catch,
                TimeSpan.FromMilliseconds(2)
            );

            var rAction = rFunctions
                .Register(
                    functionTypeId,
                    (string _) => Task.FromResult(Succeed.WithoutResult()),
                    _ => _
                );

            var functionId = new FunctionId(functionTypeId, param.ToFunctionInstanceId());
            await BusyWait.Until(
                async () => await store
                    .GetFunction(functionId)
                    .Map(f => f?.Status ?? Status.Failed) != Status.Executing
            );

            var status = await store.GetFunction(functionId).Map(f => f?.Status);
            status.ShouldBe(Status.Succeeded);
            var rResult = await rAction(param);
            rResult.EnsureSuccess();
        }

        unhandledExceptionHandler.ThrownExceptions.Count.ShouldBe(0);
    }

    public abstract Task NonCompletedActionWithScrapbookIsCompletedByWatchDog();
    protected async Task NonCompletedActionWithScrapbookIsCompletedByWatchDog(IFunctionStore store)
    {
        var functionTypeId = nameof(NonCompletedFuncIsCompletedByWatchDog).ToFunctionTypeId();
        const string param = "test";
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
                    (string _, Scrapbook _) => NeverCompletingTask,
                    _ => _
                );

            _ = nonCompletingRFunctions(param);
        }
        {
            using var rFunctions = RFunctions.Create(
                store,
                unhandledExceptionHandler.Catch,
                TimeSpan.FromMilliseconds(2)
            );

            var rAction = rFunctions
                .Register(
                    functionTypeId,
                    async (string _, Scrapbook scrapbook) =>
                    {
                        scrapbook.Value = 1;
                        await scrapbook.Save();
                        return RResult.Success;
                    },
                    _ => _
                );

            var functionId = new FunctionId(functionTypeId, param.ToFunctionInstanceId());
            await BusyWait.Until(
                async () => await store
                    .GetFunction(functionId)
                    .Map(f => f?.Status ?? Status.Failed) != Status.Executing
            );

            var storedFunction = await store.GetFunction(functionId);
            storedFunction.ShouldNotBeNull();
            storedFunction.Status.ShouldBe(Status.Succeeded);
            storedFunction.Scrapbook.ShouldNotBeNull();
            storedFunction.Scrapbook.Deserialize().CastTo<Scrapbook>().Value.ShouldBe(1);
            var rResult = await rAction(param);
            rResult.EnsureSuccess();
        }

        unhandledExceptionHandler.ThrownExceptions.Count.ShouldBe(0);
    }

    private class Scrapbook : RScrapbook
    {
        public int Value { get; set; }
    }
}