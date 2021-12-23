using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Tests.Utils;
using Cleipnir.ResilientFunctions.Utils;
using Shouldly;

namespace Cleipnir.ResilientFunctions.Tests.TestTemplates.RFunctionTests;

public abstract class FailedTests
{
    public abstract Task ExceptionThrowingFuncIsNotCompletedByWatchDog();
    protected async Task ExceptionThrowingFuncIsNotCompletedByWatchDog(IFunctionStore store)
    {
        var functionTypeId = nameof(ExceptionThrowingFuncIsNotCompletedByWatchDog).ToFunctionTypeId();
        var unhandledExceptionHandler = new UnhandledExceptionCatcher();
        const string param = "test";
        {
            var throwingFunctionWrapper = new FailedFunctionWrapper(true);
            var nonCompletingRFunctions = RFunctions
                .Create(store, unhandledExceptionHandler.Catch, crashedCheckFrequency: TimeSpan.Zero)
                .Register(
                    functionTypeId,
                    (string s) => throwingFunctionWrapper.Func(s),
                    _ => _
                );

            var result = await nonCompletingRFunctions(param);
            result.Failed.ShouldBeTrue();
        }
        {
            var throwingFunctionWrapper = new FailedFunctionWrapper(false);
            using var rFunctions = RFunctions.Create(
                store, 
                unhandledExceptionHandler.Catch,
                crashedCheckFrequency: TimeSpan.FromMilliseconds(2),
                postponedCheckFrequency: TimeSpan.FromMilliseconds(2)
            );
            var rFunc = rFunctions.Register(
                functionTypeId,
                (string s) => throwingFunctionWrapper.Func(s),
                _ => _
            );
            await Task.Delay(100);
            var functionId = new FunctionId(functionTypeId, param.ToFunctionInstanceId());
            var status = await store.GetFunction(functionId).Map(t => t?.Status);
            status.ShouldNotBeNull();
            status.ShouldBe(Status.Failed);
            (await rFunc(param)).Failed.ShouldBeTrue();
        }
            
        unhandledExceptionHandler.ThrownExceptions.Count.ShouldBe(0);
    }
    
    public abstract Task ExceptionThrowingFuncWithScrapbookIsNotCompletedByWatchDog();
    protected async Task ExceptionThrowingFuncWithScrapbookIsNotCompletedByWatchDog(IFunctionStore store)
    {
        var functionTypeId = nameof(ExceptionThrowingFuncWithScrapbookIsNotCompletedByWatchDog).ToFunctionTypeId();
        var unhandledExceptionHandler = new UnhandledExceptionCatcher();
        const string param = "test";
        {
            var throwingFunctionWrapper = new FailedFunctionWrapper(true);
            var nonCompletingRFunctions = RFunctions
                .Create(store, unhandledExceptionHandler.Catch, crashedCheckFrequency: TimeSpan.Zero)
                .Register(
                    functionTypeId,
                    (string s, Scrapbook scrapbook) => throwingFunctionWrapper.Func(s, scrapbook),
                    _ => _
                );

            var result = await nonCompletingRFunctions(param);
            result.Failed.ShouldBeTrue();
        }
        {
            var throwingFunctionWrapper = new FailedFunctionWrapper(false);
            using var rFunctions = 
                RFunctions.Create(
                    store,
                    unhandledExceptionHandler.Catch,
                    crashedCheckFrequency: TimeSpan.FromMilliseconds(2),
                    postponedCheckFrequency: TimeSpan.FromMilliseconds(2)
                );
            var rFunc = rFunctions.Register(functionTypeId,
                (string s, Scrapbook scrapbook) => throwingFunctionWrapper.Func(s, scrapbook),
                _ => _
            );
                
            await Task.Delay(100);
            var functionId = new FunctionId(functionTypeId, param.ToFunctionInstanceId());
            var storedFunction = await store.GetFunction(functionId);
            storedFunction.ShouldNotBeNull();
            storedFunction.Status.ShouldBe(Status.Failed);

            storedFunction.Scrapbook.ShouldNotBeNull();
            storedFunction.Scrapbook.Deserialize().CastTo<Scrapbook>().Value.ShouldBe(1);
            
            (await rFunc(param)).Failed.ShouldBeTrue();
        }
            
        unhandledExceptionHandler.ThrownExceptions.Count.ShouldBe(0);
    }
    
    public abstract Task ExceptionThrowingActionIsNotCompletedByWatchDog();
    protected async Task ExceptionThrowingActionIsNotCompletedByWatchDog(IFunctionStore store)
    {
        var functionTypeId = nameof(ExceptionThrowingActionIsNotCompletedByWatchDog).ToFunctionTypeId();
        var unhandledExceptionHandler = new UnhandledExceptionCatcher();
        const string param = "test";
        {
            var throwingFunctionWrapper = new FailedFunctionWrapper(true);
            var nonCompletingRFunctions = RFunctions
                .Create(store, unhandledExceptionHandler.Catch, crashedCheckFrequency: TimeSpan.Zero)
                .Register(
                    functionTypeId,
                    (string s) => throwingFunctionWrapper.Action(s),
                    _ => _
                );

            var result = await nonCompletingRFunctions(param);
            result.Failed.ShouldBe(true);
        }
        {
            var throwingFunctionWrapper = new FailedFunctionWrapper(false);
            using var rFunctions = RFunctions.Create(
                store, 
                unhandledExceptionHandler.Catch,
                crashedCheckFrequency: TimeSpan.FromMilliseconds(2),
                postponedCheckFrequency: TimeSpan.FromMilliseconds(2)
            );
            var rFunc = rFunctions.Register(
                functionTypeId,
                (string s) => throwingFunctionWrapper.Action(s),
                _ => _
            );
            await Task.Delay(100);
            var functionId = new FunctionId(functionTypeId, param.ToFunctionInstanceId());
            var status = await store.GetFunction(functionId).Map(t => t?.Status);
            status.ShouldNotBeNull();
            status.ShouldBe(Status.Failed);
            (await rFunc(param)).Failed.ShouldBeTrue();
        }
            
        unhandledExceptionHandler.ThrownExceptions.Count.ShouldBe(0);
    }
    
    public abstract Task ExceptionThrowingActionWithScrapbookIsNotCompletedByWatchDog();
    protected async Task ExceptionThrowingActionWithScrapbookIsNotCompletedByWatchDog(IFunctionStore store)
    {
        var functionTypeId = nameof(ExceptionThrowingActionWithScrapbookIsNotCompletedByWatchDog).ToFunctionTypeId();
        var unhandledExceptionHandler = new UnhandledExceptionCatcher();
        const string param = "test";
        {
            var throwingFunctionWrapper = new FailedFunctionWrapper(true);
            var nonCompletingRFunctions = RFunctions
                .Create(store, unhandledExceptionHandler.Catch, crashedCheckFrequency: TimeSpan.Zero)
                .Register(
                    functionTypeId,
                    (string s, Scrapbook scrapbook) => throwingFunctionWrapper.Action(s, scrapbook),
                    _ => _
                );

            var result = await nonCompletingRFunctions(param);
            result.Failed.ShouldBe(true);
        }
        {
            var throwingFunctionWrapper = new FailedFunctionWrapper(true);
            using var rFunctions = RFunctions.Create(
                store, 
                unhandledExceptionHandler.Catch,
                crashedCheckFrequency: TimeSpan.FromMilliseconds(2)
            );
            var rFunc = rFunctions.Register(
                functionTypeId,
                (string s, Scrapbook scrapbook) => throwingFunctionWrapper.Action(s, scrapbook),
                _ => _
            );
                
            await Task.Delay(100);
            var functionId = new FunctionId(functionTypeId, param.ToFunctionInstanceId());
            var storedFunction = await store.GetFunction(functionId);
            storedFunction.ShouldNotBeNull();
            storedFunction.Status.ShouldBe(Status.Failed);

            storedFunction.Scrapbook.ShouldNotBeNull();
            storedFunction.Scrapbook.Deserialize().CastTo<Scrapbook>().Value.ShouldBe(1);
            (await rFunc(param)).Failed.ShouldBeTrue();
        }
            
        unhandledExceptionHandler.ThrownExceptions.Count.ShouldBe(0);
    }
    
    private class FailedFunctionWrapper
    {
        private readonly bool _shouldThrow;

        public FailedFunctionWrapper(bool shouldThrow) => _shouldThrow = shouldThrow;

        public async Task<RResult<string>> Func(string s)
        {
            await Task.CompletedTask;
            if (_shouldThrow)
                return Fail.WithException(new Exception());

            return s.ToUpper();
        }
            
        public async Task<RResult<string>> Func(string s, Scrapbook scrapbook)
        {
            scrapbook.Value = 1;
            await scrapbook.Save();
                
            if (_shouldThrow)
                return Fail.WithException(new Exception());

            scrapbook.Value = 2;
            await scrapbook.Save();
            
            return s.ToUpper();
        }

        public async Task<RResult> Action(string s)
        {
            await Task.CompletedTask;
                
            if (_shouldThrow)
                return Fail.WithException(new Exception());
            
            return RResult.Success;
        }
            
        public async Task<RResult> Action(string s, Scrapbook scrapbook)
        {
            scrapbook.Value = 1;
            await scrapbook.Save();
                
            if (_shouldThrow)
                return Fail.WithException(new Exception());
                
            scrapbook.Value = 2;
            await scrapbook.Save();
            
            return RResult.Success;
        }
    }

    private class Scrapbook : RScrapbook
    {
        public int Value { get; set; }
    }
}