using System;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.ExceptionHandling;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.ParameterSerialization;
using Cleipnir.ResilientFunctions.ShutdownCoordination;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Tests.InMemoryTests.SignOfLifeUpdaterTests;
using Cleipnir.ResilientFunctions.Tests.TestTemplates;
using Cleipnir.ResilientFunctions.Tests.Utils;
using Cleipnir.ResilientFunctions.Watchdogs;
using Cleipnir.ResilientFunctions.Watchdogs.Invocation;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Shouldly;

namespace Cleipnir.ResilientFunctions.Tests.MockTests
{
    [TestClass]
    public class CrashedWatchdogExceptionThrowingTests
    {
        private StoredParameter Parameter1 { get; } = new("hello".ToJson(), typeof(string).SimpleQualifiedName());
        
        [TestMethod]
        public void AnFrameworkExceptionIsThrownWhenFunctionStoreThrowsException()
        {
            var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
            
            var storeMock = new FunctionStoreMock
            {
                SetupGetFunctionsWithStatus = (_, _, _) => throw new Exception()
            };

            using var crashedWatchdog = new CrashedWatchdog<string>(
                "functionTypeId".ToFunctionTypeId(),
                (param, _) => Funcs.ToUpper(param.ToString()!),
                storeMock,
                new RFuncInvoker(
                    storeMock, 
                    DefaultSerializer.Instance,
                    new NeverExecutingSignOfLifeUpdaterFactory(),
                    new UnhandledExceptionHandler(unhandledExceptionCatcher.Catch),
                    new ShutdownCoordinator()
                ),
                TimeSpan.FromMilliseconds(1),
                new UnhandledExceptionHandler(unhandledExceptionCatcher.Catch),
                new ShutdownCoordinator()
            );

            _ = crashedWatchdog.Start();

            BusyWait.Until(() => unhandledExceptionCatcher.ThrownExceptions.Any());
            
            unhandledExceptionCatcher.ThrownExceptions.Count.ShouldBe(1);
            (unhandledExceptionCatcher.ThrownExceptions[0] is FrameworkException).ShouldBeTrue();
        }
        
        [TestMethod]
        public void AnUnhandledFrameworkExceptionIsEmittedWhenSetFunctionStateThrowsException()
        {
            var unhandledExceptionCatcher = new UnhandledExceptionCatcher();

            var storeMock = new FunctionStoreMock
            {
                SetupGetFunctionsWithStatus = 
                    (_, _, _) => new[]{ new StoredFunctionStatus(
                        "id".ToFunctionInstanceId(),
                        Epoch: 0, 
                        SignOfLife: 0, 
                        Status.Executing, 
                        PostponedUntil: null
                    )}.AsEnumerable().ToTask(),
                SetupGetFunction = id => 
                    new StoredFunction(
                        id, 
                        Parameter1,
                        Scrapbook: null,
                        Status.Executing,
                        Result: null,
                        Failure: null,
                        PostponedUntil: null,
                        Epoch: 0,
                        SignOfLife: 0
                    ).ToNullable().ToTask(),
                SetupSetFunctionState = (_, _, _, _, _, _, _) => throw new Exception(),
            };
            
            using var crashedWatchdog = new CrashedWatchdog<string>(
                "functionTypeId".ToFunctionTypeId(),
                (param, _) => Funcs.ToUpper(param.ToString()!),
                storeMock,
                new RFuncInvoker(
                    storeMock, 
                    DefaultSerializer.Instance,
                    new NeverExecutingSignOfLifeUpdaterFactory(),
                    new UnhandledExceptionHandler(unhandledExceptionCatcher.Catch),
                    new ShutdownCoordinator()
                ),
                checkFrequency: TimeSpan.FromMilliseconds(1),
                new UnhandledExceptionHandler(unhandledExceptionCatcher.Catch),
                new ShutdownCoordinator()
            );

            _ = crashedWatchdog.Start();
            
            BusyWait.Until(() => unhandledExceptionCatcher.ThrownExceptions.Any());
            
            unhandledExceptionCatcher.ThrownExceptions.Count.ShouldBe(1);
            (unhandledExceptionCatcher.ThrownExceptions[0] is FrameworkException).ShouldBeTrue();
        }
        
        [TestMethod]
        public void AnUnhandledInvocationExceptionIsThrownWhenRFunctionThrowsException()
        {
            var unhandledExceptionCatcher = new UnhandledExceptionCatcher();

            var storeMock = new FunctionStoreMock
            {
                SetupGetFunctionsWithStatus = (_, _, _) => 
                    new StoredFunctionStatus(
                        "it".ToFunctionInstanceId(), 
                        Epoch: 100, 
                        SignOfLife: 10, 
                        Status.Executing, 
                        PostponedUntil: null
                    ).ToList().AsEnumerable().ToTask(),
                SetupGetFunction = id => 
                    new StoredFunction(
                        id,
                        Parameter1,
                        Scrapbook: null,
                        Status.Executing,
                        Result: null,
                        Failure: null,
                        PostponedUntil: null,
                        Epoch: 0,
                        SignOfLife: 0
                    ).ToNullable().ToTask()
            };

            using var crashedWatchdog = new CrashedWatchdog<string>(
                "functionTypeId".ToFunctionTypeId(),
                (param, _) => Funcs.ThrowsException(param.ToString()!),
                storeMock,
                new RFuncInvoker(
                    storeMock, 
                    DefaultSerializer.Instance,
                    new NeverExecutingSignOfLifeUpdaterFactory(),
                    new UnhandledExceptionHandler(unhandledExceptionCatcher.Catch),
                    new ShutdownCoordinator()
                ),
                checkFrequency: TimeSpan.FromMilliseconds(1),
                new UnhandledExceptionHandler(unhandledExceptionCatcher.Catch),
                new ShutdownCoordinator()
            );

            _ = crashedWatchdog.Start();

            BusyWait.Until(() => unhandledExceptionCatcher.ThrownExceptions.Any());
            
            (unhandledExceptionCatcher.ThrownExceptions[0] is RFunctionException).ShouldBeTrue();
        }

        [TestMethod]
        public async Task DisposeInvocationCompletesAfterCurrentWorkIsCompleted()
        {
            var unhandledExceptionCatcher = new UnhandledExceptionCatcher();

            var functionGotten = new Synced<bool>();
            var disposeInvoked = new Synced<bool>();
            var disposeCompleted = new Synced<bool>();
            
            var storeMock = new FunctionStoreMock
            {
                SetupGetFunctionsWithStatus = (_, _, _) => 
                    new StoredFunctionStatus(
                        "it".ToFunctionInstanceId(), 
                        Epoch: 100, 
                        SignOfLife: 10, 
                        Status.Executing, 
                        PostponedUntil: null
                    ).ToList().AsEnumerable().ToTask(),
                SetupGetFunction = async id =>
                {
                    functionGotten.Value = true;
                    await BusyWait.UntilAsync(() => disposeInvoked.Value, maxWait: TimeSpan.FromSeconds(10));
                    return new StoredFunction(
                        id,
                        Parameter1,
                        Scrapbook: null,
                        Status.Executing,
                        Result: null,
                        Failure: null,
                        PostponedUntil: null,
                        Epoch: 0,
                        SignOfLife: 0
                    );
                }
            };
            
            using var crashedWatchdog = new CrashedWatchdog<string>(
                "functionTypeId".ToFunctionTypeId(),
                (param, _) => Funcs.ThrowsException(param.ToString()!),
                storeMock,
                new RFuncInvoker(
                    storeMock, 
                    DefaultSerializer.Instance,
                    new NeverExecutingSignOfLifeUpdaterFactory(),
                    new UnhandledExceptionHandler(unhandledExceptionCatcher.Catch),
                    new ShutdownCoordinator()
                ),
                checkFrequency: TimeSpan.FromMilliseconds(1),
                new UnhandledExceptionHandler(unhandledExceptionCatcher.Catch),
                new ShutdownCoordinator()
            );

            _ = crashedWatchdog.Start();

            await BusyWait.UntilAsync(() => functionGotten.Value);

            _ = Task.Run(() =>
            {
                // ReSharper disable once AccessToDisposedClosure
                crashedWatchdog.Dispose();
                disposeCompleted.Value = true;
            });

            await Task.Delay(100); //allow the started task above to be executed
            disposeCompleted.Value.ShouldBeFalse();

            disposeInvoked.Value = true;
            await BusyWait.UntilAsync(() => disposeCompleted.Value);
        }
    }
}