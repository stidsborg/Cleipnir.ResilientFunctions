using System;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Tests.Utils;
using Cleipnir.ResilientFunctions.Utils;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Moq;
using Shouldly;
using static Cleipnir.ResilientFunctions.Tests.Utils.TestUtils;

namespace Cleipnir.ResilientFunctions.Tests.InMemoryTests
{
    [TestClass]
    public class UnhandledWatchdogTests : Tests.UnhandledWatchdogTests
    {
        [TestMethod]
        public override Task UnhandledFunctionInvocationIsCompletedByWatchDog()
            => UnhandledFunctionInvocationIsCompletedByWatchDog(new InMemoryFunctionStore());
        
        [TestMethod]
        public void TheUnhandledExceptionActionIsInvokedWithAFrameworkExceptionWhenFunctionStoreThrowsException()
        {
            var unhandledExceptionCatcher = new UnhandledExceptionCatcher();

            var storeMock = new Mock<IFunctionStore>();
            storeMock
                .Setup(s => s.GetNonCompletedFunctions(It.IsAny<FunctionTypeId>(), It.IsAny<long>()))
                .Throws<FrameworkException>();
            var store = storeMock.Object;
            
            using var watchDog = new UnhandledRFunctionWatchdog<string, string>(
                "functionTypeId".ToFunctionTypeId(),
                RFunc.ToUpper,
                store,
                CreateNeverExecutionSignOfLifeUpdaterFactory(),
                TimeSpan.FromMilliseconds(1),
                unhandledExceptionCatcher.Catch
            );

            _ = watchDog.Start();

            BusyWait.Until(() => unhandledExceptionCatcher.ThrownExceptions.Any());
            
            unhandledExceptionCatcher.ThrownExceptions.Count.ShouldBe(1);
            (unhandledExceptionCatcher.ThrownExceptions[0] is FrameworkException).ShouldBeTrue();
        }
        
        [TestMethod]
        public void TheUnhandledExceptionActionIsInvokedWithAFrameworkExceptionWhenStoreResultOnFunctionStoreThrowsException()
        {
            var unhandledExceptionCatcher = new UnhandledExceptionCatcher();

            var storeMock = new Mock<IFunctionStore>();
            storeMock
                .Setup(s => s.GetNonCompletedFunctions(It.IsAny<FunctionTypeId>(), It.IsAny<long>()))
                .Returns(
                    new StoredFunction(
                        new FunctionId("ft", "it"),
                        "".ToJson(),
                        typeof(string).SimpleQualifiedName(),
                        100
                    ).ToList().AsEnumerable().ToTask()
                );
            storeMock
                .Setup(s => s.UpdateSignOfLife(It.IsAny<FunctionId>(), It.IsAny<long>(), It.IsAny<long>()))
                .Returns(true.ToTask());
            storeMock
                .Setup(s => s.StoreFunctionResult(It.IsAny<FunctionId>(), It.IsAny<string>(), It.IsAny<string>()))
                .Throws<FrameworkException>();

            var store = storeMock.Object;
            
            using var watchDog = new UnhandledRFunctionWatchdog<string, string>(
                "functionTypeId".ToFunctionTypeId(),
                RFunc.ToUpper,
                store,
                CreateNeverExecutionSignOfLifeUpdaterFactory(),
                TimeSpan.FromMilliseconds(1),
                unhandledExceptionCatcher.Catch
            );

            _ = watchDog.Start();

            BusyWait.Until(() => unhandledExceptionCatcher.ThrownExceptions.Any());
            
            unhandledExceptionCatcher.ThrownExceptions.Count.ShouldBe(1);
            (unhandledExceptionCatcher.ThrownExceptions[0] is FrameworkException).ShouldBeTrue();
        }
        
        [TestMethod]
        public void TheUnhandledExceptionActionIsInvokedWithRFunctionExceptionWhenRFunctionThrowsException()
        {
            var unhandledExceptionCatcher = new UnhandledExceptionCatcher();

            var storeMock = new Mock<IFunctionStore>();
            storeMock
                .Setup(s => s.GetNonCompletedFunctions(It.IsAny<FunctionTypeId>(), It.IsAny<long>()))
                .Returns(
                    new StoredFunction(
                        new FunctionId("ft", "it"),
                        "".ToJson(),
                        typeof(string).SimpleQualifiedName(),
                        100
                    ).ToList().AsEnumerable().ToTask()
                );
            storeMock
                .Setup(s => s.UpdateSignOfLife(It.IsAny<FunctionId>(), It.IsAny<long>(), It.IsAny<long>()))
                .Returns(true.ToTask());

            var store = storeMock.Object;
            
            using var watchDog = new UnhandledRFunctionWatchdog<string, string>(
                "functionTypeId".ToFunctionTypeId(),
                RFunc.ThrowsException,
                store,
                CreateNeverExecutionSignOfLifeUpdaterFactory(),
                TimeSpan.FromMilliseconds(1),
                unhandledExceptionCatcher.Catch
            );

            _ = watchDog.Start();
            
            BusyWait.Until(() => unhandledExceptionCatcher.ThrownExceptions.Any());
            
            (unhandledExceptionCatcher.ThrownExceptions[0] is RFunctionException).ShouldBeTrue();
        }
    }
}