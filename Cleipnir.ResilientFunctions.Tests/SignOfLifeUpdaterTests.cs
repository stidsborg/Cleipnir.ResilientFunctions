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

namespace Cleipnir.ResilientFunctions.Tests
{
    [TestClass]
    public class SignOfLifeUpdaterTests
    {
        private readonly FunctionId _functionId = new FunctionId("functionId", "instanceId");
        private UnhandledExceptionCatcher _unhandledExceptionCatcher = new();
        
        [TestInitialize]
        public void SetUp() => _unhandledExceptionCatcher = new();

        [TestMethod]
        public async Task AfterSignOfLifeIsStartedStoreIsInvokedContinuouslyWithExpectedDelay()
        {
            const long expectedSignOfLife = 100;

            var mock = new Mock<IFunctionStore>();
            var invocations = new SyncedList<Parameters>();

            mock.Setup(s => s.UpdateSignOfLife(It.IsAny<FunctionId>(), It.IsAny<long>(), It.IsAny<long>()))
                .Callback<FunctionId, long, long>(
                    (functionId, expectedSignOfLife, newSignOfLife)
                        => invocations.Add(new Parameters(functionId, expectedSignOfLife, newSignOfLife))
                )
                .Returns(true.ToTask());

            var updaterFactory = new SignOfLifeUpdaterFactory(
                mock.Object,
                _unhandledExceptionCatcher.Catch,
                TimeSpan.FromMilliseconds(10)
            );
            using var updater = updaterFactory.CreateAndStart(_functionId, expectedSignOfLife);

            await Task.Delay(100);
            updater.Dispose();

            invocations.Count.ShouldBeGreaterThan(2);
            invocations.All(p => p.FunctionId == _functionId).ShouldBeTrue();

            _ = invocations.Aggregate(expectedSignOfLife, (prevSignOfLife, parameters) =>
            {
                parameters.ExpectedSignOfLife.ShouldBe(prevSignOfLife);
                prevSignOfLife.ShouldBeLessThan(parameters.NewSignOfLife);
                return parameters.NewSignOfLife;
            });
        }

        [TestMethod]
        public async Task SignOfLifeStopsInvokingStoreWhenFalseIsReturnedFromStore()
        {
            const long expectedSignOfLife = 100;
            
            var mock = new Mock<IFunctionStore>();
            mock.Setup(s => s.UpdateSignOfLife(It.IsAny<FunctionId>(), It.IsAny<long>(), It.IsAny<long>()))
                .Returns(false.ToTask());
            
            var updaterFactory = new SignOfLifeUpdaterFactory(
                mock.Object, 
                _unhandledExceptionCatcher.Catch,
                TimeSpan.FromMilliseconds(10)
            );
            using var updater = updaterFactory.CreateAndStart(_functionId, expectedSignOfLife);

            await Task.Delay(100);
            updater.Dispose();

            mock.Invocations.Count.ShouldBe(1);
            _unhandledExceptionCatcher.ThrownExceptions.ShouldBeEmpty();
        }
        
        [TestMethod]
        public void WhenFunctionStoreThrowsExceptionAnTheUnhandledExceptionActionIsInvokedWithAFrameworkException()
        {
            const long expectedSignOfLife = 100;

            var mock = new Mock<IFunctionStore>();
            mock.Setup(s => s.UpdateSignOfLife(It.IsAny<FunctionId>(), It.IsAny<long>(), It.IsAny<long>()))
                .Throws<FrameworkException>();
            
            var updaterFactory = new SignOfLifeUpdaterFactory(
                mock.Object, 
                _unhandledExceptionCatcher.Catch,
                TimeSpan.FromMilliseconds(1)
            );

            updaterFactory.CreateAndStart(_functionId, expectedSignOfLife);
            BusyWait.Until(() => _unhandledExceptionCatcher.ThrownExceptions.Any());
            
            _unhandledExceptionCatcher.ThrownExceptions.Count.ShouldBe(1);
            var thrownException = _unhandledExceptionCatcher.ThrownExceptions[0];
            (thrownException is FrameworkException).ShouldBeTrue();
            mock.Invocations.Count.ShouldBe(1);
        }

        private record Parameters(FunctionId FunctionId, long ExpectedSignOfLife, long NewSignOfLife);
    }
}