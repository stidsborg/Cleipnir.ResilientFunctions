using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Storage;
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
        
        [TestMethod]
        public async Task AfterSignOfLifeIsStartedStoreIsInvokedContinuouslyWithExpectedDelay()
        {
            const long expectedSignOfLife = 100;
            
            var mock = new Mock<IFunctionStore>();
            var sync = new object();
            var invocations = new List<Parameters>();

            mock.Setup(s => s.UpdateSignOfLife(It.IsAny<FunctionId>(), It.IsAny<long>(), It.IsAny<long>()))
                .Callback<FunctionId, long, long>(
                    (functionId, expectedSignOfLife, newSignOfLife) =>
                    {
                        lock (sync)
                            invocations.Add(new Parameters(functionId, expectedSignOfLife, newSignOfLife));
                    })
                .Returns(true.ToTask());
            
            var updaterFactory = new SignOfLifeUpdaterFactory(
                mock.Object, 
                TimeSpan.FromMilliseconds(10)
            );
            using var updater = updaterFactory.CreateAndStart(_functionId, expectedSignOfLife);

            await Task.Delay(100);
            updater.Dispose();

            lock (sync)
            {
                invocations.Count.ShouldBeGreaterThan(2);
                invocations.All(p => p.FunctionId == _functionId).ShouldBeTrue();

                _ = invocations.Aggregate(expectedSignOfLife, (prevSignOfLife, parameters) =>
                {
                    parameters.ExpectedSignOfLife.ShouldBe(prevSignOfLife);
                    prevSignOfLife.ShouldBeLessThan(parameters.NewSignOfLife);
                    return parameters.NewSignOfLife;
                });
            }
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
                TimeSpan.FromMilliseconds(10)
            );
            using var updater = updaterFactory.CreateAndStart(_functionId, expectedSignOfLife);

            await Task.Delay(100);
            updater.Dispose();

            mock.Invocations.Count.ShouldBe(1);
        }

        private record Parameters(FunctionId FunctionId, long ExpectedSignOfLife, long NewSignOfLife);
    }
}