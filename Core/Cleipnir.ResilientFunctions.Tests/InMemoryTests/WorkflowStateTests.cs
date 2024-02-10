using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Shouldly;

namespace Cleipnir.ResilientFunctions.Tests.InMemoryTests
{
    [TestClass]
    public class WorkflowStateTests : TestTemplates.WorkflowStateTests
    {
        [TestMethod]
        public override Task SunshineScenario()
            => SunshineScenario(FunctionStoreFactory.Create());

        [TestMethod]
        public override Task StateIsNotUpdatedWhenEpochIsNotAsExpected()
            => StateIsNotUpdatedWhenEpochIsNotAsExpected(
                FunctionStoreFactory.Create()
            );

        [TestMethod]
        public override Task ConcreteStateTypeIsUsedWhenSpecifiedAtRegistration()
            => ConcreteStateTypeIsUsedWhenSpecifiedAtRegistration(
                FunctionStoreFactory.Create()
            );

        [TestMethod]
        public override Task ChangesToStateDictionaryArePersisted()
            => ChangesToStateDictionaryArePersisted(FunctionStoreFactory.Create());
        
        [TestMethod]
        public void StateThrowsExceptionIsInitializedMultipleTimes()
        {
            var state = new WorkflowState();
            state.Initialize(() => Task.CompletedTask);
            Should.Throw<InvalidOperationException>(() => state.Initialize(() => Task.CompletedTask));
        }
        
        [TestMethod]
        public void StateThrowsExceptionIfSavedBeforeInitialized()
        {
            var state = new WorkflowState();
            Should.ThrowAsync<InvalidOperationException>(() => state.Save());
        }
    }
}