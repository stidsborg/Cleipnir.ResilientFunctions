using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Shouldly;

namespace Cleipnir.ResilientFunctions.Tests.InMemoryTests
{
    [TestClass]
    public class ScrapbookTests : TestTemplates.ScrapbookTests
    {
        [TestMethod]
        public override Task SunshineScenario()
            => SunshineScenario(FunctionStoreFactory.Create());

        [TestMethod]
        public override Task ScrapbookIsNotUpdatedWhenEpochIsNotAsExpected()
            => ScrapbookIsNotUpdatedWhenEpochIsNotAsExpected(
                FunctionStoreFactory.Create()
            );

        [TestMethod]
        public override Task ConcreteScrapbookTypeIsUsedWhenSpecifiedAtRegistration()
            => ConcreteScrapbookTypeIsUsedWhenSpecifiedAtRegistration(
                FunctionStoreFactory.Create()
            );

        [TestMethod]
        public override Task ChangesToStateDictionaryArePersisted()
            => ChangesToStateDictionaryArePersisted(FunctionStoreFactory.Create());
        
        [TestMethod]
        public void ScrapbookThrowsExceptionIsInitializedMultipleTimes()
        {
            var scrapbook = new RScrapbook();
            scrapbook.Initialize(() => Task.CompletedTask);
            Should.Throw<InvalidOperationException>(() => scrapbook.Initialize(() => Task.CompletedTask));
        }
        
        [TestMethod]
        public void ScrapbookThrowsExceptionIfSavedBeforeInitialized()
        {
            var scrapbook = new RScrapbook();
            Should.ThrowAsync<InvalidOperationException>(() => scrapbook.Save());
        }
        
        private class TestScrapbook : RScrapbook {}
    }
}