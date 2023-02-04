using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Storage;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Shouldly;

namespace Cleipnir.ResilientFunctions.Tests.InMemoryTests
{
    [TestClass]
    public class ScrapbookTests : TestTemplates.ScrapbookTests
    {
        [TestMethod]
        public override Task SunshineScenario()
            => SunshineScenario(new InMemoryFunctionStore().CastTo<IFunctionStore>().ToTask());

        [TestMethod]
        public override Task ScrapbookIsNotUpdatedWhenEpochIsNotAsExpected()
            => ScrapbookIsNotUpdatedWhenEpochIsNotAsExpected(
                new InMemoryFunctionStore().CastTo<IFunctionStore>().ToTask()
            );

        [TestMethod]
        public override Task ConcreteScrapbookTypeIsUsedWhenSpecifiedAtRegistration()
            => ConcreteScrapbookTypeIsUsedWhenSpecifiedAtRegistration(
                new InMemoryFunctionStore().CastTo<IFunctionStore>().ToTask()
            );

        [TestMethod]
        public override Task ChangesToStateDictionaryArePersisted()
            => ChangesToStateDictionaryArePersisted(new InMemoryFunctionStore().CastTo<IFunctionStore>().ToTask());
                
        [TestMethod]
        public async Task ScrapbookThrowsExceptionWhenSavedBeforeInitialized()
        {
            var scrapbook = new TestScrapbook();
            try
            {
                await scrapbook.Save();
            }
            catch (InvalidOperationException e)
            {
                e.Message.ShouldBe("'TestScrapbook' scrapbook was uninitialized on save");
            }
        }
        
        [TestMethod]
        public async Task NewLockIsNotGrantedWhileScrapbookIsAlreadyLocked()
        {
            var scrapbook = new RScrapbook();

            var lock1 = await scrapbook.Lock();
            var lock2Task = scrapbook.Lock();
            
            lock2Task.IsCompleted.ShouldBeFalse();
            await Task.Delay(5);
            lock1.Dispose();
            await BusyWait.UntilAsync(() => lock2Task.IsCompletedSuccessfully);
        }
        
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