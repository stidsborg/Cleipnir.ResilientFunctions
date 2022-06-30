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
        public override Task ScrapbookIsNotUpdatedWhenVersionStampIsNotAsExpected()
            => ScrapbookIsNotUpdatedWhenVersionStampIsNotAsExpected(
                new InMemoryFunctionStore().CastTo<IFunctionStore>().ToTask()
            );

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
        
        private class TestScrapbook : Scrapbook {}
    }
}