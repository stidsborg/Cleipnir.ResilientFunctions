using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Storage;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.Tests.InMemoryTests
{
    [TestClass]
    public class CrashedWatchdogTests : TestTemplates.WatchDogsTests.CrashedWatchdogTests
    {
        [TestMethod]
        public override Task CrashedFunctionInvocationIsCompletedByWatchDog()
            => CrashedFunctionInvocationIsCompletedByWatchDog(new InMemoryFunctionStore());

        [TestMethod]
        public override Task CrashedFunctionWithScrapbookInvocationIsCompletedByWatchDog()
            => CrashedFunctionWithScrapbookInvocationIsCompletedByWatchDog(new InMemoryFunctionStore());

        [TestMethod]
        public override Task CrashedActionInvocationIsCompletedByWatchDog()
            => CrashedActionInvocationIsCompletedByWatchDog(new InMemoryFunctionStore());

        [TestMethod]
        public override Task CrashedActionWithScrapbookInvocationIsCompletedByWatchDog()
            => CrashedActionWithScrapbookInvocationIsCompletedByWatchDog(new InMemoryFunctionStore());
    }
}