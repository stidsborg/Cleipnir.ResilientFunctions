using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Storage;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.Tests.InMemoryTests
{
    [TestClass]
    public class UnhandledWatchdogTests : Tests.UnhandledWatchdogTests
    {
        [TestMethod]
        public override Task UnhandledFunctionInvocationIsCompletedByWatchDog()
            => UnhandledFunctionInvocationIsCompletedByWatchDog(new InMemoryFunctionStore());
    }
}