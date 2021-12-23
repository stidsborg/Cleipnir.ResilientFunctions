using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Storage;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.Tests.InMemoryTests
{
    [TestClass]
    public class CrashedWatchdogTests : TestTemplates.CrashedWatchdogTests
    {
        [TestMethod]
        public override Task CrashedFunctionInvocationIsCompletedByWatchDog()
            => CrashedFunctionInvocationIsCompletedByWatchDog(new InMemoryFunctionStore());
    }
}