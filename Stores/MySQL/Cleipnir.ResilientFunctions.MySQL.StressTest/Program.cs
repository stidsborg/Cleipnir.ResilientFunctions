using Cleipnir.ResilientFunctions.StressTests;
using Cleipnir.ResilientFunctions.StressTests.Common;

namespace Cleipnir.ResilientFunctions.MySQL.StressTest;

internal static class Program
{
    private static async Task Main()
    {
        var helper = new Helper();
        await AllTests.Perform(helper);
    }
}