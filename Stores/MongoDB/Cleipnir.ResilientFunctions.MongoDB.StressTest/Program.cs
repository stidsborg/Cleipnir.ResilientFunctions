using Cleipnir.ResilientFunctions.StressTests;

namespace Cleipnir.ResilientFunctions.MongoDB.StressTest;

internal static class Program
{
    private static async Task Main()
    {
        var helper = new Helper();
        await AllTests.Perform(helper);
    }
}