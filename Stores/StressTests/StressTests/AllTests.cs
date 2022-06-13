using Cleipnir.ResilientFunctions.StressTests.Engines;

namespace Cleipnir.ResilientFunctions.StressTests.StressTests;

public static class AllTests
{
    public static async Task Perform(IEngine helper)
    {
        await CrashedTest.Perform(helper);
        Console.WriteLine();
        await PostponedTest.Perform(helper);
        Console.WriteLine();
    }
}