using Cleipnir.ResilientFunctions.StressTests.Common.Engines;

namespace Cleipnir.ResilientFunctions.StressTests.Common;

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