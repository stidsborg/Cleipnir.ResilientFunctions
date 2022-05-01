namespace Cleipnir.ResilientFunctions.StressTests;

public static class AllTests
{
    public static async Task Perform(IHelper helper)
    {
        await CrashedTest.Perform(helper);
        Console.WriteLine();
        await PostponedTest.Perform(helper);
    }
}