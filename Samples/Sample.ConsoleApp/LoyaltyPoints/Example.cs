using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Storage;

namespace ConsoleApp.LoyaltyPoints;

public static class Example
{
    public static async Task Perform()
    {
        var store = new InMemoryFunctionStore();
        
        var functions = new FunctionsRegistry(
            store,
            new Settings(unhandledExceptionHandler: Console.WriteLine)
        );

        var registration = functions
            .RegisterAction<string, LoyaltyPointsFlow.State>(
                nameof(LoyaltyPointsFlow),
                LoyaltyPointsFlow.Execute
            );

        const string customerId = "USR_1231";
        await registration.Schedule(customerId, customerId);

        Console.ReadLine();
    }
}