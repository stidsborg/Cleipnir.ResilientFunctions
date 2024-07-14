using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Storage;

namespace ConsoleApp.CustomerSignUp;

public static class Example
{
    public static async Task Perform()
    {
        var store = new InMemoryFunctionStore();
        
        var functions = new FunctionsRegistry(
            store,
            new Settings(unhandledExceptionHandler: Console.WriteLine)
        );

        var rAction = functions
            .RegisterAction<string>(
             "CustomerSignupFlow",
                SignupFlow.Start
            ).Invoke;

        var offerDate = new DateOnly(2022, 1, 1);
        await rAction(
            flowInstance: offerDate.ToString(),
            param: "billgates@microsoft.net"
        );
        
        Console.WriteLine("Offers sent successfully");
    }
}