using System;
using Cleipnir.ResilientFunctions;
using Cleipnir.ResilientFunctions.InnerDecorators;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Utils.Scrapbooks;

namespace ConsoleApp.Backoff.Exponential;

public static class Example
{
    public static void Perform()
    {
        var functionType = "ExponentialBackoffExample";
        var store = new InMemoryFunctionStore();
        using var rFunctions = new RFunctions(store);
        var rFunc = rFunctions.RegisterFunc(
            functionType,
            OnFailure.BackoffExponentially<string, BackoffScrapbook, string>(
                Inner,
                firstDelay: TimeSpan.FromMilliseconds(1000),
                factor: 2,
                maxRetries: 3,
                onException: LogError
            )
        ).Invoke;

        _ = rFunc("InstanceId1", "hello world");
        
        Console.WriteLine("Press enter to exit");
        Console.ReadLine();
    }

    private static string Inner(string param, BackoffScrapbook scrapbook)
    {
        Console.WriteLine("Invoking Inner-function: " + DateTime.Now);
        if (scrapbook.Retry == 3)
        {
            Console.WriteLine("Invocation succeeded");
            return param.ToUpper();
        }
        throw new TimeoutException("Operation timed out");
    }

    private static void LogError(Exception exception, BackoffScrapbook scrapbook) => Console.WriteLine(exception);
}