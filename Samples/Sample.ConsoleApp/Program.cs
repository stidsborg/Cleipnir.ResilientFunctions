using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Reactive;
using Cleipnir.ResilientFunctions.Reactive.Extensions;

namespace ConsoleApp;

internal static class Program
{
    private static async Task Main()
    {
        await WorkDistribution.Example.Perform();
    }

    private static void Do(IReactiveChain<object> messages)
    {
        messages
            .OfType<PaymentCompleted>()
            .TakeUntilTimeout("PaymentCompleted", expiresIn: TimeSpan.FromMinutes(10))
            .SuspendUntilCompletion();
        
        
    }

    private record PaymentCompleted();
}