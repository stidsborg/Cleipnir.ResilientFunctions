using System;
using System.Diagnostics;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Storage;

namespace ConsoleApp
{
    internal static class Program
    {
        private static readonly HeisenSwitch Switch = new();
        private static async Task Main()
        {
            var store = new InMemoryFunctionStore();
            {
                var rFunctions = RFunctions.Create(
                    store,
                    postponedCheckFrequency: TimeSpan.FromMilliseconds(0),
                    crashedCheckFrequency: TimeSpan.FromMilliseconds(0)
                );
                
                var rFunc = rFunctions.Register<string, string>(
                    "stuff".ToFunctionTypeId(),
                    Func,
                    _ => _
                );

                _ = rFunc("stuff");
            }
            {
                var rFunctions = RFunctions.Create(
                    store,
                    postponedCheckFrequency: TimeSpan.FromMilliseconds(0),
                    crashedCheckFrequency: TimeSpan.FromMilliseconds(1000)
                );
                _ = rFunctions.Register<string, string>(
                    "stuff".ToFunctionTypeId(),
                    Func,
                    _ => _
                );
            }

            await Task.Delay(100_000);
        }

        private static async Task<RResult<string>> Func(string s)
        {
            Console.WriteLine($"INVOKING({DateTime.UtcNow.ToString("O")}) '{s}'");
            Console.WriteLine(new StackTrace());
            
            await Task.CompletedTask;
            if (!Switch) await Task.Delay(1000000);

            return s.ToUpper();
        }
    }
}