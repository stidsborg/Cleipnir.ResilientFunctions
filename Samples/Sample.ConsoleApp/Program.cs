using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.SqlServer;
using Microsoft.Data.SqlClient;

namespace ConsoleApp
{
    internal static class Program
    {
        private static void Main(string[] args)
        {
            var store = new SqlServerFunctionStore(CreateConnection);
            store.Initialize();
            store.Truncate();
            
            _ = Service1();
            _ = Service2();

            Console.WriteLine("PRESS ENTER TO EXIT");
            Console.ReadLine();
        }

        private static async Task Service1()
        {
            var functions = RFunctions.Create(
                new SqlServerFunctionStore(CreateConnection),
                unhandledExceptionHandler: Console.WriteLine,
                unhandledFunctionsCheckFrequency: TimeSpan.Zero
            );

            var callApi = functions.Register(
                "call.api".ToFunctionTypeId(),
                default(string),
                new ApiCaller(true, 1).CallApi
            );

            _ = callApi("input"); //will fail
            await Task.Delay(2_000);
            
            var output = await callApi("input");
            Console.WriteLine($"[SERVICE1] Function Return Value: '{output}'");
        }
        
        private static async Task Service2()
        {
            var functions = RFunctions.Create(
                new SqlServerFunctionStore(CreateConnection),
                unhandledExceptionHandler: Console.WriteLine,
                unhandledFunctionsCheckFrequency: TimeSpan.FromMilliseconds(4_000)
            );

           var callApi = functions.Register(
                "call.api".ToFunctionTypeId(),
                default(string),
                new ApiCaller(false,2).CallApi
            );
           
           await Task.Delay(2_000);
            
           var output = await callApi("input");
           Console.WriteLine($"[SERVICE2] Function Return Value: '{output}'");
        }

        private static async Task<SqlConnection> CreateConnection()
        {
            const string connectionString = "Server=localhost;Database=master;User Id=sa;Password=Pa55word!";
            var connection = new SqlConnection(connectionString);
            await connection.OpenAsync();
            return connection;
        }

        private class ApiCaller
        {
            private readonly bool _shouldFail;
            private readonly int _service;

            public ApiCaller(bool shouldFail, int service)
            {
                _shouldFail = shouldFail;
                _service = service;
            }

            public async Task<string> CallApi(string input)
            {
                Console.WriteLine($"[SERVICE{_service}] Executing CallApi");
                await Task.Delay(1_000);
                if (_shouldFail)
                {
                    Console.WriteLine($"[SERVICE{_service}] Throwing Exception");
                    throw new Exception("api call failed");
                }
                
                return "output";
            }
        }
    }
}