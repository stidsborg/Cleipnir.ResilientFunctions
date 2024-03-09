using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Storage;

namespace ConsoleApp.TravelAgency;

public class Example
{
    public static async Task Perform()
    {
        var store = new InMemoryFunctionStore();
        var functions = new FunctionsRegistry(store, new Settings(unhandledExceptionHandler: Console.WriteLine));

        var rFunc = functions.RegisterAction<BookingRequest>(
            functionTypeId: "TravelAgency",
            inner: Saga.BookTravel
        );

        var bookingRequest = new BookingRequest(
            BookingId: Guid.NewGuid(),
            CustomerId: Guid.NewGuid(),
            Amount: 120,
            Details: "Vegetarian"
        );
        await rFunc.Schedule(bookingRequest.BookingId.ToString(), bookingRequest);

        Console.WriteLine("Waiting for completion");
        Console.ReadLine();
    }
}