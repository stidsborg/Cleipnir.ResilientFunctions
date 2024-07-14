using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Storage;
using ConsoleApp.TravelAgency.ExternalServices;

namespace ConsoleApp.TravelAgency;

public class Example
{
    public static async Task Perform()
    {
        var store = new InMemoryFunctionStore();
        var functions = new FunctionsRegistry(
            store,
            new Settings(
                unhandledExceptionHandler: Console.WriteLine,
                leaseLength: TimeSpan.FromSeconds(5)
            )
        );

        var registration = functions.RegisterAction<BookingRequest>(
            flowType: "TravelAgency",
            inner: Saga.BookTravel
        );

        AirlineService.Start();
        CarRentalService.Start();
        HotelBookingService.Start();
        
        var bookingRequest = new BookingRequest(
            BookingId: Guid.NewGuid(),
            CustomerId: Guid.NewGuid(),
            Amount: 120,
            Details: "Vegetarian"
        );
        await registration.Schedule(
            bookingRequest.BookingId.ToString(),
            bookingRequest
        );
        
        MessageBroker.Subscribe(async @event =>
        {
            var bookingId = @event switch
            {
                CarRented carRented => carRented.BookingId,
                FlightBooked flightBooked => flightBooked.BookingId,
                HotelBooked hotelBooked => hotelBooked.BookingId,
                _ => default(Guid?)
            };
            if (bookingId is null)
                return;

            await registration
                .MessageWriters
                .For(bookingId.ToString()!)
                .AppendMessage(@event);
        });

        Console.WriteLine("Waiting for completion");
        Console.ReadLine();
    }
}