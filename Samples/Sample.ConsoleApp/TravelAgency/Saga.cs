using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Cleipnir.ResilientFunctions.Reactive.Extensions;

namespace ConsoleApp.TravelAgency;

public static class Saga
{
    public static async Task BookTravel(BookingRequest bookingRequest, Workflow workflow)
    {
        var (effect, messages, _) = workflow;
        var (bookingId, customerId, amount, details) = bookingRequest;
        
        await effect.Capture(
            "EmitRequests", 
            async () =>
            {
                await MessageBroker.Send(new BookFlight(bookingId, customerId, details));
                await MessageBroker.Send(new BookHotel(bookingId, customerId, details));
                await MessageBroker.Send(new RentCar(bookingId, customerId, details));
            }
        );
        
        var events = await messages
            .TakeUntilTimeout("TimeoutId", expiresIn: TimeSpan.FromMinutes(1))
            .Take(3)
            .Completion();
        
        if (events.Count != 3)
        {
            await MessageBroker.Send(new BookingFailed(bookingRequest.BookingId));
            //optionally perform compensating actions
            throw new TimeoutException("All responses were not received within threshold");
        }
        
        var flightBooking = await messages.OfType<FlightBooked>().First();
        var hotelBooking = await messages.OfType<HotelBooked>().First();
        var carBooking = await messages.OfType<CarRented>().First();

        await MessageBroker.Send(
            new BookingCompletedSuccessfully(
                bookingRequest.BookingId,
                flightBooking,
                hotelBooking,
                carBooking
            )
        );
    }
}