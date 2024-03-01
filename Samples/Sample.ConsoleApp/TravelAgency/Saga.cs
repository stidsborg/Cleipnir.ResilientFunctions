using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Reactive.Extensions;

namespace ConsoleApp.TravelAgency;

public static class Saga
{
    public static async Task BookTravel(BookingRequest bookingRequest, WorkflowState state, Workflow workflow)
    {
        var messages = workflow.Messages;
        var (bookingId, customerId, amount, details) = bookingRequest;
        
        await workflow.Effect.Capture(
            "SendRequests", 
            async () =>
            {
                await MessageBroker.Send(new BookFlight(bookingId, customerId, details));
                await MessageBroker.Send(new BookHotel(bookingId, customerId, details));
                await MessageBroker.Send(new RentCar(bookingId, customerId, details));
            }
        );
        
        var events = await messages
            .Take(3)
            .TakeUntilTimeout("TimeoutId", TimeSpan.FromMinutes(1))
            .SuspendUntilCompletion(maxWait: TimeSpan.FromSeconds(5));
        
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