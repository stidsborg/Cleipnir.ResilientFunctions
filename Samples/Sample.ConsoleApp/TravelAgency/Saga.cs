using System;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Domain.Events;
using Cleipnir.ResilientFunctions.Messaging;
using Cleipnir.ResilientFunctions.Reactive;
using Cleipnir.ResilientFunctions.Reactive.Extensions;

namespace ConsoleApp.TravelAgency;

public static class Saga
{
    public static async Task BookTravel(BookingRequest bookingRequest, RScrapbook scrapbook, Context context)
    {
        var eventSource = await context.EventSource;
        var (bookingId, customerId, amount, details) = bookingRequest;
        
        await scrapbook.DoAtMostOnce(
            workId: "SendRequests", 
            async () =>
            {
                await MessageBroker.Send(new BookFlight(bookingId, customerId, details));
                await MessageBroker.Send(new BookHotel(bookingId, customerId, details));
                await MessageBroker.Send(new RentCar(bookingId, customerId, details));
            }
        );
        
        var events = await eventSource
            .Take(3)
            .TakeUntilTimeout("TimeoutId", TimeSpan.FromMinutes(1))
            .SuspendUntilCompletion(maxWait: TimeSpan.FromSeconds(5));
        
        if (events.Count != 3)
        {
            await MessageBroker.Send(new BookingFailed(bookingRequest.BookingId));
            //optionally perform compensating actions
            throw new TimeoutException("All responses were not received within threshold");
        }
        
        var flightBooking = await eventSource.OfType<FlightBooked>().First();
        var hotelBooking = await eventSource.OfType<HotelBooked>().First();
        var carBooking = await eventSource.OfType<CarRented>().First();

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