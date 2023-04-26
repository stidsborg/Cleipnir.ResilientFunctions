using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Domain.Events;
using Cleipnir.ResilientFunctions.Reactive;

namespace ConsoleApp.TravelAgency.MessagingApproach;

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
        
        var success = await eventSource
            .OfTypes<FlightBooked, HotelBooked, CarRented>()
            .Chunk(3)
            .Select(_ => true)
            .Merge(eventSource.OfType<Timeout>().Select(_ => false))
            .SuspendUntilNext();

        if (!success)
        {
            await MessageBroker.Send(new BookingFailed(bookingRequest.BookingId));
            throw new TimeoutException("All responses were not received within threshold");   
        }
        
        var flightBookingTask = await eventSource.NextOfType<FlightBooked>(); 
        var hotelBookingTask = await eventSource.NextOfType<HotelBooked>();
        var carBookingTask = await eventSource.NextOfType<CarRented>();

        await MessageBroker.Send(new BookingCompletedSuccessfully(
                bookingRequest.BookingId,
                flightBookingTask,
                hotelBookingTask,
                carBookingTask
            )
        );
    }
}