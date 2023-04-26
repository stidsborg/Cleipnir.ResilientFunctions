using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Storage;
using ConsoleApp.TravelAgency.MessagingApproach;
using ConsoleApp.TravelAgency.MessagingApproach.ExternalServices;

namespace ConsoleApp.TravelAgency;

public static class Example
{
    public static async Task PerformMessagingApproach()
    {
        AirlineService.Start();
        CarRentalService.Start();
        HotelBookingService.Start();
        
        var store = new InMemoryFunctionStore();
        
        var functions = new RFunctions(
            store,
            new Settings(unhandledExceptionHandler: Console.WriteLine)
        );

        var registration = functions
            .RegisterAction<BookingRequest, RScrapbook>(
                functionTypeId: "TravelBooking",
                Saga.BookTravel
            );

        var eventSourceWriters = registration.EventSourceWriters;
        MessageBroker.Subscribe(async @event =>
        {
            if (@event is HotelBooked hotelBooking)
                await eventSourceWriters.For(hotelBooking.BookingId.ToString()).AppendEvent(hotelBooking);
            else if (@event is CarRented carBooking)
                await eventSourceWriters.For(carBooking.BookingId.ToString()).AppendEvent(carBooking);
            else if (@event is HotelBooked booking)
                await eventSourceWriters.For(booking.BookingId.ToString()).AppendEvent(booking);
        });
        
        var bookingRequest = new BookingRequest(
            BookingId: Guid.NewGuid(),
            CustomerId: Guid.NewGuid(),
            Amount: 5200.00M,
            Details: "bookings details in some awesome json format!"
        );
        
        await registration.Schedule(bookingRequest.BookingId.ToString(), bookingRequest);
    }
}