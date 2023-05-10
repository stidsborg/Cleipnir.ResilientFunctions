﻿using System;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Domain.Events;
using Cleipnir.ResilientFunctions.Reactive;

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
                await eventSource.TimeoutProvider.RegisterTimeout("timeout", expiresIn: TimeSpan.FromMinutes(1));
            }
        );

        
        var first3 = await eventSource.Chunk(3).Next();
        if (first3.OfType<Timeout>().Any())
        {
            await MessageBroker.Send(new BookingFailed(bookingRequest.BookingId));
            throw new TimeoutException("All responses were not received within threshold");
        }
        
        var flightBooking = first3.OfType<FlightBooked>().Single(); 
        var hotelBooking = first3.OfType<HotelBooked>().Single();
        var carBooking = first3.OfType<CarRented>().Single();

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