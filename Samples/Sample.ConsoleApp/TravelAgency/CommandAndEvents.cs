using System;

namespace ConsoleApp.TravelAgency;

public record CommandAndEvents;
public record BookFlight(Guid BookingId, Guid CustomerId, string Details) : CommandAndEvents;
public record FlightBooked(Guid BookingId) : CommandAndEvents;
public record BookHotel(Guid BookingId, Guid CustomerId, string Details) : CommandAndEvents;
public record HotelBooked(Guid BookingId) : CommandAndEvents;
public record RentCar(Guid BookingId, Guid CustomerId, string Details) : CommandAndEvents;
public record CarRented(Guid BookingId) : CommandAndEvents;

public record BookingCompletedSuccessfully(Guid BookingId, FlightBooked FlightBooking, HotelBooked HotelBooking, CarRented CarBooking) : CommandAndEvents;
public record BookingFailed(Guid BookingId) : CommandAndEvents;

