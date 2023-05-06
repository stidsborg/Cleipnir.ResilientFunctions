using System;

namespace ConsoleApp.TravelAgency;

public record BookingRequest(Guid BookingId, Guid CustomerId, decimal Amount, string Details);