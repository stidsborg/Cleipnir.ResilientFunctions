namespace Sample.WebApi.Model;

public class Booking
{
    public string FlightBookingId { get; }
    public string HotelBookingId { get; }
    
    public Booking(string flightBookingId, string hotelBookingId)
    {
        FlightBookingId = flightBookingId;
        HotelBookingId = hotelBookingId;
    }
}