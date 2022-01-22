namespace Sample.WebApi.Model;

public class Order
{
    public Guid Id { get; }
    public string HotelOrder { get; }
    public string FlightOrder { get; }
    
    public Order(Guid id, string hotelOrder, string flightOrder)
    {
        Id = id;
        HotelOrder = hotelOrder;
        FlightOrder = flightOrder;
    }
}