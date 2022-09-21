using Cleipnir.ResilientFunctions;
using Cleipnir.ResilientFunctions.AspNetCore.Core;
using Cleipnir.ResilientFunctions.Domain;
using Sample.WebApi.Model;
using Sample.WebApi.Utils;

namespace Sample.WebApi.Saga;

public class BookingScrapbook : RScrapbook
{
    public string? HotelBooking { get; set; }
    public string? FlightBooking { get; set; }
}

public class BookingSaga : IRegisterRFuncOnInstantiation
{
    private readonly ILogger<BookingSaga> _logger;
    private readonly HttpClient _httpClient = new();
    private const string FLIGHT_URL = "https://postman-echo.com/post";
    private const string HOTEL_URL = "https://postman-echo.com/post";

    public RFunc.Invoke<OrderAndRequestIds, BookingScrapbook, Booking> BookTravel { get; }
    
    public BookingSaga(RFunctions rFunctions, ILogger<BookingSaga> logger)
    {
        _logger = logger;
        
        BookTravel = rFunctions.RegisterFunc<OrderAndRequestIds, BookingScrapbook, Booking>(
            nameof(BookingSaga).ToFunctionTypeId(),
            _BookTravel
        ).Invoke;
    }

    private async Task<Booking> _BookTravel(OrderAndRequestIds param, BookingScrapbook scrapbook)
    {
        _logger.LogInformation("Started booking of order {OrderId}", param.Order.Id);
        
        var (order, flightRequestId, hotelRequestId) = param;

        var flightBooking = await BookFlight(order, flightRequestId, scrapbook);
        var hotelBooking = await BookHotel(order, hotelRequestId, scrapbook);

        _logger.LogInformation("Completed booking of order {OrderId}", param.Order.Id);
        
        return new Booking(flightBooking, hotelBooking);
    }

    private async Task<string> BookFlight(Order order, Guid flightRequestId, BookingScrapbook scrapbook)
    {
        if (scrapbook.FlightBooking != null) return scrapbook.FlightBooking;
        
        var flightResponse = await _httpClient.PostAsync(
            FLIGHT_URL,
            new StringContent(
                new FlightOrder(
                    flightRequestId.ToString("N"),
                    order.FlightOrder
                ).ToJson()
            )
        );
        flightResponse.EnsureSuccessStatusCode();
        var content = await flightResponse.Content.ReadAsStringAsync();
        scrapbook.FlightBooking = content;
        await scrapbook.Save();

        return scrapbook.FlightBooking;
    }
    
    private async Task<string> BookHotel(Order order, Guid flightRequestId, BookingScrapbook scrapbook)
    {
        if (scrapbook.HotelBooking != null) return scrapbook.HotelBooking;
        
        var hotelResponse = await _httpClient.PostAsync(
            HOTEL_URL,
            new StringContent(
                new HotelOrder(
                    flightRequestId.ToString("N"),
                    order.FlightOrder
                ).ToJson()
            )
        );
        hotelResponse.EnsureSuccessStatusCode();
        var content = await hotelResponse.Content.ReadAsStringAsync();
        scrapbook.HotelBooking = content;
        await scrapbook.Save();

        return scrapbook.HotelBooking;
    }

    private record FlightOrder(string RequestId, string BookingInformation);
    private record HotelOrder(string RequestId, string BookingInformation);
}