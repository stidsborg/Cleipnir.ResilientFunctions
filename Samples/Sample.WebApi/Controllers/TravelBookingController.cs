using Cleipnir.ResilientFunctions.AspNetCore;
using Microsoft.AspNetCore.Mvc;
using Sample.WebApi.Model;
using Sample.WebApi.Saga;

namespace Sample.WebApi.Controllers;

[ApiController]
[Route("[controller]")]
public class TravelBookingController : ControllerBase, IRegisterRFuncOnInstantiation
{
    private readonly ILogger<TravelBookingController> _logger;
    private readonly BookingSaga _bookingSaga;

    public TravelBookingController(ILogger<TravelBookingController> logger, BookingSaga bookingSaga)
    {
        _logger = logger;
        _bookingSaga = bookingSaga;
    }
    
    [HttpPost]
    public async Task<Booking> Post(Order order)
    {
        var orderAndRequestIds = new OrderAndRequestIds(
            order,
            FlightRequestId: Guid.NewGuid(),
            HotelRequestId: Guid.NewGuid()
        );
        var result = await _bookingSaga.BookTravel.Invoke(order.Id.ToString(), orderAndRequestIds);
        var booking = result.EnsureSuccess();
        return booking;
    }
}