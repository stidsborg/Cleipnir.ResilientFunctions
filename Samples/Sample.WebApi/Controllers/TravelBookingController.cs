using Cleipnir.ResilientFunctions.AspNetCore;
using Microsoft.AspNetCore.Mvc;
using Sample.WebApi.Model;
using Sample.WebApi.Saga;

namespace Sample.WebApi.Controllers;

[ApiController]
[Route("[controller]")]
public class TravelBookingController : ControllerBase, IRegisterRFuncOnInstantiation
{
    private readonly BookingSaga _bookingSaga;

    public TravelBookingController(BookingSaga bookingSaga) => _bookingSaga = bookingSaga;

    [HttpPost]
    public async Task<Booking> Post(Order order)
    {
        var orderAndRequestIds = new OrderAndRequestIds(
            order,
            FlightRequestId: Guid.NewGuid(),
            HotelRequestId: Guid.NewGuid()
        );
        var booking = await _bookingSaga.BookTravel.Invoke(order.Id.ToString(), orderAndRequestIds);
        return booking;
    }
}