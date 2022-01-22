using Sample.WebApi.Model;

namespace Sample.WebApi.Saga;

public record OrderAndRequestIds(Order Order, Guid FlightRequestId, Guid HotelRequestId);