using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Messaging;

namespace Cleipnir.ResilientFunctions;

public abstract class BaseRegistration
{
    protected Postman Postman { get; } 
    protected BaseRegistration(Postman postman) => Postman = postman;

    public Task RouteMessage<T>(T message, string correlationId, string? idempotencyKey = null) where T : notnull 
        => Postman.RouteMessage(message, correlationId, idempotencyKey);
}