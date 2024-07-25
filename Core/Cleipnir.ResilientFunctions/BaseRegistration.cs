using Cleipnir.ResilientFunctions.Messaging;

namespace Cleipnir.ResilientFunctions;

public abstract class BaseRegistration(Postman postman)
{
    public Postman Postman { get; } = postman;
}