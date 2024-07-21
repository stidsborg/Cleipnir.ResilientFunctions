using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Messaging;

namespace Cleipnir.ResilientFunctions;

public abstract class BaseRegistration(Postman postman)
{
    public Task PostMessage<T>(T message) where T : class 
        => PostMessage(message, typeof(T));
    public Task PostMessage(object message, Type messageType)
        => postman.PostMessage(message, messageType);
}