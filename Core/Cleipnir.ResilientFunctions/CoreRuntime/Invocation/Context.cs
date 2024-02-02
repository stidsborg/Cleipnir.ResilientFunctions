using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Domain.Exceptions;
using Cleipnir.ResilientFunctions.Messaging;

namespace Cleipnir.ResilientFunctions.CoreRuntime.Invocation;

public class Context : IDisposable
{
    private readonly Func<FunctionId, MessageWriter> _messageWriterFunc;
    
    public FunctionId FunctionId { get; }
    public Messages Messages { get; }
    public Activities Activities { get; }
    public Utilities Utilities { get; }
    
    public Context(FunctionId functionId, Messages messages, Activities activities, Utilities utilities, Func<FunctionId, MessageWriter> messageWriterFunc)
    {
        _messageWriterFunc = messageWriterFunc;
        FunctionId = functionId;
        Utilities = utilities;
        Messages = messages;
        Activities = activities;
    }

    public void Deconstruct(out Activities activities, out Messages messages)
    {
        activities = Activities;
        messages = Messages;
    }
    
    public void Dispose() => Messages.Dispose();

    public async Task PublishMessage<T>(FunctionId receiver, T message, string? idempotencyKey) where T : notnull
    {
        var messageWriter = _messageWriterFunc(receiver);
        await messageWriter.AppendMessage(message, idempotencyKey);
    }

    public async Task Delay(string activityId, TimeSpan @for)
    {
        var expiry = await Activities.Do(activityId, () => DateTime.UtcNow + @for);
        if (expiry <= DateTime.UtcNow)
            return;

        throw new PostponeInvocationException(expiry);
    } 
}