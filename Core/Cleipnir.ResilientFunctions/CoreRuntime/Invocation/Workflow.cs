using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Domain.Exceptions;
using Cleipnir.ResilientFunctions.Messaging;

namespace Cleipnir.ResilientFunctions.CoreRuntime.Invocation;

public class Workflow
{
    private readonly Func<FunctionId, MessageWriter> _messageWriterFunc;
    
    public FunctionId FunctionId { get; }
    public Messages Messages { get; }
    public Effect Effect { get; }
    public States States { get; }
    public Utilities Utilities { get; }
    
    public Workflow(FunctionId functionId, Messages messages, Effect effect, States states, Utilities utilities, Func<FunctionId, MessageWriter> messageWriterFunc)
    {
        _messageWriterFunc = messageWriterFunc;
        FunctionId = functionId;
        Utilities = utilities;
        Messages = messages;
        Effect = effect;
        States = states;
    }

    public void Deconstruct(out Effect effect, out Messages messages, out States states)
    {
        effect = Effect;
        messages = Messages;
        states = States;
    }

    public async Task PublishMessage<T>(FunctionId receiver, T message, string? idempotencyKey) where T : notnull
    {
        var messageWriter = _messageWriterFunc(receiver);
        await messageWriter.AppendMessage(message, idempotencyKey);
    }

    public Task Delay(string effectId, TimeSpan @for) => Delay(effectId, until: DateTime.UtcNow + @for);
    public async Task Delay(string effectId, DateTime until)
    {
        var expiry = await Effect.Capture(effectId, until.ToUniversalTime);
        if (expiry <= DateTime.UtcNow)
            return;

        throw new PostponeInvocationException(expiry);
    }
}