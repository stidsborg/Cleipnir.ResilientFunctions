﻿using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Domain.Exceptions.Commands;
using Cleipnir.ResilientFunctions.Messaging;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.CoreRuntime.Invocation;

public class Workflow
{
    public FlowId FlowId { get; }
    internal StoredId StoredId { get; }
    public Messages Messages { get; }
    public Effect Effect { get; }
    public States States { get; }
    public Utilities Utilities { get; }
    public Correlations Correlations { get; }
    public DistributedSemaphores Semaphores { get; }
    
    public Workflow(FlowId flowId, StoredId storedId, Messages messages, Effect effect, States states, Utilities utilities, Correlations correlations, DistributedSemaphores semaphores)
    {
        FlowId = flowId;
        StoredId = storedId;
        Utilities = utilities;
        Messages = messages;
        Effect = effect;
        States = states;
        Correlations = correlations;
        Semaphores = semaphores;
    }

    public void Deconstruct(out Effect effect, out Messages messages, out States states)
    {
        effect = Effect;
        messages = Messages;
        states = States;
    }

    public async Task RegisterCorrelation(string correlation)
    {
        await Correlations.Register(correlation);
    }

    public Task Delay(string effectId, TimeSpan @for) => Delay(effectId, until: DateTime.UtcNow + @for);
    public async Task Delay(string effectId, DateTime until)
    {
        var expiry = await Effect.Capture(effectId, until.ToUniversalTime);
        if (expiry <= DateTime.UtcNow)
            return;

        throw new PostponeInvocationException(expiry);
    }

    public void Postpone(TimeSpan @for) => throw new PostponeInvocationException(@for);
    public void Postpone(DateTime until) => throw new PostponeInvocationException(until);
}