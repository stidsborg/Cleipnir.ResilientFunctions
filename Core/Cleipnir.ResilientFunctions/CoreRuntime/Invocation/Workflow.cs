using System;
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

    public async Task RegisterCorrelation(string correlation) => await Correlations.Register(correlation);

    public Task Delay(TimeSpan @for) => Delay(until: DateTime.UtcNow + @for);
    public Task Delay(DateTime until)
    {
        if (until <= DateTime.UtcNow)
            return Task.CompletedTask;

        throw new PostponeInvocationException(until);
    }
}