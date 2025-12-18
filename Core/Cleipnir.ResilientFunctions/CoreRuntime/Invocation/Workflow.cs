using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Domain.Exceptions.Commands;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Messaging;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.CoreRuntime.Invocation;

public class Workflow
{
    public FlowId FlowId { get; }
    internal StoredId StoredId { get; }
    public Messages Messages { get; }
    public Effect Effect { get; }
    public Utilities Utilities { get; }
    public Correlations Correlations { get; }
    public Synchronization Synchronization { get; }
    private readonly UtcNow _utcNow;
    
    
    public Workflow(FlowId flowId, StoredId storedId, Messages messages, Effect effect, Utilities utilities, Correlations correlations, DistributedSemaphores semaphores, UtcNow utcNow)
    {
        FlowId = flowId;
        StoredId = storedId;
        Utilities = utilities;
        Messages = messages;
        Effect = effect;
        Correlations = correlations;
        Synchronization = new Synchronization(semaphores);
        _utcNow = utcNow;
    }

    public void Deconstruct(out Effect effect, out Messages messages)
    {
        effect = Effect;
        messages = Messages;
    }

    public async Task RegisterCorrelation(string correlation) => await Correlations.Register(correlation);

    public Task Delay(TimeSpan @for, bool suspend = true, string? alias = null) => Delay(until: _utcNow() + @for, suspend, alias);
    public async Task Delay(DateTime until, bool suspend = true, string? alias = null)
    {
        var effectId = Effect.TakeNextImplicitId();
        var timeoutId = EffectId.CreateWithCurrentContext(effectId);

        var (status, expiry) = await Messages.FlowRegisteredTimeouts.RegisterTimeout(
            timeoutId,
            until,
            publishMessage: false,
            alias
        );

        if (status is TimeoutStatus.Completed or TimeoutStatus.Cancelled)
        {
            return;
        }

        var delay =  (expiry - _utcNow()).RoundUpToZero();
        if (!suspend)
        {
            await Task.Delay(delay);
            delay = TimeSpan.Zero;
        }
        
        if (delay == TimeSpan.Zero)
            await Messages
                .FlowRegisteredTimeouts
                .CompleteTimeout(timeoutId, alias);
        else
            throw new SuspendInvocationException();                
    }

    public string ExecutionTree()
    {
        return $"{FlowId} ({StoredId}):" + Environment.NewLine + Effect.ExecutionTree();
    }
}