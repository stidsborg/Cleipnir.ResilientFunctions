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
    public States States { get; }
    public Utilities Utilities { get; }
    public Correlations Correlations { get; }
    [Obsolete("Use Synchronization property instead")]
    public DistributedSemaphores Semaphores { get; }
    public Synchronization Synchronization { get; }
    private readonly UtcNow _utcNow;
    
    
    public Workflow(FlowId flowId, StoredId storedId, Messages messages, Effect effect, States states, Utilities utilities, Correlations correlations, DistributedSemaphores semaphores, UtcNow utcNow)
    {
        FlowId = flowId;
        StoredId = storedId;
        Utilities = utilities;
        Messages = messages;
        Effect = effect;
        States = states;
        Correlations = correlations;
        #pragma warning disable //todo remove in the future
        Semaphores = semaphores;
        #pragma warning restore
        Synchronization = new Synchronization(semaphores);
        _utcNow = utcNow;
    }

    public void Deconstruct(out Effect effect, out Messages messages, out States states)
    {
        effect = Effect;
        messages = Messages;
        states = States;
    }

    public async Task RegisterCorrelation(string correlation) => await Correlations.Register(correlation);

    public Task Delay(TimeSpan @for, bool suspend = true, string? effectId = null) => Delay(until: _utcNow() + @for, suspend, effectId);
    public async Task Delay(DateTime until, bool suspend = true, string? effectId = null)
    {
        effectId ??= $"Delay#{Effect.TakeNextImplicitId()}";
        var systemEffectId = EffectId.CreateWithCurrentContext(effectId, EffectType.System);
        until = await Effect.CreateOrGet(systemEffectId, until);
        var delay = (until - _utcNow()).RoundUpToZero();

        if (delay == TimeSpan.Zero)
            return;
        
        if (!suspend)
            await Task.Delay(delay);
        else 
            throw new PostponeInvocationException(until);            
    }
}