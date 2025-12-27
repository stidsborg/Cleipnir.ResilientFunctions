using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Domain.Exceptions.Commands;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Messaging;
using Cleipnir.ResilientFunctions.Queuing;
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
    private QueueManager _queueManager;
    private readonly UtcNow _utcNow;


    public Workflow(FlowId flowId, StoredId storedId, Messages messages, Effect effect, Utilities utilities, Correlations correlations, DistributedSemaphores semaphores, QueueManager queueManager, UtcNow utcNow)
    {
        FlowId = flowId;
        StoredId = storedId;
        Utilities = utilities;
        Messages = messages;
        Effect = effect;
        Correlations = correlations;
        Synchronization = new Synchronization(semaphores);
        _queueManager = queueManager;
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

    public Task<T> Message<T>()
    {
        var effectId = Effect.CreateNextImplicitId();
        return _queueManager.CreateQueueClient().Pull<T>(this, effectId, filter: null);
    }
    
    public Task<T?> Message<T>(DateTime waitUntil)
    {
        var effectId = Effect.CreateNextImplicitId();
        return _queueManager.CreateQueueClient().Pull<T>(this, effectId, waitUntil, filter: null);
    }

    public Task<T?> Message<T>(TimeSpan waitFor)
    {
        var effectId = Effect.CreateNextImplicitId();
        return _queueManager.CreateQueueClient().Pull<T>(this, effectId, waitFor, filter: null);
    }

    public Task<T> Message<T>(Func<T, bool> filter)
    {
        var effectId = Effect.CreateNextImplicitId();
        return _queueManager.CreateQueueClient().Pull<T>(this, effectId, filter);
    }
    
    public Task<T?> Message<T>(Func<T, bool> filter, DateTime waitUntil)
    {
        var effectId = Effect.CreateNextImplicitId();
        return _queueManager.CreateQueueClient().Pull<T>(this, effectId, waitUntil, filter);
    }
    
    public Task<T?> Message<T>(Func<T, bool> filter, TimeSpan waitFor)
    {
        var effectId = Effect.CreateNextImplicitId();
        return _queueManager.CreateQueueClient().Pull<T>(this, effectId, waitFor, filter);
    }

    public Task<T> Parallelle<T>(Func<Task<T>> work) => Effect.RunParallelle(work);

    public string ExecutionTree()
    {
        return $"{FlowId} ({StoredId}):" + Environment.NewLine + Effect.ExecutionTree();
    }
}