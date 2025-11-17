using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Domain.Events;
using Cleipnir.ResilientFunctions.Domain.Exceptions;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.CoreRuntime;

public interface IRegisteredTimeouts : IDisposable
{
    Task<Tuple<TimeoutStatus, DateTime>> RegisterTimeout(EffectId timeoutId, DateTime expiresAt, bool publishMessage);
    Task<Tuple<TimeoutStatus, DateTime>> RegisterTimeout(EffectId timeoutId, TimeSpan expiresIn, bool publishMessage);
    Task CancelTimeout(EffectId timeoutId);
    Task CompleteTimeout(EffectId timeoutId);
}

public enum TimeoutStatus
{
    Created = 0,
    Registered = 1,
    Cancelled = 2,
    Completed = 3
}

public class FlowRegisteredTimeouts(Effect effect, UtcNow utcNow, FlowMinimumTimeout flowMinimumTimeout, PublishTimeoutEvent publishTimeoutEvent, UnhandledExceptionHandler unhandledExceptionHandler, FlowId flowId) : IRegisteredTimeouts
{
    private volatile bool _disposed;
    public string GetNextImplicitId() => EffectContext.CurrentContext.NextImplicitId();
    
    
    public async Task<Tuple<TimeoutStatus, DateTime>> RegisterTimeout(EffectId timeoutId, DateTime expiresAt, bool publishMessage)
    {
        expiresAt = expiresAt.ToUniversalTime();
        
        if (!await effect.Contains(timeoutId))
        {
            var value = $"{(int)TimeoutStatus.Registered}_{expiresAt.Ticks}";
            await effect.Upsert(timeoutId, value, flush: false); 
            
            flowMinimumTimeout.AddTimeout(timeoutId, expiresAt);
            if (publishMessage)
                RegisterTimeoutEventPublish(timeoutId, expiresAt);
            
            return Tuple.Create(TimeoutStatus.Registered, expiresAt);
        }
        else
        {
            var value = await effect.Get<string>(timeoutId);
            var values = value.Split("_");
            var status = values[0].ToInt().ToEnum<TimeoutStatus>();
            var expiry = values[1].ToLong().ToUtc();

            if (status == TimeoutStatus.Registered)
            {
                flowMinimumTimeout.AddTimeout(timeoutId, expiry);
                if (publishMessage)
                    RegisterTimeoutEventPublish(timeoutId, expiresAt);                
            }

            return Tuple.Create(status, expiry);
        }
    }
    
    public Task<Tuple<TimeoutStatus, DateTime>> RegisterTimeout(string timeoutId, TimeSpan expiresIn, bool publishMessage)
        => RegisterTimeout(EffectId.CreateWithCurrentContext(timeoutId, EffectType.Timeout), expiresAt: utcNow().Add(expiresIn), publishMessage);
    public Task<Tuple<TimeoutStatus, DateTime>> RegisterTimeout(string timeoutId, DateTime expiresAt, bool publishMessage)
        => RegisterTimeout(EffectId.CreateWithCurrentContext(timeoutId, EffectType.Timeout), expiresAt, publishMessage);
    public Task<Tuple<TimeoutStatus, DateTime>> RegisterTimeout(EffectId timeoutId, TimeSpan expiresIn, bool publishMessage)
        => RegisterTimeout(timeoutId, expiresAt: utcNow().Add(expiresIn), publishMessage);
    
    public async Task CancelTimeout(EffectId timeoutId)
    {
        if (!await effect.Contains(timeoutId))
            return;
        
        var values = (await effect.Get<string>(timeoutId)).Split("_");
        var timeoutStatus = values[0].ToInt().ToEnum<TimeoutStatus>();
        var expiresAt = values[1];
        
        if (timeoutStatus == TimeoutStatus.Cancelled)
            return;

        var value = $"{(int)TimeoutStatus.Cancelled}_{expiresAt}";
        await effect.Upsert(timeoutId, value, flush: false);
        flowMinimumTimeout.RemoveTimeout(timeoutId);
    }
    
    public async Task CompleteTimeout(EffectId timeoutId)
    {
        if (!await effect.Contains(timeoutId))
            return;
        
        var values = (await effect.Get<string>(timeoutId)).Split("_");
        var timeoutStatus = values[0].ToInt().ToEnum<TimeoutStatus>();
        var expiresAt = values[1];
        
        if (timeoutStatus == TimeoutStatus.Cancelled)
            return;

        var value = $"{(int)TimeoutStatus.Completed}_{expiresAt}";
        await effect.Upsert(timeoutId, value, flush: false);
        flowMinimumTimeout.RemoveTimeout(timeoutId);
    }

    public void Dispose() => _disposed = true;

    private void RegisterTimeoutEventPublish(EffectId timeoutId, DateTime expiresAt)
    {
        _ = Task.Run(async () =>
            {
                var delay = (expiresAt - utcNow()).RoundUpToZero();
                while (!_disposed)
                {
                    while (delay > TimeSpan.Zero)
                    {
                        await Task.Delay(delay);
                        delay = (expiresAt - utcNow()).RoundUpToZero();
                    }

                    if (_disposed)
                        return;
                    
                    try
                    {
                        await publishTimeoutEvent(new TimeoutEvent(timeoutId, expiresAt));
                        return;
                    }
                    catch (Exception exception)
                    {
                        var frameworkException = new FrameworkException($"TimeoutEvent publish failed for flow: '{flowId}'", exception, flowId);
                        unhandledExceptionHandler.Invoke(frameworkException);
                        await Task.Delay(5_000);
                    }
                }
            });
    }
}