using System;
using System.IO.Pipelines;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime;
using Cleipnir.ResilientFunctions.Domain.Exceptions.Commands;
using Cleipnir.ResilientFunctions.Helpers;

namespace Cleipnir.ResilientFunctions.Domain;

/// <summary>
/// Retry for behavior for a failed effect
/// </summary>
/// <param name="initialInterval">Initial delay before retry for first failure.</param>
/// <param name="backoffCoefficient">Interval increase coefficient per retry.</param>
/// <param name="maximumInterval">Maximum interval between retries.</param>
/// <param name="maximumAttempts">Maximum number of retry attempts before failing effect.</param>
/// <param name="shouldRetry">Function deciding if retry should be allowed for the thrown exception.</param>
/// <param name="suspendThreshold">Suspend flow invocation when delay before the next retry exceeds this value.</param>
public class RetryPolicy(TimeSpan initialInterval, double backoffCoefficient, TimeSpan maximumInterval, int maximumAttempts, Func<Exception, bool> shouldRetry, TimeSpan suspendThreshold)
{
    public static RetryPolicy Default { get; }
        = new(
            initialInterval: TimeSpan.FromSeconds(1),
            backoffCoefficient: 2,
            maximumInterval: TimeSpan.MaxValue,
            maximumAttempts: 15,
            shouldRetry: _ => true,
            suspendThreshold: TimeSpan.FromSeconds(5)
        );
    
    /// <summary>
    /// Retry for behavior for a failed effect
    /// </summary>
    /// <param name="initialInterval">Initial delay before retry for first failure. Default: 1 second.</param>
    /// <param name="backoffCoefficient">Interval increase coefficient per retry. Default: 2</param>
    /// <param name="maximumInterval">Maximum interval between retries. Default unlimited.</param>
    /// <param name="maximumAttempts">Maximum number of retry attempts before failing effect. Default: 15 times.</param>
    /// <param name="shouldRetry">Function deciding if retry should be allowed for the thrown exception. Default: Retry is allowed for all exceptions.</param>
    /// <param name="suspendThreshold">Suspend flow invocation when delay before the next retry exceeds this value. Default: 5 seconds.</param>
    public static RetryPolicy CreateLinearBackoff(
        TimeSpan? initialInterval = null, TimeSpan? maximumInterval = null,
        int maximumAttempts = 15, Func<Exception, bool>? shouldRetry = null, TimeSpan? suspendThreshold = null
    ) => new(
        initialInterval ?? TimeSpan.FromSeconds(1),
        backoffCoefficient: 1,
        maximumInterval ?? TimeSpan.MaxValue,
        maximumAttempts,
        shouldRetry ?? (_ => true),
        suspendThreshold ?? TimeSpan.FromSeconds(5)
    );
    
    /// <summary>
    /// Retry for behavior for a failed effect
    /// </summary>
    /// <param name="interval">Delay before retry after failure. Default: 1 second.</param>
    /// <param name="maximumAttempts">Maximum number of retry attempts before failing effect. Default: 15 times.</param>
    /// <param name="shouldRetry">Function deciding if retry should be allowed for the thrown exception. Default: Retry is allowed for all exceptions.</param>
    /// <param name="suspendThreshold">Suspend flow invocation when delay before the next retry exceeds this value. Default: 5 seconds.</param>
    public static RetryPolicy CreateConstantDelay(
        TimeSpan? interval = null, int maximumAttempts = 15, Func<Exception, bool>? shouldRetry = null, TimeSpan? suspendThreshold = null
    ) => new(
        interval ?? TimeSpan.FromSeconds(1),
        backoffCoefficient: 1,
        maximumInterval: TimeSpan.MaxValue, 
        maximumAttempts,
        shouldRetry ?? (_ => true),
        suspendThreshold ?? TimeSpan.FromSeconds(5)
    );

    /// <summary>
    /// Retry for behavior for a failed effect
    /// </summary>
    /// <param name="initialInterval">Initial delay before retry for first failure. Default: 1 second.</param>
    /// <param name="backoffCoefficient">Interval increase coefficient per retry. Default: 2</param>
    /// <param name="maximumInterval">Maximum interval between retries. Default unlimited.</param>
    /// <param name="maximumAttempts">Maximum number of retry attempts before failing effect. Default: 15 times.</param>
    /// <param name="shouldRetry">Function deciding if retry should be allowed for the thrown exception. Default: Retry is allowed for all exceptions.</param>
    /// <param name="suspendThreshold">Suspend flow invocation when delay before the next retry exceeds this value. Default: 5 seconds.</param>
    public static RetryPolicy Create(
        TimeSpan? initialInterval = null, double backoffCoefficient = 2, TimeSpan? maximumInterval = null,
        int maximumAttempts = 15, Func<Exception, bool>? shouldRetry = null, TimeSpan? suspendThreshold = null
    ) => new(
        initialInterval ?? TimeSpan.FromSeconds(1),
        backoffCoefficient,
        maximumInterval ?? TimeSpan.MaxValue,
        maximumAttempts,
        shouldRetry ?? (_ => true),
        suspendThreshold ?? TimeSpan.FromSeconds(5)
    );
    
    
    public Task Invoke(Func<Task> work, Effect effect, UtcNow utcNow, FlowMinimumTimeout flowMinimumTimeout)
    {
        return Invoke(
            work: async () =>
            {
                await work();
                return Unit.Instance;
            },
            effect,
            utcNow,
            flowMinimumTimeout
        );
    }

    public async Task<T> Invoke<T>(Func<Task<T>> work, Effect effect, UtcNow utcNow, FlowMinimumTimeout flowMinimumTimeout)
    {
        var delayUntilId = effect.CreateEffectId(0);
        var delayUntilOption = await effect.TryGet<long>(delayUntilId);
        var delayUntil = delayUntilOption.HasValue ? delayUntilOption.Value.ToDateTime() : DateTime.MinValue;
        if (delayUntilOption.HasValue && delayUntil > utcNow())
        {
            flowMinimumTimeout.AddTimeout(delayUntilId, delayUntil);
            throw new SuspendInvocationException();
        }

        var iterationId = effect.CreateEffectId(1);
        var iteration = await effect.CreateOrGet(iterationId, 0, alias: null, flush: false);
        if (iteration >= maximumAttempts)
            throw new InvalidOperationException($"Retry attempts exceeded maximum attempts value '{maximumAttempts}'");
        
        while (true)
        {
            try
            {
                return await work();
            }
            catch (Exception e)
            {
                if (!shouldRetry(e))
                    throw;

                var delay = initialInterval * Math.Pow(backoffCoefficient, iteration);
                if (delay > maximumInterval)
                    delay = maximumInterval;
                
                delayUntil = utcNow().Add(delay);
                iteration += 1;
                {
                    await effect.Upserts(
                        [
                            new EffectResult(delayUntilId, delayUntil.Ticks, null),
                            new EffectResult(iterationId, iteration, null)
                        ],
                        flush: false
                    );
                }

                if (iteration >= maximumAttempts)
                    throw;

                if (delay >= suspendThreshold)
                {
                    flowMinimumTimeout.AddTimeout(delayUntilId, delayUntil);
                    throw new SuspendInvocationException();
                }
                
                await Task.Delay(delay);
            }            
        }
    }
}