﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Messaging;
using Cleipnir.ResilientFunctions.Utils.Arbitrator;
using Cleipnir.ResilientFunctions.Utils.Monitor;
using Cleipnir.ResilientFunctions.Utils.Register;

namespace Cleipnir.ResilientFunctions.Storage;

public class InMemoryFunctionStore : IFunctionStore, IEventStore
{
    private readonly Dictionary<FunctionId, State> _states = new();
    private readonly Dictionary<FunctionId, List<StoredEvent>> _events = new();
    private readonly object _sync = new();

    public IEventStore EventStore => this;
    public ITimeoutStore TimeoutStore { get; } = new InMemoryTimeoutStore();

    public Utilities Utilities { get; } = new(new InMemoryMonitor(), new InMemoryRegister(), new InMemoryArbitrator());
    
    public Task Initialize() => Task.CompletedTask;

    #region FunctionStore

    public Task<bool> CreateFunction(
        FunctionId functionId,
        StoredParameter param,
        StoredScrapbook storedScrapbook,
        long crashedCheckFrequency)
    {
        lock (_sync)
        {
            if (_states.ContainsKey(functionId))
                return false.ToTask();

            _states[functionId] = new State
            {
                FunctionId = functionId,
                Param = param,
                Scrapbook = storedScrapbook,
                Status = Status.Executing,
                Epoch = 0,
                SignOfLife = 0,
                Exception = null,
                Result = new StoredResult(ResultJson: null, ResultType: null),
                PostponeUntil = null,
                CrashedCheckFrequency = crashedCheckFrequency
            };

            _events[functionId] = new List<StoredEvent>();

            return true.ToTask();
        }
    }

    public Task<bool> IncrementAlreadyPostponedFunctionEpoch(FunctionId functionId, int expectedEpoch)
    {
        lock (_sync)
        {
            if (!_states.ContainsKey(functionId)) return false.ToTask();
            var state = _states[functionId];
            if (state.Epoch != expectedEpoch)
                return false.ToTask();

            state.Epoch += 1;
            return true.ToTask();
        }
    }

    public Task<bool> RestartExecution(
        FunctionId functionId,
        int expectedEpoch,
        long crashedCheckFrequency)
    {
        lock (_sync)
        {
            if (!_states.ContainsKey(functionId))
                return false.ToTask();

            var state = _states[functionId];
            if (state.Epoch != expectedEpoch)
                return false.ToTask();

            state.Epoch += 1;
            state.Status = Status.Executing;
            state.CrashedCheckFrequency = crashedCheckFrequency;
            return true.ToTask();
        }
    }

    public Task<bool> UpdateSignOfLife(FunctionId functionId, int expectedEpoch, int newSignOfLife, ComplimentaryState.UpdateSignOfLife _)
    {
        lock (_sync)
        {
            if (!_states.ContainsKey(functionId))
                return false.ToTask();

            var state = _states[functionId];
            if (state.Epoch != expectedEpoch)
                return false.ToTask();

            state.SignOfLife = newSignOfLife;
            return true.ToTask();
        }
    }

    public Task<IEnumerable<StoredExecutingFunction>> GetExecutingFunctions(FunctionTypeId functionTypeId)
    {
        lock (_sync)
            return _states
                .Values
                .Where(s => s.FunctionId.TypeId == functionTypeId)
                .Where(s => s.Status == Status.Executing)
                .Select(s => new StoredExecutingFunction(s.FunctionId.InstanceId, s.Epoch, s.SignOfLife, s.CrashedCheckFrequency))
                .ToList()
                .AsEnumerable()
                .ToTask();
    }

    public Task<IEnumerable<StoredPostponedFunction>> GetPostponedFunctions(FunctionTypeId functionTypeId, long expiresBefore)
    {
        lock (_sync)
            return _states
                .Values
                .Where(s => s.FunctionId.TypeId == functionTypeId)
                .Where(s => s.Status == Status.Postponed)
                .Where(s => s.PostponeUntil <= expiresBefore)
                .Select(s =>
                    new StoredPostponedFunction(
                        s.FunctionId.InstanceId,
                        s.Epoch,
                        s.PostponeUntil!.Value
                    )
                )
                .ToList()
                .AsEnumerable()
                .ToTask();
    }

    public Task<IEnumerable<StoredEligibleSuspendedFunction>> GetEligibleSuspendedFunctions(FunctionTypeId functionTypeId)
    {
        lock (_sync)
            return _states
                .Values
                .Where(s => s.FunctionId.TypeId == functionTypeId)
                .Where(s => s.Status == Status.Suspended)
                .Where(s =>
                {
                    var events = EventStore.GetEvents(s.FunctionId).Result;
                    return s.SuspendUntilEventSourceCountAtLeast <= events.Count();
                })
                .Select(s => new StoredEligibleSuspendedFunction(s.FunctionId.InstanceId, s.Epoch))
                .ToList()
                .AsEnumerable()
                .ToTask();
    }

    public Task<bool> SetFunctionState(
        FunctionId functionId,
        Status status,
        StoredParameter storedParameter,
        StoredScrapbook storedScrapbook,
        StoredResult storedResult,
        StoredException? storedException,
        long? postponeUntil,
        ReplaceEvents? events,
        int expectedEpoch)
    {
        lock (_sync)
        {
            if (!_states.ContainsKey(functionId))
                return false.ToTask();

            var state = _states[functionId];
            if (state.Epoch != expectedEpoch)
                return false.ToTask();
            if (events != null)
            {
                var expectedCount = events.ExistingCount;
                if (expectedCount == 0 && !_events.ContainsKey(functionId) && _events[functionId].Count > 0)
                    return false.ToTask();
                if (_events[functionId].Count != expectedCount)
                    return false.ToTask();

                _events[functionId] = events.Events.ToList();
            }

            state.Status = status;
            state.Param = storedParameter;
            state.Scrapbook = storedScrapbook;
            state.Result = storedResult;
            state.Exception = storedException;
            state.PostponeUntil = postponeUntil;

            state.Epoch += 1;

            return true.ToTask();
        }
    }

    public Task<bool> SaveScrapbookForExecutingFunction( 
        FunctionId functionId,
        string scrapbookJson,
        int expectedEpoch,
        ComplimentaryState.SaveScrapbookForExecutingFunction _)
    {
        lock (_sync)
        {
            if (!_states.ContainsKey(functionId)) return false.ToTask();
            var state = _states[functionId];
            if (state.Epoch != expectedEpoch) return false.ToTask();

            state.Scrapbook = state.Scrapbook with { ScrapbookJson = scrapbookJson };
            return true.ToTask();
        }
    }

    public Task<bool> SetParameters(FunctionId functionId, StoredParameter storedParameter, StoredScrapbook storedScrapbook, ReplaceEvents? events, int expectedEpoch)
    {
        lock (_sync)
        {
            if (!_states.ContainsKey(functionId)) return false.ToTask();
            var state = _states[functionId];
            if (state.Epoch != expectedEpoch) return false.ToTask();

            if (events != null)
            {
                var expectedCount = events.ExistingCount;
                if (expectedCount == 0 && !_events.ContainsKey(functionId) && _events[functionId].Count > 0)
                    return false.ToTask();
                if (_events[functionId].Count != expectedCount)
                    return false.ToTask();

                _events[functionId] = events.Events.ToList();
            }
            
            state.Param = storedParameter;
            state.Scrapbook = storedScrapbook;

            state.Epoch += 1;

            return true.ToTask();
        }
    }

    public Task<bool> SucceedFunction(FunctionId functionId, StoredResult result, string scrapbookJson, int expectedEpoch, ComplimentaryState.SetResult _)
    {
        lock (_sync)
        {
            if (!_states.ContainsKey(functionId)) return false.ToTask();

            var state = _states[functionId];
            if (state.Epoch != expectedEpoch) return false.ToTask();

            state.Status = Status.Succeeded;
            state.Result = result;
            state.Scrapbook = state.Scrapbook with { ScrapbookJson = scrapbookJson };

            return true.ToTask();
        }
    }

    public Task<bool> PostponeFunction(FunctionId functionId, long postponeUntil, string scrapbookJson, int expectedEpoch, ComplimentaryState.SetResult _)
    {
        lock (_sync)
        {
            if (!_states.ContainsKey(functionId)) return false.ToTask();

            var state = _states[functionId];
            if (state.Epoch != expectedEpoch) return false.ToTask();

            state.Status = Status.Postponed;
            state.PostponeUntil = postponeUntil;
            state.Scrapbook = state.Scrapbook with { ScrapbookJson = scrapbookJson };

            return true.ToTask();
        }
    }
    
    public Task<SuspensionResult> SuspendFunction(FunctionId functionId, int expectedEventCount, string scrapbookJson, int expectedEpoch, ComplimentaryState.SetResult _)
    {
        lock (_sync)
        {
            if (!_states.ContainsKey(functionId)) 
                return SuspensionResult.ConcurrentStateModification.ToTask();

            var state = _states[functionId];
            if (state.Epoch != expectedEpoch) 
                return SuspensionResult.ConcurrentStateModification.ToTask();

            if (_events[functionId].Count > expectedEventCount)
                return SuspensionResult.EventCountMismatch.ToTask();
                
            state.Status = Status.Suspended;
            state.SuspendUntilEventSourceCountAtLeast = expectedEventCount;
            state.Scrapbook = state.Scrapbook with { ScrapbookJson = scrapbookJson };
            
            return SuspensionResult.Success.ToTask();
        }
    }

    public Task<bool> FailFunction(FunctionId functionId, StoredException storedException, string scrapbookJson, int expectedEpoch, ComplimentaryState.SetResult _)
    {
        lock (_sync)
        {
            if (!_states.ContainsKey(functionId)) return false.ToTask();

            var state = _states[functionId];
            if (state.Epoch != expectedEpoch) return false.ToTask();

            state.Status = Status.Failed;
            state.Exception = storedException;
            state.Scrapbook = state.Scrapbook with { ScrapbookJson = scrapbookJson };

            return true.ToTask();
        }
    }

    public Task<StoredFunction?> GetFunction(FunctionId functionId)
    {
        lock (_sync)
        {
            if (!_states.ContainsKey(functionId))
                return default(StoredFunction).ToTask();

            var state = _states[functionId];

            return new StoredFunction(
                    functionId,
                    state.Param,
                    state.Scrapbook,
                    state.Status,
                    state.Result,
                    state.Exception,
                    state.PostponeUntil,
                    state.SuspendUntilEventSourceCountAtLeast,
                    state.Epoch,
                    state.SignOfLife,
                    state.CrashedCheckFrequency
                )
                .ToNullable()
                .ToTask();
        }
    }
    
    public Task<bool> DeleteFunction(FunctionId functionId, int? expectedEpoch = null)
    {
        lock (_sync)
        {
            if (!_states.ContainsKey(functionId))
                return false.ToTask();
            
            if (expectedEpoch == null)
            {
                _states.Remove(functionId);
                _events.Remove(functionId);
                return true.ToTask();
            }
            
            var state = _states[functionId];
            if (state.Epoch == expectedEpoch.Value)
            {
                _states.Remove(functionId);
                _events.Remove(functionId);
                return true.ToTask();
            }
            
            return false.ToTask();
        }
    }

    private class State
    {
        public FunctionId FunctionId { get; init; } = null!;
        public StoredParameter Param { get; set; } = null!;
        public StoredScrapbook Scrapbook { get; set; } = null!;
        public Status Status { get; set; }
        public StoredResult Result { get; set; } = new StoredResult(ResultJson: null, ResultType: null);
        public StoredException? Exception { get; set; }
        public long? PostponeUntil { get; set; }
        public int Epoch { get; set; }
        public int SignOfLife { get; set; }
        public long CrashedCheckFrequency { get; set; }
        public int SuspendUntilEventSourceCountAtLeast { get; set; }
    }
    #endregion
    #region EventStore

    public Task<SuspensionStatus> AppendEvent(FunctionId functionId, StoredEvent storedEvent)
        => AppendEvents(functionId, new[] { storedEvent });

    public Task<SuspensionStatus> AppendEvent(FunctionId functionId, string eventJson, string eventType, string? idempotencyKey = null)
        => AppendEvent(functionId, new StoredEvent(eventJson, eventType, idempotencyKey));

    public Task<SuspensionStatus> AppendEvents(FunctionId functionId, IEnumerable<StoredEvent> storedEvents)
    {
        lock (_sync)
        {
            if (!_events.ContainsKey(functionId))
                _events[functionId] = new List<StoredEvent>();

            var events = _events[functionId];
            foreach (var storedEvent in storedEvents)
                if (storedEvent.IdempotencyKey == null ||
                    events.All(e => e.IdempotencyKey != storedEvent.IdempotencyKey))
                    events.Add(storedEvent);

            return _states[functionId].Status == Status.Suspended
                ? Task.FromResult(new SuspensionStatus(Suspended: true, Epoch: _states[functionId].Epoch))
                : Task.FromResult(new SuspensionStatus(Suspended: false, Epoch: null));
        }
    }

    public Task Truncate(FunctionId functionId)
    {
        lock (_sync)
            _events[functionId] = new List<StoredEvent>();

        return Task.CompletedTask;
    }

    public Task<IEnumerable<StoredEvent>> GetEvents(FunctionId functionId)
    {
        lock (_sync)
        {
            if (!_events.ContainsKey(functionId))
                return Enumerable.Empty<StoredEvent>().ToTask();

            return _events[functionId].ToList().AsEnumerable().ToTask();
        }
    }

    public Task<EventsSubscription> SubscribeToEvents(FunctionId functionId)
    {
        var disposed = false;
        var skip = 0;

        var subscription = new EventsSubscription(
            pullEvents: () =>
            {
                List<StoredEvent>? events;

                lock (_sync)
                    if (disposed)
                        return Task.FromResult((IReadOnlyList<StoredEvent>) Array.Empty<StoredEvent>());
                    else if (_events.ContainsKey(functionId) && _events[functionId].Count > skip)
                    {
                        events = _events[functionId].Skip(skip).ToList();
                        skip += events.Count;
                    }
                    else
                        events = new List<StoredEvent>();

                return Task.FromResult((IReadOnlyList<StoredEvent>) events);
            },
            dispose: () =>
            {
                lock (_sync)
                    disposed = true;
                
                return ValueTask.CompletedTask;
            }
        );

        return Task.FromResult(subscription);
    }

    #endregion
}