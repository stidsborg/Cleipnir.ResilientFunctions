using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain.Exceptions;
using Cleipnir.ResilientFunctions.Messaging;

namespace Cleipnir.ResilientFunctions.CoreRuntime.Watchdogs;

/// <summary>
/// Tracks the positions this replica has pushed to its flows but not yet cleared from the store, and clears them
/// (deleting the messages) once a QueueManager reports them handled. The MessageWatchdog uses the ignore-set to
/// avoid re-fetching pushed messages; QueueManagers call <see cref="Clear"/> to delete handled ones.
/// </summary>
internal sealed class MessageClearer(
    IMessageStore messageStore,
    UnhandledExceptionHandler unhandledExceptionHandler,
    TimeSpan retryDelay)
    : IMessageClearer
{
    // Positions pushed to this replica's flows but not yet cleared from the store. Handed to the MessageWatchdog
    // as the ignore-set so they are not re-fetched; trimmed by Clear once their messages are deleted.
    private readonly HashSet<long> _pushedPositions = new();
    private readonly Lock _pushedPositionsLock = new();

    // Coalescing delete pipeline (see Clear): the first caller starts the drain, callers that arrive while a
    // delete is in flight batch up and are flushed together. Guarded by _deleteLock; the drain trims
    // _pushedPositions under _pushedPositionsLock after each batch's delete lands.
    private readonly Lock _deleteLock = new();
    private List<long> _pendingDeletes = new();
    private TaskCompletionSource _pendingDeletesTcs = new();
    private bool _draining;

    /// <summary>Records positions just pushed to flows so they are excluded from the next fetch.</summary>
    public void MarkPushed(IEnumerable<long> positions)
    {
        lock (_pushedPositionsLock)
            foreach (var position in positions)
                _pushedPositions.Add(position);
    }

    public void ReopenPositions(IEnumerable<long> positions)
    {
        lock (_pushedPositionsLock)
            foreach (var position in positions)
                _pushedPositions.Remove(position);
    }

    /// <summary>Snapshot of the not-yet-cleared positions, passed to the store as the fetch ignore-set.</summary>
    public IReadOnlyList<long> NonClearedPositions()
    {
        lock (_pushedPositionsLock)
            return _pushedPositions.ToList();
    }

    /// <summary>
    /// Deletes the given handled message positions from the store on a QueueManager's behalf, then drops them
    /// from the ignore-set instead of carrying them forever. The returned task completes only once that has
    /// happened for the caller's positions, so the caller knows it will not receive those messages again.
    ///
    /// Calls are coalesced: the first one starts the drain and runs immediately, while calls that arrive while a
    /// delete is in flight are batched and flushed together - collapsing a burst of small deletes into one query.
    /// Deleting before trimming avoids re-fetching a position that is no longer ignored but not yet gone from the
    /// store.
    /// </summary>
    public Task Clear(IReadOnlyList<long> positions)
    {
        if (positions.Count == 0)
            return Task.CompletedTask;

        Task completion;
        bool startDraining;
        lock (_deleteLock)
        {
            _pendingDeletes.AddRange(positions);
            completion = _pendingDeletesTcs.Task;
            startDraining = !_draining;
            if (startDraining)
                _draining = true;
        }

        if (startDraining)
            _ = Task.Run(DrainPendingDeletes);

        return completion;
    }

    // Drains the pending-delete queue one batch at a time until it is empty. Only the drain that set _draining
    // runs at a time; calls arriving mid-flight just enqueue and are picked up by a later batch. A failing batch
    // is retried until it lands (callers are told a message is gone only once it truly is), notifying the
    // unhandled-exception handler on each failure.
    private async Task DrainPendingDeletes()
    {
        while (true)
        {
            List<long> pendingDeletes;
            TaskCompletionSource completionTcs;
            lock (_deleteLock)
            {
                if (_pendingDeletes.Count == 0)
                {
                    _draining = false;
                    return;
                }

                pendingDeletes = _pendingDeletes;
                _pendingDeletes = new List<long>();
                completionTcs = _pendingDeletesTcs;
                _pendingDeletesTcs = new TaskCompletionSource();
            }

            // Retry until the delete lands - a failed delete must not fault the callers (they are told the
            // message is gone only once it truly is). Each failure notifies the unhandled-exception handler.
            while (true)
            {
                try
                {
                    await messageStore.DeleteMessages(pendingDeletes);
                    break;
                }
                catch (Exception exception)
                {
                    unhandledExceptionHandler.Invoke(
                        new FrameworkException(
                            $"{nameof(MessageClearer)} failed to delete handled messages - retrying",
                            innerException: exception
                        )
                    );
                    await Task.Delay(retryDelay);
                }
            }

            lock (_pushedPositionsLock)
                foreach (var position in pendingDeletes)
                    _pushedPositions.Remove(position);

            // Offload to the pool so awaiting callers' continuations do not run on the drain loop's thread.
            _ = Task.Run(completionTcs.SetResult);
        }
    }
}
