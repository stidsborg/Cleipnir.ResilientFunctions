# TODO

Outstanding work items, roughly prioritized.

## Correctness / robustness

1. **Shield serialization via decoration — serializers must not throw.**
   `QueueManager.ProcessMessages` assumes `ISerializer.Serialize`/`SerializeType` never throw (its staging
   try/catch has been removed on that assumption). Extend the `ErrorHandlingDecorator` pattern — which already
   normalizes deserialization failures into `DeserializationException` — to the serialize side, so an asymmetric
   user serializer (deserializes fine, fails to re-serialize) cannot fault the delivery pipeline.
   *GitHub issue to be created.*

2. **Bound unbounded fetches codebase-wide.**
   Store fetches (messages, dlq, effects) are unbounded; add paging/limits. Noted during the dead-letter-queue
   work (PR #217).

3. **Ignore-set entries never expire.**
   A position marked pushed but never cleared or reopened — an incarnation ending abnormally without a final
   flush, or never-matched staged rows of an ended flow — stays fetch-ignored until process restart. Consider a
   TTL on `MessageClearer`'s pushed-positions set so stranding self-heals (re-pushes are idempotent by design).

## Hardening (convention → structure)

4. **Delivery batch atomicity is three adjacent calls.**
   The delivered-message capture, child-effect prune and delivered-position marking must land in one
   snapshot-atomic pending batch; today that is one `FlushlessUpserts` plus `PruneDeliveredMessage`'s separate
   writes under the same lock (`QueueManager.DeliverMessages`). Restructure into a single multi-entry upsert so
   the invariant is syntactic rather than positional.

5. **`BeforeFlush`/`AfterFlush` must stay under the flush lock.**
   The single-field watermark (`QueueManager._positionsCoveredByFlush`) is sound only because flush cycles never
   overlap — both hooks run inside `EffectResults.Flush`'s `_flushSync` section. Documented at the call sites;
   consider a structural guard.

6. **Reserved effect-id ownership is convention only.**
   Only `QueueManager` may write `DeliveredPositionsId` / `StagedMessagesRoot` / `IdempotencyKeysRoot` children
   (control-panel `ExistingMessages` excepted). Nothing enforces it.

7. **Store failures should be handled inside the store, not by compensating catch blocks at the call sites.**
   Because store calls throw on any transient failure, callers are pushed towards `try/catch` blocks that undo
   bookkeeping and rethrow — a shape we do not want, since correctness then depends on catch blocks performing
   state fixups:

   ```csharp
   try { results = await _functionStore.RestartExecutions(...); }
   catch { _messageClearer.ReopenPositions(...); throw; }
   ```

   Those blocks have been removed from `FlowsManager.RestartExecutions` — both the claim and the follow-up
   `GetFunction` status lookup now stand on their own — at the cost of leaving the in-hand positions marked
   pushed when a store call throws, so they are not re-fetched until the process restarts (see item 3). The one
   remaining catch in the file is the same problem in other clothes: `TryDeadLetterMessages`, whose whole `bool`
   return exists to convert a store throw into "caller, reopen the positions".

   The fix is to give the store operations a total contract: handle (and retry/report) failures inside the store
   implementation and express the outcome as data — e.g. `RestartExecutions` returning which ids were claimed,
   which are owned elsewhere and which could not be reached — so the delivery path reads linearly, stranding is
   impossible, and an escaping exception means a framework bug rather than an expected condition. Same treatment
   for the other store-touching paths in the watchdogs.

## Tests

8. **Re-enable the `[Ignore]`d control-panel message tests** in the SqlServer and MariaDB suites
   (`ControlPanelsExistingMessagesContainsPreviouslyAddedMessages` and related) once their flakiness is
   understood and fixed.

## Existing inline TODOs

9. `Invoker.PrepareForReInvocation` — `param! //todo implement param null case`
10. `FlowsManagers.cs:74` — `// todo log a warning here`
11. `InvocationHelper.cs:352` — `//todo should flush be true`
12. `StoredMessage.DefaultDeserialize` — `//todo remove`
