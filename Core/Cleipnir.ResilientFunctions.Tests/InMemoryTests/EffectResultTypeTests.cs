using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime;
using Cleipnir.ResilientFunctions.CoreRuntime.Serialization;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Tests.Utils;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Shouldly;

namespace Cleipnir.ResilientFunctions.Tests.InMemoryTests;

[TestClass]
public class EffectResultTypeTests
{
    private static Effect CreateEffect(StoredId storedId, IFunctionStore functionStore, IReadOnlyList<StoredEffect>? existingEffects = null)
    {
        var effectResults = new EffectResults(
            TestFlowId.Create(),
            storedId,
            existingEffects ?? new List<StoredEffect>(),
            functionStore,
            DefaultSerializer.Instance,
            owner: null,
            storageSession: null,
            clearChildren: true
        );

        return new Effect(
            effectResults,
            utcNow: () => DateTime.UtcNow,
            new FlowTimeouts(),
            new FlowExecutionState(storedId, subflows: 1, waitingSubflows: 0, new FlowTimeouts(), completed: ForeverTask.Instance)
        );
    }

    private static async Task<StoredEffect> GetStoredEffect(IFunctionStore store, StoredId storedId, EffectId effectId)
        => (await store.GetFunction(storedId))!.Effects!.Single(e => e.EffectId == effectId);

    private static async Task<StoredEffect> GetSingleStoredEffect(IFunctionStore store, StoredId storedId)
        => (await store.GetFunction(storedId))!.Effects!.Single();

    [TestMethod]
    public async Task CapturedResultIsPersistedWithItsType()
    {
        var store = new InMemoryFunctionStore();
        var storedId = TestStoredId.Create();
        await store.CreateFunction(storedId, "humanInstanceId", param: null, postponeUntil: null, timestamp: DateTime.UtcNow.Ticks, parent: null, owner: null);

        var effect = CreateEffect(storedId, store);
        await effect.Capture(() => "SomeResult".ToTask());

        var storedEffect = await GetSingleStoredEffect(store, storedId);
        var resultType = DefaultSerializer.Instance.ResolveType(storedEffect.ResultType!);
        resultType.ShouldBe(typeof(string));
        DefaultSerializer.Instance.Deserialize(storedEffect.Result!, resultType!).ShouldBe("SomeResult");
    }

    [TestMethod]
    public async Task UpsertedValueIsPersistedWithItsType()
    {
        var store = new InMemoryFunctionStore();
        var storedId = TestStoredId.Create();
        await store.CreateFunction(storedId, "humanInstanceId", param: null, postponeUntil: null, timestamp: DateTime.UtcNow.Ticks, parent: null, owner: null);

        var effect = CreateEffect(storedId, store);
        var effectId = new EffectId([1]);
        await effect.Upsert(effectId, value: 42, alias: null, flush: true);

        var storedEffect = await GetStoredEffect(store, storedId, effectId);
        var resultType = DefaultSerializer.Instance.ResolveType(storedEffect.ResultType!);
        resultType.ShouldBe(typeof(int));
        DefaultSerializer.Instance.Deserialize(storedEffect.Result!, resultType!).ShouldBe(42);
    }

    [TestMethod]
    public async Task CreateOrGetValueIsPersistedWithItsType()
    {
        var store = new InMemoryFunctionStore();
        var storedId = TestStoredId.Create();
        await store.CreateFunction(storedId, "humanInstanceId", param: null, postponeUntil: null, timestamp: DateTime.UtcNow.Ticks, parent: null, owner: null);

        var effect = CreateEffect(storedId, store);
        var effectId = new EffectId([1]);
        await effect.CreateOrGet(effectId, value: new Person("Peter", 32), alias: null, flush: true);

        var storedEffect = await GetStoredEffect(store, storedId, effectId);
        var resultType = DefaultSerializer.Instance.ResolveType(storedEffect.ResultType!);
        resultType.ShouldBe(typeof(Person));
        DefaultSerializer.Instance.Deserialize(storedEffect.Result!, resultType!).ShouldBe(new Person("Peter", 32));
    }

    [TestMethod]
    public async Task ResultCapturedThroughBaseTypeIsPersistedAndReadBackAsItsActualType()
    {
        var store = new InMemoryFunctionStore();
        var storedId = TestStoredId.Create();
        await store.CreateFunction(storedId, "humanInstanceId", param: null, postponeUntil: null, timestamp: DateTime.UtcNow.Ticks, parent: null, owner: null);

        EffectContext.Reset();
        var effect = CreateEffect(storedId, store);
        await effect.Capture<Animal>(() => Task.FromResult<Animal>(new Dog("Fido", Breed: "Beagle")));

        var storedEffect = await GetSingleStoredEffect(store, storedId);
        DefaultSerializer.Instance.ResolveType(storedEffect.ResultType!).ShouldBe(typeof(Dog));

        // Replaying the same capture against the persisted effect returns the instance that was captured -
        // not an Animal-shaped shell of it.
        EffectContext.Reset();
        var restarted = CreateEffect(storedId, store, existingEffects: [storedEffect]);
        var replayed = await restarted.Capture<Animal>(
            () => Task.FromException<Animal>(new InvalidOperationException("Work should not be invoked on replay"))
        );
        replayed.ShouldBe(new Dog("Fido", Breed: "Beagle"));
    }

    [TestMethod]
    public async Task LazilyTypedSequenceIsMaterializedBeforeItIsPersisted()
    {
        var store = new InMemoryFunctionStore();
        var storedId = TestStoredId.Create();
        await store.CreateFunction(storedId, "humanInstanceId", param: null, postponeUntil: null, timestamp: DateTime.UtcNow.Ticks, parent: null, owner: null);

        var names = new List<string> { "Peter", "Ole", "Paul" };

        EffectContext.Reset();
        var effect = CreateEffect(storedId, store);
        // The runtime type here is a compiler-generated LINQ iterator - it must not be what gets persisted.
        await effect.Capture<IEnumerable<string>>(() => Task.FromResult(names.Where(n => n.Length == 5)));

        var storedEffect = await GetSingleStoredEffect(store, storedId);
        DefaultSerializer.Instance.ResolveType(storedEffect.ResultType!).ShouldBe(typeof(List<string>));

        EffectContext.Reset();
        var restarted = CreateEffect(storedId, store, existingEffects: [storedEffect]);
        var replayed = await restarted.Capture<IEnumerable<string>>(
            () => Task.FromException<IEnumerable<string>>(new InvalidOperationException("Work should not be invoked on replay"))
        );
        replayed.ShouldBe(new[] { "Peter" });
    }

    [TestMethod]
    public async Task LazilyTypedSequenceCapturedAsObjectIsMaterializedBeforeItIsPersisted()
    {
        var store = new InMemoryFunctionStore();
        var storedId = TestStoredId.Create();
        await store.CreateFunction(storedId, "humanInstanceId", param: null, postponeUntil: null, timestamp: DateTime.UtcNow.Ticks, parent: null, owner: null);

        var numbers = new List<int> { 1, 2, 3 };

        EffectContext.Reset();
        var effect = CreateEffect(storedId, store);
        await effect.Capture<object>(() => Task.FromResult<object>(numbers.Select(n => n * 2)));

        var storedEffect = await GetSingleStoredEffect(store, storedId);
        DefaultSerializer.Instance.ResolveType(storedEffect.ResultType!).ShouldBe(typeof(List<int>));

        // Without the materialized type the declared type is all there is to go on, and object yields a
        // JsonElement rather than the captured sequence.
        EffectContext.Reset();
        var restarted = CreateEffect(storedId, store, existingEffects: [storedEffect]);
        var replayed = await restarted.Capture<object>(
            () => Task.FromException<object>(new InvalidOperationException("Work should not be invoked on replay"))
        );
        replayed.ShouldBe(new List<int> { 2, 4, 6 });
    }

    [TestMethod]
    public async Task PubliclyNamedCollectionIsPersistedAsIs()
    {
        var store = new InMemoryFunctionStore();
        var storedId = TestStoredId.Create();
        await store.CreateFunction(storedId, "humanInstanceId", param: null, postponeUntil: null, timestamp: DateTime.UtcNow.Ticks, parent: null, owner: null);

        EffectContext.Reset();
        var effect = CreateEffect(storedId, store);
        await effect.Capture<IEnumerable<string>>(() => Task.FromResult<IEnumerable<string>>(new[] { "Peter", "Ole" }));

        var storedEffect = await GetSingleStoredEffect(store, storedId);
        DefaultSerializer.Instance.ResolveType(storedEffect.ResultType!).ShouldBe(typeof(string[]));
    }

    [TestMethod]
    public async Task DictionaryIsPersistedAsIs()
    {
        var store = new InMemoryFunctionStore();
        var storedId = TestStoredId.Create();
        await store.CreateFunction(storedId, "humanInstanceId", param: null, postponeUntil: null, timestamp: DateTime.UtcNow.Ticks, parent: null, owner: null);

        EffectContext.Reset();
        var effect = CreateEffect(storedId, store);
        var dictionary = new Dictionary<string, int> { { "Peter", 32 } };
        await effect.Capture<IDictionary<string, int>>(() => Task.FromResult<IDictionary<string, int>>(dictionary));

        var storedEffect = await GetSingleStoredEffect(store, storedId);
        DefaultSerializer.Instance.ResolveType(storedEffect.ResultType!).ShouldBe(typeof(Dictionary<string, int>));

        EffectContext.Reset();
        var restarted = CreateEffect(storedId, store, existingEffects: [storedEffect]);
        var replayed = await restarted.Capture<IDictionary<string, int>>(
            () => Task.FromException<IDictionary<string, int>>(new InvalidOperationException("Work should not be invoked on replay"))
        );
        replayed.ShouldBe(dictionary);
    }

    [TestMethod]
    public async Task NonVisibleReadOnlyDictionaryIsMaterializedIntoADictionaryBeforeItIsPersisted()
    {
        var store = new InMemoryFunctionStore();
        var storedId = TestStoredId.Create();
        await store.CreateFunction(storedId, "humanInstanceId", param: null, postponeUntil: null, timestamp: DateTime.UtcNow.Ticks, parent: null, owner: null);

        EffectContext.Reset();
        var effect = CreateEffect(storedId, store);
        IReadOnlyDictionary<string, int> wrapper = new ReadOnlyDictionaryWrapper(new Dictionary<string, int> { { "Peter", 32 } });
        await effect.Capture(() => Task.FromResult(wrapper));

        // Materialized as a dictionary - not a list of pairs - so the payload keeps its JSON-object shape.
        var storedEffect = await GetSingleStoredEffect(store, storedId);
        DefaultSerializer.Instance.ResolveType(storedEffect.ResultType!).ShouldBe(typeof(Dictionary<string, int>));

        EffectContext.Reset();
        var restarted = CreateEffect(storedId, store, existingEffects: [storedEffect]);
        var replayed = await restarted.Capture<IReadOnlyDictionary<string, int>>(
            () => Task.FromException<IReadOnlyDictionary<string, int>>(new InvalidOperationException("Work should not be invoked on replay"))
        );
        replayed.ShouldBe(new Dictionary<string, int> { { "Peter", 32 } });
    }

    [TestMethod]
    public async Task EffectWithoutResultHasNoResultType()
    {
        var store = new InMemoryFunctionStore();
        var storedId = TestStoredId.Create();
        await store.CreateFunction(storedId, "humanInstanceId", param: null, postponeUntil: null, timestamp: DateTime.UtcNow.Ticks, parent: null, owner: null);

        var effect = CreateEffect(storedId, store);
        await effect.Capture(() => Task.CompletedTask);

        var storedEffect = await GetSingleStoredEffect(store, storedId);
        storedEffect.Result.ShouldBeNull();
        storedEffect.ResultType.ShouldBeNull();
    }

    private record Person(string Name, int Age);

    private abstract record Animal(string Name);
    private record Dog(string Name, string Breed) : Animal(Name);

    private class ReadOnlyDictionaryWrapper(Dictionary<string, int> inner) : IReadOnlyDictionary<string, int>
    {
        public IEnumerator<KeyValuePair<string, int>> GetEnumerator() => inner.GetEnumerator();
        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
        public int Count => inner.Count;
        public bool ContainsKey(string key) => inner.ContainsKey(key);
        public bool TryGetValue(string key, out int value) => inner.TryGetValue(key, out value);
        public int this[string key] => inner[key];
        public IEnumerable<string> Keys => inner.Keys;
        public IEnumerable<int> Values => inner.Values;
    }
}
