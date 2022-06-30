using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.ParameterSerialization;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Tests.Utils;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Newtonsoft.Json;
using Shouldly;

namespace Cleipnir.ResilientFunctions.Tests.InMemoryTests;

[TestClass]
public class SerializationTests
{
    [TestMethod]
    public async Task TypeNameChangeScenarioIsHandledSuccessfullyByCustomSerializer()
    {
        var prev = new PersonPrev { Name = "Peter" };
        var serializer = new Serializer();
        var flag = new SyncedFlag();
        var store = new InMemoryFunctionStore();
        await store.CreateFunction(
            new FunctionId("typeId", "instanceId"),
            new StoredParameter(prev.ToJson(), typeof(PersonPrev).SimpleQualifiedName()),
            scrapbookType: null,
            initialStatus: Status.Executing,
            initialEpoch: 0,
            initialSignOfLife: 0
        ).ShouldBeTrueAsync();
        
        using var rFunctions = new FunctionContainer(
            store, 
            new Settings(CrashedCheckFrequency: TimeSpan.FromMilliseconds(1))
        );

        var personCurr = default(PersonCurr);
        _ = rFunctions
            .RegisterAction(
                "typeId".ToFunctionTypeId(),
                Task (PersonCurr p) =>
                {
                    personCurr = p;
                    flag.Raise();
                    return Task.CompletedTask;
                },
                new Settings(Serializer: serializer)
            );

        await flag.WaitForRaised();
        personCurr.ShouldNotBeNull();
        personCurr.Name.ShouldBe("Peter");
    }

    private class PersonPrev
    {
        public string? Name { get; set; }
    }

    private class PersonCurr
    {
        public string? Name { get; set; }
    }

    private class Serializer : ISerializer
    {
        private readonly DefaultSerializer _defaultSerializer = DefaultSerializer.Instance;
        
        public string SerializeParameter(object parameter)
            => JsonConvert.SerializeObject(parameter);

        public object DeserializeParameter(string json, string type)
        {
            if (type.Contains(nameof(PersonPrev)))
                return JsonConvert.DeserializeObject<PersonCurr>(json)!;

            return JsonConvert.DeserializeObject(
                json,
                Type.GetType(type, throwOnError: true)!
            )!;
        }

        public string SerializeScrapbook(Scrapbook scrapbook)
            => _defaultSerializer.SerializeScrapbook(scrapbook);

        public Scrapbook DeserializeScrapbook(string? json, string type)
            => _defaultSerializer.DeserializeScrapbook(json, type);

        public string SerializeError(Error error)
            => _defaultSerializer.SerializeError(error);

        public Error DeserializeError(string json)
            => _defaultSerializer.DeserializeError(json);
        
        public string SerializeResult(object result)
            => _defaultSerializer.SerializeResult(result);

        public object DeserializeResult(string json, string type)
            => _defaultSerializer.DeserializeResult(json, type);
    }
}