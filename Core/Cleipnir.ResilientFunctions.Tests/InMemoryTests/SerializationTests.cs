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
            crashedCheckFrequency: 100
        ).ShouldBeTrueAsync();
        
        using var rFunctions = new RFunctions(
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
        public TParam DeserializeParameter<TParam>(string json, string type)
        {
            if (type.Contains(nameof(PersonPrev)))
                return (TParam) (object) JsonConvert.DeserializeObject<PersonCurr>(json)!;

            return (TParam) JsonConvert.DeserializeObject(
                json,
                Type.GetType(type, throwOnError: true)!
            )!;
        }

        public string SerializeScrapbook(RScrapbook scrapbook)
            => _defaultSerializer.SerializeScrapbook(scrapbook);
        public TScrapbook DeserializeScrapbook<TScrapbook>(string? json, string type) where TScrapbook : RScrapbook
            => _defaultSerializer.DeserializeScrapbook<TScrapbook>(json, type);

        public string SerializeError(RError error)
            => _defaultSerializer.SerializeError(error);
        public RError DeserializeError(string json)
            => _defaultSerializer.DeserializeError(json);
        
        public string SerializeResult(object result)
            => _defaultSerializer.SerializeResult(result);
        public TResult DeserializeResult<TResult>(string json, string type)
            => _defaultSerializer.DeserializeResult<TResult>(json, type);
    }
}