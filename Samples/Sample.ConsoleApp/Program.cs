using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Messaging;
using Cleipnir.ResilientFunctions.Redis;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Tests.Messaging.Utils;

namespace ConsoleApp;

internal static class Program
{
    private static async Task Main()
    {
        var redisStore = new RedisFunctionStore("localhost");
        var eventStore = new RedisEventStore("localhost");
        var functionId = new FunctionId("OrderFlow", "MK-54321");
        await redisStore.DeleteFunction(functionId);
        await eventStore.Truncate(functionId);
        var result = await redisStore.CreateFunction(
            functionId,
            new StoredParameter("hello".ToJson(), typeof(string).SimpleQualifiedName()),
            storedScrapbook: new StoredScrapbook(new RScrapbook().ToJson(), typeof(RScrapbook).SimpleQualifiedName()),
            storedEvents: null,
            leaseExpiration: DateTime.UtcNow.Ticks,
            postponeUntil: 2L
        );
        await eventStore.AppendEvents(functionId, storedEvents: new []
        {
            new StoredEvent("hello world".ToJson(), typeof(string).SimpleQualifiedName()),
            new StoredEvent("hello universe".ToJson(), typeof(string).SimpleQualifiedName())
        });
        
        var events = await eventStore.GetEvents(functionId);
        Console.ReadLine();
        
        await redisStore.SaveScrapbookForExecutingFunction(
            functionId,
            "updatedScrapbook",
            expectedEpoch: 0,
            new ComplimentaryState.SaveScrapbookForExecutingFunction()
        );

        //await redisStore.RestartExecution(functionId, expectedEpoch: 0, leaseExpiration: 1);
        
//        var sf = await redisStore.GetFunction(functionId);

//        var deleted = await redisStore.DeleteFunction(functionId);//, expectedEpoch: 0);

        await redisStore.FailFunction(
            functionId,
            new StoredException("some msg", null, typeof(Exception).SimpleQualifiedName()),
            "scrapbookJson",
            expectedEpoch: 0,
            new ComplimentaryState.SetResult()
        );
        var sf = await redisStore.GetFunction(functionId);

        await redisStore.PostponeFunction(
            functionId,
            postponeUntil: DateTime.UtcNow.Ticks,
            "scrapbookJson",
            expectedEpoch: 0,
            new ComplimentaryState.SetResult()
        );

        await redisStore.IncrementAlreadyPostponedFunctionEpoch(functionId, expectedEpoch: 0);
        
        await redisStore.SucceedFunction(
            functionId,
            new StoredResult("someJson", "someType"),
            "someScrapbookJson",
            0,
            new ComplimentaryState.SetResult()
        );
        sf = await redisStore.GetFunction(functionId);
        Console.WriteLine(result);
        Console.WriteLine(sf);
    }
}