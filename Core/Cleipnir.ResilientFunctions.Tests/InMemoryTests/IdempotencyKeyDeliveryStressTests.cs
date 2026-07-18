using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Tests.Utils;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.Tests.InMemoryTests;

// Regression stress test for the duplicate-idempotency-key redelivery race behind flaky
// MultipleIterationsWithDuplicateIdempotencyKeysProcessCorrectly failures: a watchdog push racing the
// disposal of a suspending incarnation used to overwrite the pending delivered-positions list with a
// nearly-empty one, durably erasing the incarnation's delivered markings so the reopened rows were
// redelivered by a later incarnation. The tightened timings make the race likely within the 30 rounds
// (pre-fix: ~3-4 failing rounds per run).
[TestClass]
public class IdempotencyKeyDeliveryStressTests
{
    [TestMethod]
    public async Task DuplicateIdempotencyKeysAreNotRedeliveredAcrossRestarts()
    {
        var failures = new List<string>();
        for (var round = 0; round < 30; round++)
        {
            var functionStore = await Utils.CreateInMemoryFunctionStoreTask();
            var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
            using var functionsRegistry = new FunctionsRegistry(
                functionStore,
                new Settings(unhandledExceptionCatcher.Catch, watchdogCheckFrequency: TimeSpan.FromMilliseconds(20))
            );

            StoredId? storedId = null;
            var invocations = new SyncedList<string>();
            var rFunc = functionsRegistry.RegisterFunc(
                $"StressRound{round}",
                inner: async Task<string> (string _, Workflow workflow) =>
                {
                    storedId = workflow.StoredId;
                    var invocationLog = $"inv@{DateTime.UtcNow:HH:mm:ss.fff}";
                    var receivedMessages = new List<string>();
                    var message = "";
                    while (message != "stop")
                    {
                        message = await workflow.Message<string>(TimeSpan.FromMilliseconds(30));
                        invocations.Add($"{invocationLog} got:{message ?? "<null>"}");

                        if (message is null)
                            await workflow.Effect.Flush();
                        else if (message is "10" or "20" or "30" or "40")
                        {
                            await workflow.Delay(TimeSpan.FromMilliseconds(30));
                            receivedMessages.Add(message);
                        }
                        else if (message != "stop")
                            receivedMessages.Add(message);
                    }

                    return string.Join(",", receivedMessages);
                }
            );

            var scheduled = await rFunc.Schedule("instanceId", "");
            var messageWriter = rFunc.MessageWriters.For("instanceId".ToFlowInstance());
            for (var iteration = 0; iteration < 100; iteration += 10)
                for (var repeat = 0; repeat < 2; repeat++)
                    for (var i = 0; i < 10; i++)
                        await messageWriter.AppendMessage((iteration + i).ToString(), idempotencyKey: ((iteration + i) % 50).ToString());

            await BusyWait.Until(() => storedId != null);
            await BusyWait.Until(
                async () => await functionStore.MessageStore.GetMessages([storedId!]).SelectAsync(m => m[storedId!].Count) == 0,
                maxWait: TimeSpan.FromSeconds(30)
            );
            await messageWriter.AppendMessage("stop");

            var result = await scheduled.Completion(timeout: TimeSpan.FromSeconds(30));
            var receivedMessages = result.Split(',').Select(int.Parse).OrderBy(x => x).ToList();

            if (receivedMessages.Count != 50 || receivedMessages.Where((v, idx) => v != idx).Any())
            {
                var counts = receivedMessages.GroupBy(v => v).Where(g => g.Count() > 1).Select(g => $"{g.Key}x{g.Count()}");
                failures.Add(
                    $"round {round}: count={receivedMessages.Count} dups=[{string.Join(",", counts)}]" +
                    Environment.NewLine + $"  raw=[{result}]" +
                    Environment.NewLine + $"  deliveries:{Environment.NewLine}    {string.Join(Environment.NewLine + "    ", invocations)}"
                );
            }
        }

        if (failures.Any())
            throw new Exception(string.Join(Environment.NewLine, failures));
    }
}
