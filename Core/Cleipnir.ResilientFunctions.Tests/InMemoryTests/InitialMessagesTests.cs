using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Messaging;
using Cleipnir.ResilientFunctions.Tests.Utils;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Shouldly;

namespace Cleipnir.ResilientFunctions.Tests.InMemoryTests;

[TestClass]
public class InitialMessagesTests
{
    // Initial messages are written straight into the flow's effect state as staged-message children and are
    // admitted on sight when staged, so duplicate idempotency keys are resolved when the children are created -
    // the first message per key wins. Messages without a key are always distinct.
    [TestMethod]
    public async Task InitialMessagesWithDuplicateIdempotencyKeysAreFilteredOut()
    {
        var store = await Utils.CreateInMemoryFunctionStoreTask();
        var flowId = TestFlowId.Create();
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();

        using var functionsRegistry = new FunctionsRegistry(store, new Settings(unhandledExceptionCatcher.Catch));

        var registration = functionsRegistry.RegisterFunc(
            flowId.Type,
            async Task<string> (string _, Workflow workflow) =>
            {
                var received = new List<string>();
                while (true)
                {
                    var message = await workflow.Message<string>(TimeSpan.FromMilliseconds(250));
                    if (message is null)
                        break;

                    received.Add(message);
                }

                return string.Join(",", received);
            }
        );

        var scheduled = await registration.Schedule(
            flowInstance: flowId.Instance.Value,
            param: "param",
            initialState: InitialState.CreateWithMessagesOnly([
                new MessageAndIdempotencyKey("first", IdempotencyKey: "key"),
                new MessageAndIdempotencyKey("duplicate", IdempotencyKey: "key"),
                new MessageAndIdempotencyKey("second", IdempotencyKey: "otherKey"),
                new MessageAndIdempotencyKey("unkeyed"),
                new MessageAndIdempotencyKey("unkeyed")
            ])
        );

        var result = await scheduled.Completion(TimeSpan.FromSeconds(10));

        // "duplicate" is dropped, the two unkeyed messages both survive, and the supplied order is preserved.
        result.ShouldBe("first,second,unkeyed,unkeyed");

        unhandledExceptionCatcher.ShouldNotHaveExceptions();
    }
}
