using System;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Domain.Events;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Reactive.Extensions;
using Cleipnir.ResilientFunctions.Reactive.Utilities;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Tests.Utils;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Shouldly;

namespace Cleipnir.ResilientFunctions.Tests.ReactiveTests;

[TestClass]
public class TimeoutTests
{
    //rewrite into integration tests
    [TestMethod]
    public async Task StreamCompletesAndThrowsNoResultExceptionAfterFiredTimeoutEvent()
    {
        var timeoutId = "TimeoutId";
        var expiresAt = DateTime.UtcNow.Add(TimeSpan.FromMinutes(15));
        
        using var registry = new FunctionsRegistry(new InMemoryFunctionStore());
        var flow = registry.RegisterFunc<string, bool>("Flow",
            async (_, workflow) =>
            {
                var messages = workflow.Messages;
                try
                {
                    await messages.TakeUntilTimeout(timeoutId, expiresAt).First(maxWait: TimeSpan.FromSeconds(5));
                }
                catch (NoResultException)
                {
                    return true;
                }

                return false;
            });

        var scheduled = await flow.Schedule("Instance", param: "");
        
        var cp = await flow.ControlPanel("Instance").ShouldNotBeNullAsync();
        await cp.BusyWaitUntil(c => c.Effects.AllIds.SelectAsync(ids => ids.Any()));
        
        var timeoutEffectId = (await cp.Effects.AllIds).Single(eId => eId.Id == timeoutId);
        var effectTimeout = await cp.Effects.GetValue<DateTime>(timeoutEffectId);
        
        effectTimeout.ShouldBe(expiresAt);

        await flow.SendMessage("Instance", new TimeoutEvent(timeoutEffectId, effectTimeout));
        var result = await scheduled.Completion();
        result.ShouldBeTrue();
    }
    
    
    [TestMethod]
    public async Task StreamCompletesAndReturnsNothingAfterFiredTimeoutEvent()
    {
        var timeoutId = "TimeoutId";
        var effectId = new EffectId(timeoutId, EffectType.Timeout, Context: "");
        var expiresAt = DateTime.UtcNow.Add(TimeSpan.FromMinutes(15));
    
        using var registry = new FunctionsRegistry(new InMemoryFunctionStore());
        var flow = registry.RegisterFunc<string, Option<object>>("Flow",
            async (_, workflow) =>
            {
                var messages = workflow.Messages;
                return await messages.TakeUntilTimeout(timeoutId, expiresAt).FirstOrNone();
            });

        var scheduled = await flow.Schedule("Instance", param: "");
        
        var cp = await flow.ControlPanel("Instance").ShouldNotBeNullAsync();
        await cp.BusyWaitUntil(c => c.Effects.AllIds.SelectAsync(ids => ids.Any()));

        var effectExpiresAt = await cp.Effects.GetValue<DateTime>(effectId);
        effectExpiresAt.ShouldBe(expiresAt);

        await flow.SendMessage("Instance", new TimeoutEvent(effectId, expiresAt));

        var result = await scheduled.Completion();
        result.HasValue.ShouldBeFalse();
    }
    
    [TestMethod]
    public async Task StreamCompletesSuccessfullyWhenEventSupersedesTimeout()
    {
        var timeoutId = "TimeoutId";
        var expiresAt = DateTime.UtcNow.Add(TimeSpan.FromMinutes(15));
     
        using var registry = new FunctionsRegistry(new InMemoryFunctionStore());
        var flow = registry.RegisterFunc<string, string>("Flow",
            async (_, workflow) =>
            {
                var messages = workflow.Messages;
                return await messages.TakeUntilTimeout(timeoutId, expiresAt).FirstOfType<string>();
            });

        var scheduled = await flow.Schedule("Instance", param: "");
        await flow.SendMessage("Instance", "Hello");

        var result = await scheduled.Completion();
        result.ShouldBe("Hello");
    }
    
    [TestMethod]
    public async Task StreamCompletesSuccessfullyWithValuedOptionWhenEventSupersedesTimeout()
    {
        var timeoutId = "TimeoutId";
        var expiresAt = DateTime.UtcNow.Add(TimeSpan.FromMinutes(15));
        
        using var registry = new FunctionsRegistry(new InMemoryFunctionStore());
        var flow = registry.RegisterFunc<string, Option<string>>("Flow",
            async (_, workflow) =>
            {
                var messages = workflow.Messages;
                return await messages.TakeUntilTimeout(timeoutId, expiresAt).OfType<string>().FirstOrNone();
            });

        var scheduled = await flow.Schedule("Instance", param: "");
        await flow.SendMessage("Instance", "Hello");

        var result = await scheduled.Completion();
        result.HasValue.ShouldBeTrue();
        result.Value.ShouldBe("Hello");
    }
}