using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Queuing;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Shouldly;

namespace Cleipnir.ResilientFunctions.Tests.UtilsTests;

[TestClass]
public class QueueFlagTests
{
    [TestMethod]
    public async Task RaiseBeforeWaitCompletesImmediately()
    {
        var flag = new QueueFlag();

        flag.Raise();
        var task = flag.WaitForRaised();

        task.IsCompleted.ShouldBeTrue();
        await task;
    }

    [TestMethod]
    public async Task WaitBeforeRaiseCompletesAfterRaise()
    {
        var flag = new QueueFlag();

        var waitTask = flag.WaitForRaised();
        waitTask.IsCompleted.ShouldBeFalse();

        flag.Raise();

        await waitTask.ShouldCompleteIn(100);
    }

    [TestMethod]
    public async Task MultipleRaisesOnlySignalsOnce()
    {
        var flag = new QueueFlag();

        flag.Raise();
        flag.Raise();
        flag.Raise();

        var firstWait = flag.WaitForRaised();
        firstWait.IsCompleted.ShouldBeTrue();

        var secondWait = flag.WaitForRaised();
        secondWait.IsCompleted.ShouldBeFalse();

        flag.Raise();
        await secondWait.ShouldCompleteIn(100);
    }

    [TestMethod]
    public async Task AlternatingRaiseAndWaitWorks()
    {
        var flag = new QueueFlag();

        for (var i = 0; i < 5; i++)
        {
            flag.Raise();
            var task = flag.WaitForRaised();
            task.IsCompleted.ShouldBeTrue();
            await task;
        }
    }

    [TestMethod]
    public async Task AlternatingWaitAndRaiseWorks()
    {
        var flag = new QueueFlag();

        for (var i = 0; i < 5; i++)
        {
            var waitTask = flag.WaitForRaised();
            waitTask.IsCompleted.ShouldBeFalse();

            flag.Raise();
            await waitTask.ShouldCompleteIn(100);
        }
    }

    [TestMethod]
    public async Task ConcurrentProducerConsumerScenario()
    {
        var flag = new QueueFlag();
        var producerCount = 0;
        var consumerCount = 0;
        var iterations = 100;

        var producer = Task.Run(async () =>
        {
            for (var i = 0; i < iterations; i++)
            {
                producerCount++;
                flag.Raise();
                await Task.Delay(1);
            }
        });

        var consumer = Task.Run(async () =>
        {
            for (var i = 0; i < iterations; i++)
            {
                await flag.WaitForRaised();
                consumerCount++;
            }
        });

        await Task.WhenAll(producer, consumer).ShouldCompleteIn(5000);

        producerCount.ShouldBe(iterations);
        consumerCount.ShouldBe(iterations);
    }

    [TestMethod]
    public async Task RaiseWithNoWaiterThenWaitCompletesImmediately()
    {
        var flag = new QueueFlag();

        flag.Raise();
        await Task.Delay(10);

        var task = flag.WaitForRaised();
        task.IsCompleted.ShouldBeTrue();
        await task;
    }
}

internal static class TaskExtensions
{
    public static async Task ShouldCompleteIn(this Task task, int milliseconds)
    {
        var completedTask = await Task.WhenAny(task, Task.Delay(milliseconds));
        if (completedTask != task)
            throw new AssertFailedException($"Task did not complete within {milliseconds}ms");

        await task;
    }
}