﻿using System.Collections.Concurrent;
using System.Diagnostics;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.StressTests.Engines;
using Cleipnir.ResilientFunctions.StressTests.StressTests.Utils;

namespace Cleipnir.ResilientFunctions.StressTests.StressTests;

public static class BulkInsertionTest
{
    public static async Task<TestResult> Perform(IEngine helper)
    {
        const int testSize = 5000;

        await helper.InitializeDatabaseAndInitializeAndTruncateTable();
        var store = await helper.CreateFunctionStore();
        
        var flowType = new FlowType("BulkInsertionTest");
        var storedType = await store.TypeStore.InsertOrGetStoredType(flowType);
        
        var bag = new ConcurrentBag<int>();
        
        using var functionsRegistry1 = new FunctionsRegistry(
            store,
            new Settings(
                unhandledExceptionHandler: Console.WriteLine,
                leaseLength: TimeSpan.FromSeconds(10),
                watchdogCheckFrequency: TimeSpan.FromMilliseconds(1000)
            )
        );
        functionsRegistry1.RegisterAction(
            flowType,
            Task (string param) =>
            {
                bag.Add(1);
                return Task.CompletedTask;
            }
        );
        
        using var functionsRegistry2 = new FunctionsRegistry(
            store,
            new Settings(
                unhandledExceptionHandler: Console.WriteLine,
                leaseLength: TimeSpan.FromSeconds(10),
                watchdogCheckFrequency: TimeSpan.FromMilliseconds(1000)
            )
        );
        functionsRegistry2.RegisterAction(
            flowType,
            Task (string param) =>
            {
                bag.Add(2);
                return Task.CompletedTask;
            }
        );
        
        using var functionsRegistry3 = new FunctionsRegistry(
            store,
            new Settings(
                unhandledExceptionHandler: Console.WriteLine,
                leaseLength: TimeSpan.FromSeconds(10),
                watchdogCheckFrequency: TimeSpan.FromMilliseconds(1000)
            )
        );
        functionsRegistry3.RegisterAction(
            flowType,
            Task (string param) =>
            {
                bag.Add(3);
                return Task.CompletedTask;
            }
        );
        
        var stopWatch = new Stopwatch();
        stopWatch.Start();
        
        Console.WriteLine("BULK_INSERTION_TEST: Initializing");
        var functions = Enumerable
            .Range(0, testSize)
            .Select(i => new IdWithParam(
                new StoredId(storedType, i.ToString().ToStoredInstance()),
                "humanInstanceId",
                i.ToString().ToJson().ToUtf8Bytes())
            );
        await store.BulkScheduleFunctions(functions, parent: null);
        
        stopWatch.Stop();
        var insertionAverageSpeed = testSize * 1000 / stopWatch.ElapsedMilliseconds;
        Console.WriteLine($"BULK_INSERTION_TEST: Initialization took: {stopWatch.Elapsed} with average speed (s): {insertionAverageSpeed}");

        Console.WriteLine("BULK_INSERTION_TEST: Waiting for invocations to begin");

        var executionAverageSpeed = await 
            WaitFor.AllSuccessfullyCompleted(helper, testSize, logPrefix: "BULK_INSERTION_TEST: ");

        var results = bag.GroupBy(_ => _).Select(g => new { Id = g.Key, Count = g.Count() }.ToString()).ToList();
        Console.WriteLine("BULK_INSERTION_TEST WORK-DISTRIBUTION:");
        Console.WriteLine(string.Join(Environment.NewLine, results));
        Console.WriteLine();
        
        return new TestResult(insertionAverageSpeed, executionAverageSpeed);
    }
}