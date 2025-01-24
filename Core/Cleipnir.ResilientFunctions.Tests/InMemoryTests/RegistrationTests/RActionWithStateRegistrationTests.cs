﻿using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.Serialization;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Storage;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Newtonsoft.Json.Linq;
using Shouldly;

namespace Cleipnir.ResilientFunctions.Tests.InMemoryTests.RegistrationTests;

[TestClass]
public class RActionWithStateRegistrationTests
{
    private readonly FlowType _flowType = new FlowType("flowType");
    private const string flowInstance = "flowInstance";
    
    [TestMethod]
    public async Task ConstructedFuncInvokeCanBeCreatedAndInvoked()
    {
        using var rFunctions = CreateRFunctions();
        var rAction = rFunctions
            .RegisterAction<string>(
                _flowType,
                InnerAction
            )
            .Invoke;

        await rAction(flowInstance, "hello world");
    }
    
    [TestMethod]
    public async Task ConstructedFuncWithCustomSerializerCanBeCreatedAndInvoked()
    {
        using var rFunctions = CreateRFunctions();
        var serializer = new Serializer();
        var rAction = rFunctions
            .RegisterAction<string>(
                _flowType,
                InnerAction,
                new Settings(serializer: serializer)
            )
            .Invoke;

        await rAction(flowInstance, "hello world");
        serializer.Invoked.ShouldBeTrue();
    }

    private async Task InnerAction(string param) => await Task.CompletedTask;
    private FunctionsRegistry CreateRFunctions() => new(new InMemoryFunctionStore());

    private class Serializer : ISerializer
    {
        public bool Invoked { get; set; }
        private ISerializer Default { get; } = DefaultSerializer.Instance;

        public byte[] Serialize<T>(T value) 
        {
            Invoked = true;
            return Default.Serialize(value);
        }

        public T Deserialize<T>(byte[] json) 
            => Default.Deserialize<T>(json);

        public StoredException SerializeException(FatalWorkflowException exception)
            => Default.SerializeException(exception);
        public FatalWorkflowException DeserializeException(FlowId flowId, StoredException storedException)
            => Default.DeserializeException(flowId, storedException);
        
        public SerializedMessage SerializeMessage<TEvent>(TEvent message) where TEvent : notnull
            => Default.SerializeMessage(message);
        public object DeserializeMessage(byte[] json, byte[] type)
            => Default.DeserializeMessage(json, type);
    }
}