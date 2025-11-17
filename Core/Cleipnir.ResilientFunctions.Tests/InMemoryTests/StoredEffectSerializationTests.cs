using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Storage;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Shouldly;

namespace Cleipnir.ResilientFunctions.Tests.InMemoryTests;

[TestClass]
public class StoredEffectSerializationTests
{
    [TestMethod]
    public void CompletedStoredEffectWithResultCanBeSerializedAndDeserialized()
    {
        var effectId = new EffectId("SomeEffect", "");
        var result = "SomeResult"u8.ToArray();
        var storedEffect = StoredEffect.CreateCompleted(effectId, result);

        var serialized = storedEffect.Serialize();
        var deserialized = StoredEffect.Deserialize(serialized);

        deserialized.EffectId.ShouldBe(effectId);
        deserialized.WorkStatus.ShouldBe(WorkStatus.Completed);
        deserialized.Result.ShouldBe(result);
        deserialized.StoredException.ShouldBeNull();
    }

    [TestMethod]
    public void CompletedStoredEffectWithoutResultCanBeSerializedAndDeserialized()
    {
        var effectId = new EffectId("SomeEffect", "");
        var storedEffect = StoredEffect.CreateCompleted(effectId);

        var serialized = storedEffect.Serialize();
        var deserialized = StoredEffect.Deserialize(serialized);

        deserialized.EffectId.ShouldBe(effectId);
        deserialized.WorkStatus.ShouldBe(WorkStatus.Completed);
        deserialized.Result.ShouldBeNull();
        deserialized.StoredException.ShouldBeNull();
    }

    [TestMethod]
    public void StartedStoredEffectCanBeSerializedAndDeserialized()
    {
        var effectId = new EffectId("SomeEffect", "");
        var storedEffect = StoredEffect.CreateStarted(effectId);

        var serialized = storedEffect.Serialize();
        var deserialized = StoredEffect.Deserialize(serialized);

        deserialized.EffectId.ShouldBe(effectId);
        deserialized.WorkStatus.ShouldBe(WorkStatus.Started);
        deserialized.Result.ShouldBeNull();
        deserialized.StoredException.ShouldBeNull();
    }

    [TestMethod]
    public void FailedStoredEffectWithExceptionCanBeSerializedAndDeserialized()
    {
        var effectId = new EffectId("SomeEffect", "");
        var storedException = new StoredException(
            ExceptionMessage: "Something went wrong",
            ExceptionStackTrace: "at SomeMethod() in SomeFile.cs:line 42",
            ExceptionType: "System.InvalidOperationException"
        );
        var storedEffect = StoredEffect.CreateFailed(effectId, storedException);

        var serialized = storedEffect.Serialize();
        var deserialized = StoredEffect.Deserialize(serialized);

        deserialized.EffectId.ShouldBe(effectId);
        deserialized.WorkStatus.ShouldBe(WorkStatus.Failed);
        deserialized.Result.ShouldBeNull();
        deserialized.StoredException.ShouldNotBeNull();
        deserialized.StoredException.ExceptionMessage.ShouldBe("Something went wrong");
        deserialized.StoredException.ExceptionStackTrace.ShouldBe("at SomeMethod() in SomeFile.cs:line 42");
        deserialized.StoredException.ExceptionType.ShouldBe("System.InvalidOperationException");
    }

    [TestMethod]
    public void StoredEffectWithContextCanBeSerializedAndDeserialized()
    {
        var parentEffect = new EffectId("ParentEffect", "");
        var effectId = new EffectId("ChildEffect", parentEffect.Serialize().Value);
        var result = "SomeData"u8.ToArray();
        var storedEffect = StoredEffect.CreateCompleted(effectId, result);

        var serialized = storedEffect.Serialize();
        var deserialized = StoredEffect.Deserialize(serialized);

        deserialized.EffectId.ShouldBe(effectId);
        deserialized.EffectId.Context.ShouldBe(parentEffect.Serialize().Value);
        deserialized.WorkStatus.ShouldBe(WorkStatus.Completed);
        deserialized.Result.ShouldBe(result);
    }

    [TestMethod]
    public void StoredEffectWithLargeResultCanBeSerializedAndDeserialized()
    {
        var effectId = new EffectId("LargeEffect", "");
        var largeResult = new byte[10000];
        for (int i = 0; i < largeResult.Length; i++)
            largeResult[i] = (byte)(i % 256);

        var storedEffect = StoredEffect.CreateCompleted(effectId, largeResult);

        var serialized = storedEffect.Serialize();
        var deserialized = StoredEffect.Deserialize(serialized);

        deserialized.EffectId.ShouldBe(effectId);
        deserialized.WorkStatus.ShouldBe(WorkStatus.Completed);
        deserialized.Result.ShouldBe(largeResult);
    }

    [TestMethod]
    public void StoredEffectWithSpecialCharactersInIdCanBeSerializedAndDeserialized()
    {
        var effectId = new EffectId("Effect.With\\Special.Characters", "");
        var result = "Data"u8.ToArray();
        var storedEffect = StoredEffect.CreateCompleted(effectId, result);

        var serialized = storedEffect.Serialize();
        var deserialized = StoredEffect.Deserialize(serialized);

        deserialized.EffectId.ShouldBe(effectId);
        deserialized.EffectId.Id.ShouldBe("Effect.With\\Special.Characters");
        deserialized.WorkStatus.ShouldBe(WorkStatus.Completed);
        deserialized.Result.ShouldBe(result);
    }

    [TestMethod]
    public void StoredEffectWithEmptyIdCanBeSerializedAndDeserialized()
    {
        var effectId = new EffectId("", "");
        var storedEffect = StoredEffect.CreateCompleted(effectId);

        var serialized = storedEffect.Serialize();
        var deserialized = StoredEffect.Deserialize(serialized);

        deserialized.EffectId.ShouldBe(effectId);
        deserialized.EffectId.Id.ShouldBe("");
        deserialized.WorkStatus.ShouldBe(WorkStatus.Completed);
    }

    [TestMethod]
    public void StoredEffectWithNullStackTraceCanBeSerializedAndDeserialized()
    {
        var effectId = new EffectId("FailedEffect", "");
        var storedException = new StoredException(
            ExceptionMessage: "Error occurred",
            ExceptionStackTrace: null,
            ExceptionType: "System.Exception"
        );
        var storedEffect = StoredEffect.CreateFailed(effectId, storedException);

        var serialized = storedEffect.Serialize();
        var deserialized = StoredEffect.Deserialize(serialized);

        deserialized.EffectId.ShouldBe(effectId);
        deserialized.WorkStatus.ShouldBe(WorkStatus.Failed);
        deserialized.StoredException.ShouldNotBeNull();
        deserialized.StoredException.ExceptionMessage.ShouldBe("Error occurred");
        deserialized.StoredException.ExceptionStackTrace.ShouldBeNull();
        deserialized.StoredException.ExceptionType.ShouldBe("System.Exception");
    }
}
