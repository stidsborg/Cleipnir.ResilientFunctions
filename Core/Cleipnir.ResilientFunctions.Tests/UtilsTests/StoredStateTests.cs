using System.Linq;
using System.Text;
using Cleipnir.ResilientFunctions.Storage;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.Tests.UtilsTests;

[TestClass]
public class StoredStateTests
{
    [TestMethod]
    public void StoredStateEntity_ParamType_SerializesAndDeserializesCorrectly()
    {
        // Arrange
        var entity = new StoredStateEntity(
            StoredStateType.Param,
            "param1",
            Encoding.UTF8.GetBytes("test parameter content"),
            Deleted: false
        );

        // Act
        var serialized = entity.Serialize();
        var deserialized = StoredStateEntity.Deserialize(serialized);

        // Assert
        Assert.AreEqual(entity.Type, deserialized.Type);
        Assert.AreEqual(entity.Id, deserialized.Id);
        CollectionAssert.AreEqual(entity.Content, deserialized.Content);
        Assert.AreEqual(entity.Deleted, deserialized.Deleted);
    }

    [TestMethod]
    public void StoredStateEntity_InstanceType_SerializesAndDeserializesCorrectly()
    {
        // Arrange
        var entity = new StoredStateEntity(
            StoredStateType.Instance,
            "instance1",
            Encoding.UTF8.GetBytes("test instance content"),
            Deleted: false
        );

        // Act
        var serialized = entity.Serialize();
        var deserialized = StoredStateEntity.Deserialize(serialized);

        // Assert
        Assert.AreEqual(entity.Type, deserialized.Type);
        Assert.AreEqual(entity.Id, deserialized.Id);
        CollectionAssert.AreEqual(entity.Content, deserialized.Content);
        Assert.AreEqual(entity.Deleted, deserialized.Deleted);
    }

    [TestMethod]
    public void StoredStateEntity_EffectType_SerializesAndDeserializesCorrectly()
    {
        // Arrange
        var entity = new StoredStateEntity(
            StoredStateType.Effect,
            "effect1",
            Encoding.UTF8.GetBytes("test effect content"),
            Deleted: false
        );

        // Act
        var serialized = entity.Serialize();
        var deserialized = StoredStateEntity.Deserialize(serialized);

        // Assert
        Assert.AreEqual(entity.Type, deserialized.Type);
        Assert.AreEqual(entity.Id, deserialized.Id);
        CollectionAssert.AreEqual(entity.Content, deserialized.Content);
        Assert.AreEqual(entity.Deleted, deserialized.Deleted);
    }

    [TestMethod]
    public void StoredStateEntity_MessageType_SerializesAndDeserializesCorrectly()
    {
        // Arrange
        var entity = new StoredStateEntity(
            StoredStateType.Message,
            "message1",
            Encoding.UTF8.GetBytes("test message content"),
            Deleted: false
        );

        // Act
        var serialized = entity.Serialize();
        var deserialized = StoredStateEntity.Deserialize(serialized);

        // Assert
        Assert.AreEqual(entity.Type, deserialized.Type);
        Assert.AreEqual(entity.Id, deserialized.Id);
        CollectionAssert.AreEqual(entity.Content, deserialized.Content);
        Assert.AreEqual(entity.Deleted, deserialized.Deleted);
    }

    [TestMethod]
    public void StoredStateEntity_WithDeletedFlag_SerializesAndDeserializesCorrectly()
    {
        // Arrange
        var entity = new StoredStateEntity(
            StoredStateType.Effect,
            "deletedEffect",
            Encoding.UTF8.GetBytes("deleted content"),
            Deleted: true
        );

        // Act
        var serialized = entity.Serialize();
        var deserialized = StoredStateEntity.Deserialize(serialized);

        // Assert
        Assert.AreEqual(entity.Type, deserialized.Type);
        Assert.AreEqual(entity.Id, deserialized.Id);
        CollectionAssert.AreEqual(entity.Content, deserialized.Content);
        Assert.AreEqual(entity.Deleted, deserialized.Deleted);
        Assert.IsTrue(deserialized.Deleted);
    }

    [TestMethod]
    public void StoredStateEntity_WithEmptyContent_SerializesAndDeserializesCorrectly()
    {
        // Arrange
        var entity = new StoredStateEntity(
            StoredStateType.Param,
            "emptyParam",
            [],
            Deleted: false
        );

        // Act
        var serialized = entity.Serialize();
        var deserialized = StoredStateEntity.Deserialize(serialized);

        // Assert
        Assert.AreEqual(entity.Type, deserialized.Type);
        Assert.AreEqual(entity.Id, deserialized.Id);
        CollectionAssert.AreEqual(entity.Content, deserialized.Content);
        Assert.IsEmpty(deserialized.Content);
        Assert.AreEqual(entity.Deleted, deserialized.Deleted);
    }

    [TestMethod]
    public void StoredStateEntity_WithSpecialCharactersInId_SerializesAndDeserializesCorrectly()
    {
        // Arrange
        var entity = new StoredStateEntity(
            StoredStateType.Instance,
            "id-with-special_chars.123/test@example.com",
            Encoding.UTF8.GetBytes("content"),
            Deleted: false
        );

        // Act
        var serialized = entity.Serialize();
        var deserialized = StoredStateEntity.Deserialize(serialized);

        // Assert
        Assert.AreEqual(entity.Type, deserialized.Type);
        Assert.AreEqual(entity.Id, deserialized.Id);
        CollectionAssert.AreEqual(entity.Content, deserialized.Content);
        Assert.AreEqual(entity.Deleted, deserialized.Deleted);
    }

    [TestMethod]
    public void StoredStateEntity_WithUnicodeContent_SerializesAndDeserializesCorrectly()
    {
        // Arrange
        var entity = new StoredStateEntity(
            StoredStateType.Message,
            "unicodeMsg",
            Encoding.UTF8.GetBytes("Hello 世界 🌍 Привет"),
            Deleted: false
        );

        // Act
        var serialized = entity.Serialize();
        var deserialized = StoredStateEntity.Deserialize(serialized);

        // Assert
        Assert.AreEqual(entity.Type, deserialized.Type);
        Assert.AreEqual(entity.Id, deserialized.Id);
        CollectionAssert.AreEqual(entity.Content, deserialized.Content);
        Assert.AreEqual("Hello 世界 🌍 Привет", Encoding.UTF8.GetString(deserialized.Content));
        Assert.AreEqual(entity.Deleted, deserialized.Deleted);
    }

    [TestMethod]
    public void StoredStateEntity_WithBinaryContent_SerializesAndDeserializesCorrectly()
    {
        // Arrange
        var binaryContent = new byte[] { 0x00, 0x01, 0xFF, 0xAB, 0xCD, 0xEF };
        var entity = new StoredStateEntity(
            StoredStateType.Effect,
            "binaryEffect",
            binaryContent,
            Deleted: false
        );

        // Act
        var serialized = entity.Serialize();
        var deserialized = StoredStateEntity.Deserialize(serialized);

        // Assert
        Assert.AreEqual(entity.Type, deserialized.Type);
        Assert.AreEqual(entity.Id, deserialized.Id);
        CollectionAssert.AreEqual(entity.Content, deserialized.Content);
        Assert.AreEqual(entity.Deleted, deserialized.Deleted);
    }

    [TestMethod]
    public void StoredStates_WithMultipleEntities_SerializesAndDeserializesCorrectly()
    {
        // Arrange
        var entities = new[]
        {
            new StoredStateEntity(StoredStateType.Param, "param1", Encoding.UTF8.GetBytes("param content"), Deleted: false),
            new StoredStateEntity(StoredStateType.Instance, "instance1", Encoding.UTF8.GetBytes("instance content"), Deleted: false),
            new StoredStateEntity(StoredStateType.Effect, "effect1", Encoding.UTF8.GetBytes("effect content"), Deleted: true),
            new StoredStateEntity(StoredStateType.Message, "message1", Encoding.UTF8.GetBytes("message content"), Deleted: false)
        };
        var storedStates = new StoredStates(entities);

        // Act
        var serialized = storedStates.Serialize();
        var deserialized = StoredStates.Deserialize(serialized);

        // Assert
        Assert.HasCount(entities.Length, deserialized);
        for (var i = 0; i < entities.Length; i++)
        {
            Assert.AreEqual(entities[i].Type, deserialized[i].Type);
            Assert.AreEqual(entities[i].Id, deserialized[i].Id);
            CollectionAssert.AreEqual(entities[i].Content, deserialized[i].Content);
            Assert.AreEqual(entities[i].Deleted, deserialized[i].Deleted);
        }
    }

    [TestMethod]
    public void StoredStates_WithSingleEntity_SerializesAndDeserializesCorrectly()
    {
        // Arrange
        var entities = new[]
        {
            new StoredStateEntity(StoredStateType.Param, "param1", Encoding.UTF8.GetBytes("content"), Deleted: false)
        };
        var storedStates = new StoredStates(entities);

        // Act
        var serialized = storedStates.Serialize();
        var deserialized = StoredStates.Deserialize(serialized);

        // Assert
        Assert.HasCount(1, deserialized);
        Assert.AreEqual(entities[0].Type, deserialized[0].Type);
        Assert.AreEqual(entities[0].Id, deserialized[0].Id);
        CollectionAssert.AreEqual(entities[0].Content, deserialized[0].Content);
        Assert.AreEqual(entities[0].Deleted, deserialized[0].Deleted);
    }

    [TestMethod]
    public void StoredStates_WithEmptyArray_SerializesAndDeserializesCorrectly()
    {
        // Arrange
        var entities = System.Array.Empty<StoredStateEntity>();
        var storedStates = new StoredStates(entities);

        // Act
        var serialized = storedStates.Serialize();
        var deserialized = StoredStates.Deserialize(serialized);

        // Assert
        Assert.IsEmpty(deserialized);
    }

    [TestMethod]
    public void StoredStates_WithMixedDeletedFlags_SerializesAndDeserializesCorrectly()
    {
        // Arrange
        var entities = new[]
        {
            new StoredStateEntity(StoredStateType.Param, "param1", Encoding.UTF8.GetBytes("content1"), Deleted: false),
            new StoredStateEntity(StoredStateType.Effect, "effect1", Encoding.UTF8.GetBytes("content2"), Deleted: true),
            new StoredStateEntity(StoredStateType.Instance, "instance1", Encoding.UTF8.GetBytes("content3"), Deleted: false),
            new StoredStateEntity(StoredStateType.Message, "message1", Encoding.UTF8.GetBytes("content4"), Deleted: true)
        };
        var storedStates = new StoredStates(entities);

        // Act
        var serialized = storedStates.Serialize();
        var deserialized = StoredStates.Deserialize(serialized);

        // Assert
        Assert.HasCount(entities.Length, deserialized);
        Assert.IsFalse(deserialized[0].Deleted);
        Assert.IsTrue(deserialized[1].Deleted);
        Assert.IsFalse(deserialized[2].Deleted);
        Assert.IsTrue(deserialized[3].Deleted);
    }

    [TestMethod]
    public void StoredStates_WithLargeContent_SerializesAndDeserializesCorrectly()
    {
        // Arrange
        var largeContent = Enumerable.Repeat((byte)0xAA, 10000).ToArray();
        var entities = new[]
        {
            new StoredStateEntity(StoredStateType.Instance, "largeInstance", largeContent, Deleted: false)
        };
        var storedStates = new StoredStates(entities);

        // Act
        var serialized = storedStates.Serialize();
        var deserialized = StoredStates.Deserialize(serialized);

        // Assert
        Assert.HasCount(1, deserialized);
        Assert.HasCount(10000, deserialized[0].Content);
        CollectionAssert.AreEqual(largeContent, deserialized[0].Content);
    }

    [TestMethod]
    public void StoredStates_RoundTrip_PreservesAllData()
    {
        // Arrange
        var entities = new[]
        {
            new StoredStateEntity(StoredStateType.Param, "param1", Encoding.UTF8.GetBytes("param content"), Deleted: false),
            new StoredStateEntity(StoredStateType.Instance, "instance-with-special_chars.123", Encoding.UTF8.GetBytes("instance content with unicode: 世界"), Deleted: false),
            new StoredStateEntity(StoredStateType.Effect, "effect1", new byte[] { 0x00, 0xFF, 0xAB }, Deleted: true),
            new StoredStateEntity(StoredStateType.Message, "message1", [], Deleted: false)
        };
        var originalStates = new StoredStates(entities);

        // Act - perform multiple round trips
        var serialized1 = originalStates.Serialize();
        var deserialized1 = StoredStates.Deserialize(serialized1);
        var reserialized = new StoredStates(deserialized1.ToArray()).Serialize();
        var deserialized2 = StoredStates.Deserialize(reserialized);

        // Assert - data should remain identical after multiple round trips
        Assert.HasCount(entities.Length, deserialized2);
        for (var i = 0; i < entities.Length; i++)
        {
            Assert.AreEqual(entities[i].Type, deserialized2[i].Type);
            Assert.AreEqual(entities[i].Id, deserialized2[i].Id);
            CollectionAssert.AreEqual(entities[i].Content, deserialized2[i].Content);
            Assert.AreEqual(entities[i].Deleted, deserialized2[i].Deleted);
        }
    }
}