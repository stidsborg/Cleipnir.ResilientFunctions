using System.Text.Json;

namespace Cleipnir.ResilientFunctions.CoreRuntime.Serialization;

public record TypeAndJsonElement(string Type, JsonElement Json);