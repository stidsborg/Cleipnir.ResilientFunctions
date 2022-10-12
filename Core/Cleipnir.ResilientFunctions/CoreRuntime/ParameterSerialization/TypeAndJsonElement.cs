using System.Text.Json;

namespace Cleipnir.ResilientFunctions.CoreRuntime.ParameterSerialization;

public record TypeAndJsonElement(string Type, JsonElement Json);