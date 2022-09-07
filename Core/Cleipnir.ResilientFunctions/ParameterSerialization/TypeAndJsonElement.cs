using System.Text.Json;

namespace Cleipnir.ResilientFunctions.ParameterSerialization;

public record TypeAndJsonElement(string Type, JsonElement Json);