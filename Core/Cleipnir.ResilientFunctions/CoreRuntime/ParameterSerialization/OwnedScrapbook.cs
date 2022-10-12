using Cleipnir.ResilientFunctions.Domain;

namespace Cleipnir.ResilientFunctions.CoreRuntime.ParameterSerialization;

public record OwnedScrapbook(string Owner, RScrapbook Scrapbook, string ScrapbookType);