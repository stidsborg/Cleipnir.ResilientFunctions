using Cleipnir.ResilientFunctions.Domain;

namespace Cleipnir.ResilientFunctions.ParameterSerialization;

public record OwnedScrapbook(string Owner, RScrapbook Scrapbook, string ScrapbookType);