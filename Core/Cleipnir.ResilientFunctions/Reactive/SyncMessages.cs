using System;
using System.Threading.Tasks;

namespace Cleipnir.ResilientFunctions.Reactive;

public delegate Task SyncStore(TimeSpan maxSinceLastSynced);