using System.Text.Json.Serialization;

namespace CamusDB.Client;

// A response can carry jsonb documents as native JSON, nested up to CamusJsonb.MaxNestingDepth levels.
// A float parameter can be ±infinity or NaN, which JSON has no number for: named literals write it as
// "Infinity", "-Infinity" or "NaN", the form the server reads in a float parameter. Without them the
// request failed while it was written. Finite numbers are unchanged.
[JsonSourceGenerationOptions(MaxDepth = CamusJsonb.ResponseMaxDepth, NumberHandling = JsonNumberHandling.AllowNamedFloatingPointLiterals)]
[JsonSerializable(typeof(ColumnValue))]
[JsonSerializable(typeof(CamusColumnSchema))]
[JsonSerializable(typeof(CamusErrorResponse))]
[JsonSerializable(typeof(CamusCreateDatabaseRequest))]
[JsonSerializable(typeof(CamusCreateDatabaseResponse))]
[JsonSerializable(typeof(CamusDropDatabaseRequest))]
[JsonSerializable(typeof(CamusDropDatabaseResponse))]
[JsonSerializable(typeof(CamusExecuteDDLRequest))]
[JsonSerializable(typeof(CamusExecuteDDLResponse))]
[JsonSerializable(typeof(CamusExecuteSqlNonQueryRequest))]
[JsonSerializable(typeof(CamusExecuteSqlNonQueryResponse))]
[JsonSerializable(typeof(CamusExecuteSqlQueryRequest))]
[JsonSerializable(typeof(CamusRoutingMetadataResponse))]
[JsonSerializable(typeof(CamusExecuteSqlQueryResponse))]
[JsonSerializable(typeof(CamusPrepareStatementRequest))]
[JsonSerializable(typeof(CamusPrepareStatementResponse))]
[JsonSerializable(typeof(CamusCloseStatementRequest))]
[JsonSerializable(typeof(CamusCloseStatementResponse))]
[JsonSerializable(typeof(CamusHlcTimestamp))]
[JsonSerializable(typeof(CamusInsertRequest))]
[JsonSerializable(typeof(CamusLoginRequest))]
[JsonSerializable(typeof(CamusLoginResponse))]
[JsonSerializable(typeof(CamusStartTransactionRequest))]
[JsonSerializable(typeof(CamusStartTransactionResponse))]
[JsonSerializable(typeof(CamusTransactionRequest))]
[JsonSerializable(typeof(CamusBranchRow))]
[JsonSerializable(typeof(CamusBackupInfo))]
[JsonSerializable(typeof(CamusTakeBackupRequest))]
[JsonSerializable(typeof(CamusBackupResponse))]
[JsonSerializable(typeof(CamusBackupListResponse))]
[JsonSerializable(typeof(CamusBackupGcResponse))]
[JsonSerializable(typeof(CamusBackupGcDeletion))]
[JsonSerializable(typeof(CamusBackupGcOrphan))]
[JsonSerializable(typeof(CamusNodeStatsResponse))]
internal sealed partial class CamusJsonSerializerContext : JsonSerializerContext;
