using System.Text.Json.Serialization;

namespace CamusDB.Client;

[JsonSourceGenerationOptions]
[JsonSerializable(typeof(CamusErrorResponse))]
[JsonSerializable(typeof(CamusExecuteDDLRequest))]
[JsonSerializable(typeof(CamusExecuteDDLResponse))]
[JsonSerializable(typeof(CamusExecuteSqlNonQueryRequest))]
[JsonSerializable(typeof(CamusExecuteSqlNonQueryResponse))]
[JsonSerializable(typeof(CamusExecuteSqlQueryRequest))]
[JsonSerializable(typeof(CamusExecuteSqlQueryResponse))]
[JsonSerializable(typeof(CamusInsertRequest))]
[JsonSerializable(typeof(CamusStartTransactionRequest))]
[JsonSerializable(typeof(CamusStartTransactionResponse))]
[JsonSerializable(typeof(CamusTransactionRequest))]
internal sealed partial class CamusJsonSerializerContext : JsonSerializerContext;
