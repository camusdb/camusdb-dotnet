﻿
/**
 * This file is part of CamusDB  
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Text.Json.Serialization;

namespace CamusDB.Client;

public sealed class CamusExecuteSqlQueryRequest
{
    [JsonPropertyName("txnIdPT")]
    public long TxnIdPT { get; set; }

    [JsonPropertyName("txnIdCounter")]
    public uint TxnIdCounter { get; set; }

    [JsonPropertyName("databaseName")]
    public string? DatabaseName { get; set; }

    [JsonPropertyName("sql")]
    public string? Sql { get; set; }

    [JsonPropertyName("parameters")]
    public Dictionary<string, ColumnValue>? Parameters { get; set; }
}

