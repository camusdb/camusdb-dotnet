﻿
/**
 * This file is part of CamusDB  
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Text.Json.Serialization;

namespace CamusDB.Client;

internal sealed class CamusExecuteDDLResponse
{
    [JsonPropertyName("status")]
    public string? Status { get; set; }    
}