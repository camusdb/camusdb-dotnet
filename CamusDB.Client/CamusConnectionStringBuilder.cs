﻿
/**
 * This file is part of CamusDB  
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Client;

/// <summary>
/// Represents a connection builder class
/// </summary>
public class CamusConnectionStringBuilder
{
    public SessionPoolManager? SessionPoolManager { get; set; }

    public Dictionary<string, string> Config { get; } = new();

    public CamusConnectionStringBuilder(string connectionString)
    {
        if (string.IsNullOrWhiteSpace(connectionString))
            return;

        string[] settings = connectionString.Split(";");        

        foreach (string setting in settings)
        {
            string[] varParts = setting.Split("=");            
            if (varParts.Length != 2)
                continue;

            Config.TryAdd(varParts[0], varParts[1]);
        }
    }
}


