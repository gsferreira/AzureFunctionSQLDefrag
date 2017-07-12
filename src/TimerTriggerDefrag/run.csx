#load ".\Core\DefragService.csx"

using System;
using System.Configuration;

public static void Run(TimerInfo myTimer, TraceWriter log)
{
    log.Info($"Starting: {DateTime.Now}");


    foreach(ConnectionStringSettings connectionString in ConfigurationManager.ConnectionStrings)
    {
        if(connectionString.Name.StartsWith("Defrag."))
        {
            log.Info($"Connection String: {connectionString.Name}");
            

            var defragService = new DefragService(log,
                connectionString.ConnectionString);

            defragService.Run();
        }
    }

    log.Info($"Executed at: {DateTime.Now}");
        
}