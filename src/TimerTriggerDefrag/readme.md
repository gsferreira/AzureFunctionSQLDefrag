The function will run and execute a REBUILD and REORGANIZE for Database Indexs, with a given Threshold.

### Add a database to process

Add a Connection String to the Application Settings where the name has the prefix "Defrag.".


### Update the Threshold

Update the *run.csx* file to invoke the DefragService with threshold for reorganize and for rebuild.

Example:

```csharp    
    var defragService = new DefragService(log,
                connectionString.ConnectionString,
                5, //Reorganize Indexs with more than 5% of fragmentation
                10 //Rebuild Indexs with more than 10% of fragmentation
                );
```    

### Update the schedule

Go to the *function.json* and update the cron expression of the *schedule* attribute.
By default, the function is defined to run on Sundays at 00:00:00
