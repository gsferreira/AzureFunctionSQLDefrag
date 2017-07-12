#load "Database.csx"

public class DefragService
{
    private readonly string _connectionString;
    private readonly int _reorganizeFragmentationThreshold;
    private readonly int _rebuildFragmentationThreshold;
    private readonly TraceWriter _logger;
    
    public DefragService(TraceWriter logger, string connectionString, int reorganizeFragmentationThreshold = 3, int rebuildFragmentationThreshold = 30)
    {
        _connectionString = connectionString;
        _reorganizeFragmentationThreshold = reorganizeFragmentationThreshold;
        _rebuildFragmentationThreshold = rebuildFragmentationThreshold;
        _logger = logger;
    }

    public void Run()
    {
        using (var database = new Database(_connectionString))
        {

            var tables = database.GetTables();

            rebuildIndexs(database, tables);
            reorganizeIndexs(database, tables);
        }
    }

    private void reorganizeIndexs(Database database, IList<string> tables)
    {
        _logger.Info("Reorganizing Indexs...");

        foreach (var table in tables)
        {
            _logger.Info($"Reorganizing Indexs for table '{table}'");
            var indexs = database.GetFragmentedIndexes(table, _reorganizeFragmentationThreshold);

            foreach (var index in indexs)
            {
                _logger.Info($"Reorganizing Indexs for table '{table}', Index '{index}'");

                database.ReorganizeIndex(index, table);
            }
        }
    }

    private void rebuildIndexs(Database database, IList<string> tables)
    {
        _logger.Info("Rebuilding Indexs...");
        foreach (var table in tables)
        {
            _logger.Info($"Rebuilding Indexs for table '{table}'");
            var indexs = database.GetFragmentedIndexes(table, _rebuildFragmentationThreshold);

            foreach (var index in indexs)
            {
                _logger.Info($"Rebuilding Indexs for table '{table}', Index '{index}'");
                database.RebuildIndex(index, table);
            }
        }
    }

}
