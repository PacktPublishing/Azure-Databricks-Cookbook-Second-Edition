# Reminder - this code should be executed in individual cells of Azure Databricks Notebooks

# Strategy 1: Implementing asynchronous state checkpointing

# Open your Azure Databricks environment and navigate to the notebook you’re working in. Enter the following commands to enable asynchronous state checkpointing
spark.conf.set("spark.databricks.streaming.statefulOperator.asyncCheckpoint.enabled", "true") spark.conf.set("spark.Sql.streaming.stateStore.providerClass", "com.databricks.Sql.streaming.state.RocksDBStateStoreProvider")

# Strategy 2: Streamlining with optimization recommendations

# Determine the optimal number of shuffle partitions
spark.conf.set("spark.Sql.shuffle.partitions", [appropriate_number]) 

# Reduce unnecessary processing by disabling empty micro-batches 
spark.conf.set("spark.Sql.streaming.noDataMicroBatches.enabled ", "false")

# Strategy 3: Leveraging RocksDB with changelog checkpointing

# check if RocksDB is defined as a state store provider
spark.conf.get("spark.Sql.streaming.stateStore.providerClass")

# enable changelog checkpointing
spark.conf.set("spark.sql.streaming.stateStore.rocksdb.changelogCheckpointing.enabled", "true")

