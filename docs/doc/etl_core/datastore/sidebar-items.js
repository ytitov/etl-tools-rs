initSidebarItems({"enum":[["DataOutputMessage",""],["DataSourceMessage",""],["LineType",""],["ReadContentOptions",""],["WriteContentOptions",""]],"fn":[["data_output_shutdown","the purpose of this is to drop the sender, then wait on the receiver to finish. the drop signals that it should finish, and waiting on the JoinHandle is to allow it to finish its own work.  Of course if there are copies of the sender floating around somewhere, this may cause some issues.  This is only used in the job handler, so might be a good idea to just add it to the trait"]],"mod":[["enumerate","creates generated data sources"],["error",""],["fs","Local file system data stores"],["mock","various data stores used for testing"],["transform_store",""]],"struct":[["CsvReadOptions",""],["CsvWriteOptions",""],["DataOutputStats",""],["DataSourceStats",""],["DynDataSource",""]],"trait":[["CreateDataOutput","Helps with creating DataOutput’s during run-time."],["CreateDataSource","Helps with creating DataSource’s during run-time."],["DataOutput",""],["DataSource",""],["SimpleStore","This is a simple store that acts like a key-val storage.  It is not streamted so is not meant for big files.  Primarily created for the JobRunner to store the state of the running job somewhere"]],"type":[["DataOutputItemResult",""],["DataOutputJoinHandle",""],["DataOutputTask",""],["DataOutputTx",""],["DataSourceRx",""],["DataSourceTask",""]]});