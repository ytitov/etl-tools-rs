(function() {var implementors = {};
implementors["custom_job_state"] = [{"text":"impl <a class=\"trait\" href=\"https://doc.rust-lang.org/1.54.0/core/marker/trait.Freeze.html\" title=\"trait core::marker::Freeze\">Freeze</a> for <a class=\"struct\" href=\"custom_job_state/struct.Args.html\" title=\"struct custom_job_state::Args\">Args</a>","synthetic":true,"types":["custom_job_state::Args"]},{"text":"impl <a class=\"trait\" href=\"https://doc.rust-lang.org/1.54.0/core/marker/trait.Freeze.html\" title=\"trait core::marker::Freeze\">Freeze</a> for <a class=\"struct\" href=\"custom_job_state/struct.TestSourceData.html\" title=\"struct custom_job_state::TestSourceData\">TestSourceData</a>","synthetic":true,"types":["custom_job_state::TestSourceData"]},{"text":"impl <a class=\"trait\" href=\"https://doc.rust-lang.org/1.54.0/core/marker/trait.Freeze.html\" title=\"trait core::marker::Freeze\">Freeze</a> for <a class=\"struct\" href=\"custom_job_state/struct.Info.html\" title=\"struct custom_job_state::Info\">Info</a>","synthetic":true,"types":["custom_job_state::Info"]},{"text":"impl <a class=\"trait\" href=\"https://doc.rust-lang.org/1.54.0/core/marker/trait.Freeze.html\" title=\"trait core::marker::Freeze\">Freeze</a> for <a class=\"struct\" href=\"custom_job_state/struct.TestJob.html\" title=\"struct custom_job_state::TestJob\">TestJob</a>","synthetic":true,"types":["custom_job_state::TestJob"]},{"text":"impl <a class=\"trait\" href=\"https://doc.rust-lang.org/1.54.0/core/marker/trait.Freeze.html\" title=\"trait core::marker::Freeze\">Freeze</a> for <a class=\"struct\" href=\"custom_job_state/struct.TestJobState.html\" title=\"struct custom_job_state::TestJobState\">TestJobState</a>","synthetic":true,"types":["custom_job_state::TestJobState"]}];
implementors["etl_core"] = [{"text":"impl Freeze for <a class=\"struct\" href=\"etl_core/datastore/fs/struct.LocalFsDataSource.html\" title=\"struct etl_core::datastore::fs::LocalFsDataSource\">LocalFsDataSource</a>","synthetic":true,"types":["etl_core::datastore::fs::LocalFsDataSource"]},{"text":"impl&lt;I, O&gt; Freeze for <a class=\"struct\" href=\"etl_core/datastore/job_runner/struct.JRDataSource.html\" title=\"struct etl_core::datastore::job_runner::JRDataSource\">JRDataSource</a>&lt;I, O&gt;","synthetic":true,"types":["etl_core::datastore::job_runner::JRDataSource"]},{"text":"impl Freeze for <a class=\"struct\" href=\"etl_core/datastore/mock/struct.MockJsonDataOutput.html\" title=\"struct etl_core::datastore::mock::MockJsonDataOutput\">MockJsonDataOutput</a>","synthetic":true,"types":["etl_core::datastore::mock::MockJsonDataOutput"]},{"text":"impl Freeze for <a class=\"struct\" href=\"etl_core/datastore/mock/struct.MockJsonDataSource.html\" title=\"struct etl_core::datastore::mock::MockJsonDataSource\">MockJsonDataSource</a>","synthetic":true,"types":["etl_core::datastore::mock::MockJsonDataSource"]},{"text":"impl !Freeze for <a class=\"enum\" href=\"etl_core/datastore/error/enum.DataStoreError.html\" title=\"enum etl_core::datastore::error::DataStoreError\">DataStoreError</a>","synthetic":true,"types":["etl_core::datastore::error::DataStoreError"]},{"text":"impl Freeze for <a class=\"struct\" href=\"etl_core/datastore/struct.DataSourceStats.html\" title=\"struct etl_core::datastore::DataSourceStats\">DataSourceStats</a>","synthetic":true,"types":["etl_core::datastore::DataSourceStats"]},{"text":"impl&lt;T&gt; Freeze for <a class=\"enum\" href=\"etl_core/datastore/enum.DataSourceMessage.html\" title=\"enum etl_core::datastore::DataSourceMessage\">DataSourceMessage</a>&lt;T&gt; <span class=\"where fmt-newline\">where<br>&nbsp;&nbsp;&nbsp;&nbsp;T: Freeze,&nbsp;</span>","synthetic":true,"types":["etl_core::datastore::DataSourceMessage"]},{"text":"impl&lt;T&gt; Freeze for <a class=\"enum\" href=\"etl_core/datastore/enum.DataOutputMessage.html\" title=\"enum etl_core::datastore::DataOutputMessage\">DataOutputMessage</a>&lt;T&gt; <span class=\"where fmt-newline\">where<br>&nbsp;&nbsp;&nbsp;&nbsp;T: Freeze,&nbsp;</span>","synthetic":true,"types":["etl_core::datastore::DataOutputMessage"]},{"text":"impl Freeze for <a class=\"enum\" href=\"etl_core/datastore/enum.LineType.html\" title=\"enum etl_core::datastore::LineType\">LineType</a>","synthetic":true,"types":["etl_core::datastore::LineType"]},{"text":"impl Freeze for <a class=\"struct\" href=\"etl_core/datastore/struct.CsvReadOptions.html\" title=\"struct etl_core::datastore::CsvReadOptions\">CsvReadOptions</a>","synthetic":true,"types":["etl_core::datastore::CsvReadOptions"]},{"text":"impl Freeze for <a class=\"struct\" href=\"etl_core/datastore/struct.CsvWriteOptions.html\" title=\"struct etl_core::datastore::CsvWriteOptions\">CsvWriteOptions</a>","synthetic":true,"types":["etl_core::datastore::CsvWriteOptions"]},{"text":"impl Freeze for <a class=\"enum\" href=\"etl_core/datastore/enum.ReadContentOptions.html\" title=\"enum etl_core::datastore::ReadContentOptions\">ReadContentOptions</a>","synthetic":true,"types":["etl_core::datastore::ReadContentOptions"]},{"text":"impl Freeze for <a class=\"enum\" href=\"etl_core/datastore/enum.WriteContentOptions.html\" title=\"enum etl_core::datastore::WriteContentOptions\">WriteContentOptions</a>","synthetic":true,"types":["etl_core::datastore::WriteContentOptions"]},{"text":"impl Freeze for <a class=\"struct\" href=\"etl_core/utils/log/struct.LogEntry.html\" title=\"struct etl_core::utils::log::LogEntry\">LogEntry</a>","synthetic":true,"types":["etl_core::utils::log::LogEntry"]},{"text":"impl Freeze for <a class=\"enum\" href=\"etl_core/utils/log/enum.LogEntryType.html\" title=\"enum etl_core::utils::log::LogEntryType\">LogEntryType</a>","synthetic":true,"types":["etl_core::utils::log::LogEntryType"]},{"text":"impl&lt;T&gt; Freeze for <a class=\"struct\" href=\"etl_core/utils/struct.CsvMessage.html\" title=\"struct etl_core::utils::CsvMessage\">CsvMessage</a>&lt;T&gt; <span class=\"where fmt-newline\">where<br>&nbsp;&nbsp;&nbsp;&nbsp;T: Freeze,&nbsp;</span>","synthetic":true,"types":["etl_core::utils::CsvMessage"]},{"text":"impl&lt;'a&gt; Freeze for <a class=\"struct\" href=\"etl_core/job/command/struct.SimpleCommand.html\" title=\"struct etl_core::job::command::SimpleCommand\">SimpleCommand</a>&lt;'a&gt;","synthetic":true,"types":["etl_core::job::command::SimpleCommand"]},{"text":"impl Freeze for <a class=\"enum\" href=\"etl_core/job/command/enum.JobCommandStatus.html\" title=\"enum etl_core::job::command::JobCommandStatus\">JobCommandStatus</a>","synthetic":true,"types":["etl_core::job::command::JobCommandStatus"]},{"text":"impl&lt;T&gt; Freeze for <a class=\"enum\" href=\"etl_core/job/handler/enum.TransformOutput.html\" title=\"enum etl_core::job::handler::TransformOutput\">TransformOutput</a>&lt;T&gt; <span class=\"where fmt-newline\">where<br>&nbsp;&nbsp;&nbsp;&nbsp;T: Freeze,&nbsp;</span>","synthetic":true,"types":["etl_core::job::handler::TransformOutput"]},{"text":"impl Freeze for <a class=\"enum\" href=\"etl_core/job/state/enum.StreamStatus.html\" title=\"enum etl_core::job::state::StreamStatus\">StreamStatus</a>","synthetic":true,"types":["etl_core::job::state::StreamStatus"]},{"text":"impl Freeze for <a class=\"enum\" href=\"etl_core/job/state/enum.FileStatus.html\" title=\"enum etl_core::job::state::FileStatus\">FileStatus</a>","synthetic":true,"types":["etl_core::job::state::FileStatus"]},{"text":"impl Freeze for <a class=\"struct\" href=\"etl_core/job/state/struct.JobStreamsState.html\" title=\"struct etl_core::job::state::JobStreamsState\">JobStreamsState</a>","synthetic":true,"types":["etl_core::job::state::JobStreamsState"]},{"text":"impl Freeze for <a class=\"struct\" href=\"etl_core/job/state/struct.JobState.html\" title=\"struct etl_core::job::state::JobState\">JobState</a>","synthetic":true,"types":["etl_core::job::state::JobState"]},{"text":"impl Freeze for <a class=\"enum\" href=\"etl_core/job/error/enum.JobRunnerError.html\" title=\"enum etl_core::job::error::JobRunnerError\">JobRunnerError</a>","synthetic":true,"types":["etl_core::job::error::JobRunnerError"]},{"text":"impl Freeze for <a class=\"struct\" href=\"etl_core/job/struct.JobRunner.html\" title=\"struct etl_core::job::JobRunner\">JobRunner</a>","synthetic":true,"types":["etl_core::job::JobRunner"]},{"text":"impl Freeze for <a class=\"struct\" href=\"etl_core/job/struct.JobRunnerConfig.html\" title=\"struct etl_core::job::JobRunnerConfig\">JobRunnerConfig</a>","synthetic":true,"types":["etl_core::job::JobRunnerConfig"]},{"text":"impl Freeze for <a class=\"struct\" href=\"etl_core/job/struct.JobItemInfo.html\" title=\"struct etl_core::job::JobItemInfo\">JobItemInfo</a>","synthetic":true,"types":["etl_core::job::JobItemInfo"]},{"text":"impl Freeze for <a class=\"enum\" href=\"etl_core/job/enum.JobRunnerAction.html\" title=\"enum etl_core::job::JobRunnerAction\">JobRunnerAction</a>","synthetic":true,"types":["etl_core::job::JobRunnerAction"]},{"text":"impl Freeze for <a class=\"enum\" href=\"etl_core/job_manager/enum.NotifyJobRunner.html\" title=\"enum etl_core::job_manager::NotifyJobRunner\">NotifyJobRunner</a>","synthetic":true,"types":["etl_core::job_manager::NotifyJobRunner"]},{"text":"impl Freeze for <a class=\"enum\" href=\"etl_core/job_manager/enum.NotifyJobManager.html\" title=\"enum etl_core::job_manager::NotifyJobManager\">NotifyJobManager</a>","synthetic":true,"types":["etl_core::job_manager::NotifyJobManager"]},{"text":"impl Freeze for <a class=\"enum\" href=\"etl_core/job_manager/enum.NotifyJob.html\" title=\"enum etl_core::job_manager::NotifyJob\">NotifyJob</a>","synthetic":true,"types":["etl_core::job_manager::NotifyJob"]},{"text":"impl Freeze for <a class=\"enum\" href=\"etl_core/job_manager/enum.NotifyDataSource.html\" title=\"enum etl_core::job_manager::NotifyDataSource\">NotifyDataSource</a>","synthetic":true,"types":["etl_core::job_manager::NotifyDataSource"]},{"text":"impl Freeze for <a class=\"enum\" href=\"etl_core/job_manager/enum.Message.html\" title=\"enum etl_core::job_manager::Message\">Message</a>","synthetic":true,"types":["etl_core::job_manager::Message"]},{"text":"impl Freeze for <a class=\"struct\" href=\"etl_core/job_manager/struct.JobManagerConfig.html\" title=\"struct etl_core::job_manager::JobManagerConfig\">JobManagerConfig</a>","synthetic":true,"types":["etl_core::job_manager::JobManagerConfig"]},{"text":"impl Freeze for <a class=\"struct\" href=\"etl_core/job_manager/struct.JobManager.html\" title=\"struct etl_core::job_manager::JobManager\">JobManager</a>","synthetic":true,"types":["etl_core::job_manager::JobManager"]},{"text":"impl Freeze for <a class=\"struct\" href=\"etl_core/job_manager/struct.JobManagerChannel.html\" title=\"struct etl_core::job_manager::JobManagerChannel\">JobManagerChannel</a>","synthetic":true,"types":["etl_core::job_manager::JobManagerChannel"]},{"text":"impl&lt;I, O&gt; Freeze for <a class=\"struct\" href=\"etl_core/transformer/struct.Transformer.html\" title=\"struct etl_core::transformer::Transformer\">Transformer</a>&lt;I, O&gt;","synthetic":true,"types":["etl_core::transformer::Transformer"]},{"text":"impl&lt;'a, L, R&gt; Freeze for <a class=\"struct\" href=\"etl_core/joins/struct.LeftJoin.html\" title=\"struct etl_core::joins::LeftJoin\">LeftJoin</a>&lt;'a, L, R&gt;","synthetic":true,"types":["etl_core::joins::LeftJoin"]}];
implementors["etl_mysql"] = [{"text":"impl Freeze for <a class=\"struct\" href=\"etl_mysql/datastore/struct.MySqlDataOutput.html\" title=\"struct etl_mysql::datastore::MySqlDataOutput\">MySqlDataOutput</a>","synthetic":true,"types":["etl_mysql::datastore::MySqlDataOutput"]},{"text":"impl Freeze for <a class=\"enum\" href=\"etl_mysql/datastore/enum.MySqlDataOutputPool.html\" title=\"enum etl_mysql::datastore::MySqlDataOutputPool\">MySqlDataOutputPool</a>","synthetic":true,"types":["etl_mysql::datastore::MySqlDataOutputPool"]},{"text":"impl Freeze for <a class=\"struct\" href=\"etl_mysql/datastore/struct.ExecRowsOutput.html\" title=\"struct etl_mysql::datastore::ExecRowsOutput\">ExecRowsOutput</a>","synthetic":true,"types":["etl_mysql::datastore::ExecRowsOutput"]},{"text":"impl Freeze for <a class=\"struct\" href=\"etl_mysql/datastore/struct.CreateMySqlDataOutput.html\" title=\"struct etl_mysql::datastore::CreateMySqlDataOutput\">CreateMySqlDataOutput</a>","synthetic":true,"types":["etl_mysql::datastore::CreateMySqlDataOutput"]},{"text":"impl Freeze for <a class=\"struct\" href=\"etl_mysql/struct.CreatePoolParams.html\" title=\"struct etl_mysql::CreatePoolParams\">CreatePoolParams</a>","synthetic":true,"types":["etl_mysql::CreatePoolParams"]}];
implementors["etl_s3"] = [{"text":"impl Freeze for <a class=\"struct\" href=\"etl_s3/datastore/struct.S3DataSource.html\" title=\"struct etl_s3::datastore::S3DataSource\">S3DataSource</a>","synthetic":true,"types":["etl_s3::datastore::S3DataSource"]},{"text":"impl Freeze for <a class=\"struct\" href=\"etl_s3/datastore/struct.S3DataOutput.html\" title=\"struct etl_s3::datastore::S3DataOutput\">S3DataOutput</a>","synthetic":true,"types":["etl_s3::datastore::S3DataOutput"]}];
implementors["job_pipe"] = [{"text":"impl <a class=\"trait\" href=\"https://doc.rust-lang.org/1.54.0/core/marker/trait.Freeze.html\" title=\"trait core::marker::Freeze\">Freeze</a> for <a class=\"struct\" href=\"job_pipe/struct.Args.html\" title=\"struct job_pipe::Args\">Args</a>","synthetic":true,"types":["job_pipe::Args"]},{"text":"impl <a class=\"trait\" href=\"https://doc.rust-lang.org/1.54.0/core/marker/trait.Freeze.html\" title=\"trait core::marker::Freeze\">Freeze</a> for <a class=\"struct\" href=\"job_pipe/struct.TestSourceData.html\" title=\"struct job_pipe::TestSourceData\">TestSourceData</a>","synthetic":true,"types":["job_pipe::TestSourceData"]},{"text":"impl <a class=\"trait\" href=\"https://doc.rust-lang.org/1.54.0/core/marker/trait.Freeze.html\" title=\"trait core::marker::Freeze\">Freeze</a> for <a class=\"struct\" href=\"job_pipe/struct.Info.html\" title=\"struct job_pipe::Info\">Info</a>","synthetic":true,"types":["job_pipe::Info"]},{"text":"impl <a class=\"trait\" href=\"https://doc.rust-lang.org/1.54.0/core/marker/trait.Freeze.html\" title=\"trait core::marker::Freeze\">Freeze</a> for <a class=\"struct\" href=\"job_pipe/struct.TestJobState.html\" title=\"struct job_pipe::TestJobState\">TestJobState</a>","synthetic":true,"types":["job_pipe::TestJobState"]}];
implementors["mysql"] = [{"text":"impl <a class=\"trait\" href=\"https://doc.rust-lang.org/1.54.0/core/marker/trait.Freeze.html\" title=\"trait core::marker::Freeze\">Freeze</a> for <a class=\"struct\" href=\"mysql/struct.Args.html\" title=\"struct mysql::Args\">Args</a>","synthetic":true,"types":["mysql::Args"]},{"text":"impl <a class=\"trait\" href=\"https://doc.rust-lang.org/1.54.0/core/marker/trait.Freeze.html\" title=\"trait core::marker::Freeze\">Freeze</a> for <a class=\"struct\" href=\"mysql/struct.Config.html\" title=\"struct mysql::Config\">Config</a>","synthetic":true,"types":["mysql::Config"]},{"text":"impl <a class=\"trait\" href=\"https://doc.rust-lang.org/1.54.0/core/marker/trait.Freeze.html\" title=\"trait core::marker::Freeze\">Freeze</a> for <a class=\"struct\" href=\"mysql/struct.Info.html\" title=\"struct mysql::Info\">Info</a>","synthetic":true,"types":["mysql::Info"]},{"text":"impl <a class=\"trait\" href=\"https://doc.rust-lang.org/1.54.0/core/marker/trait.Freeze.html\" title=\"trait core::marker::Freeze\">Freeze</a> for <a class=\"struct\" href=\"mysql/struct.TestJob.html\" title=\"struct mysql::TestJob\">TestJob</a>","synthetic":true,"types":["mysql::TestJob"]}];
implementors["s3"] = [{"text":"impl <a class=\"trait\" href=\"https://doc.rust-lang.org/1.54.0/core/marker/trait.Freeze.html\" title=\"trait core::marker::Freeze\">Freeze</a> for <a class=\"struct\" href=\"s3/struct.Args.html\" title=\"struct s3::Args\">Args</a>","synthetic":true,"types":["s3::Args"]},{"text":"impl <a class=\"trait\" href=\"https://doc.rust-lang.org/1.54.0/core/marker/trait.Freeze.html\" title=\"trait core::marker::Freeze\">Freeze</a> for <a class=\"struct\" href=\"s3/struct.Config.html\" title=\"struct s3::Config\">Config</a>","synthetic":true,"types":["s3::Config"]},{"text":"impl <a class=\"trait\" href=\"https://doc.rust-lang.org/1.54.0/core/marker/trait.Freeze.html\" title=\"trait core::marker::Freeze\">Freeze</a> for <a class=\"struct\" href=\"s3/struct.Info.html\" title=\"struct s3::Info\">Info</a>","synthetic":true,"types":["s3::Info"]},{"text":"impl <a class=\"trait\" href=\"https://doc.rust-lang.org/1.54.0/core/marker/trait.Freeze.html\" title=\"trait core::marker::Freeze\">Freeze</a> for <a class=\"struct\" href=\"s3/struct.TestJob.html\" title=\"struct s3::TestJob\">TestJob</a>","synthetic":true,"types":["s3::TestJob"]},{"text":"impl <a class=\"trait\" href=\"https://doc.rust-lang.org/1.54.0/core/marker/trait.Freeze.html\" title=\"trait core::marker::Freeze\">Freeze</a> for <a class=\"struct\" href=\"s3/struct.TestJob2.html\" title=\"struct s3::TestJob2\">TestJob2</a>","synthetic":true,"types":["s3::TestJob2"]}];
implementors["simple_join"] = [{"text":"impl <a class=\"trait\" href=\"https://doc.rust-lang.org/1.54.0/core/marker/trait.Freeze.html\" title=\"trait core::marker::Freeze\">Freeze</a> for <a class=\"struct\" href=\"simple_join/struct.Args.html\" title=\"struct simple_join::Args\">Args</a>","synthetic":true,"types":["simple_join::Args"]},{"text":"impl <a class=\"trait\" href=\"https://doc.rust-lang.org/1.54.0/core/marker/trait.Freeze.html\" title=\"trait core::marker::Freeze\">Freeze</a> for <a class=\"struct\" href=\"simple_join/struct.LeftTestData.html\" title=\"struct simple_join::LeftTestData\">LeftTestData</a>","synthetic":true,"types":["simple_join::LeftTestData"]},{"text":"impl <a class=\"trait\" href=\"https://doc.rust-lang.org/1.54.0/core/marker/trait.Freeze.html\" title=\"trait core::marker::Freeze\">Freeze</a> for <a class=\"struct\" href=\"simple_join/struct.RightTestData.html\" title=\"struct simple_join::RightTestData\">RightTestData</a>","synthetic":true,"types":["simple_join::RightTestData"]},{"text":"impl <a class=\"trait\" href=\"https://doc.rust-lang.org/1.54.0/core/marker/trait.Freeze.html\" title=\"trait core::marker::Freeze\">Freeze</a> for <a class=\"struct\" href=\"simple_join/struct.TestOutputData.html\" title=\"struct simple_join::TestOutputData\">TestOutputData</a>","synthetic":true,"types":["simple_join::TestOutputData"]}];
implementors["simple_pipeline"] = [{"text":"impl <a class=\"trait\" href=\"https://doc.rust-lang.org/1.54.0/core/marker/trait.Freeze.html\" title=\"trait core::marker::Freeze\">Freeze</a> for <a class=\"struct\" href=\"simple_pipeline/struct.Args.html\" title=\"struct simple_pipeline::Args\">Args</a>","synthetic":true,"types":["simple_pipeline::Args"]},{"text":"impl <a class=\"trait\" href=\"https://doc.rust-lang.org/1.54.0/core/marker/trait.Freeze.html\" title=\"trait core::marker::Freeze\">Freeze</a> for <a class=\"struct\" href=\"simple_pipeline/struct.TestSourceData.html\" title=\"struct simple_pipeline::TestSourceData\">TestSourceData</a>","synthetic":true,"types":["simple_pipeline::TestSourceData"]},{"text":"impl <a class=\"trait\" href=\"https://doc.rust-lang.org/1.54.0/core/marker/trait.Freeze.html\" title=\"trait core::marker::Freeze\">Freeze</a> for <a class=\"struct\" href=\"simple_pipeline/struct.TestOutputData.html\" title=\"struct simple_pipeline::TestOutputData\">TestOutputData</a>","synthetic":true,"types":["simple_pipeline::TestOutputData"]},{"text":"impl <a class=\"trait\" href=\"https://doc.rust-lang.org/1.54.0/core/marker/trait.Freeze.html\" title=\"trait core::marker::Freeze\">Freeze</a> for <a class=\"struct\" href=\"simple_pipeline/struct.TestJobState.html\" title=\"struct simple_pipeline::TestJobState\">TestJobState</a>","synthetic":true,"types":["simple_pipeline::TestJobState"]},{"text":"impl <a class=\"trait\" href=\"https://doc.rust-lang.org/1.54.0/core/marker/trait.Freeze.html\" title=\"trait core::marker::Freeze\">Freeze</a> for <a class=\"struct\" href=\"simple_pipeline/struct.TestTransformer.html\" title=\"struct simple_pipeline::TestTransformer\">TestTransformer</a>","synthetic":true,"types":["simple_pipeline::TestTransformer"]}];
implementors["test_connection"] = [{"text":"impl <a class=\"trait\" href=\"https://doc.rust-lang.org/1.54.0/core/marker/trait.Freeze.html\" title=\"trait core::marker::Freeze\">Freeze</a> for <a class=\"struct\" href=\"test_connection/struct.Args.html\" title=\"struct test_connection::Args\">Args</a>","synthetic":true,"types":["test_connection::Args"]},{"text":"impl <a class=\"trait\" href=\"https://doc.rust-lang.org/1.54.0/core/marker/trait.Freeze.html\" title=\"trait core::marker::Freeze\">Freeze</a> for <a class=\"struct\" href=\"test_connection/struct.Config.html\" title=\"struct test_connection::Config\">Config</a>","synthetic":true,"types":["test_connection::Config"]}];
implementors["transform"] = [{"text":"impl <a class=\"trait\" href=\"https://doc.rust-lang.org/1.54.0/core/marker/trait.Freeze.html\" title=\"trait core::marker::Freeze\">Freeze</a> for <a class=\"struct\" href=\"transform/struct.Args.html\" title=\"struct transform::Args\">Args</a>","synthetic":true,"types":["transform::Args"]},{"text":"impl <a class=\"trait\" href=\"https://doc.rust-lang.org/1.54.0/core/marker/trait.Freeze.html\" title=\"trait core::marker::Freeze\">Freeze</a> for <a class=\"struct\" href=\"transform/struct.TestSourceData.html\" title=\"struct transform::TestSourceData\">TestSourceData</a>","synthetic":true,"types":["transform::TestSourceData"]},{"text":"impl <a class=\"trait\" href=\"https://doc.rust-lang.org/1.54.0/core/marker/trait.Freeze.html\" title=\"trait core::marker::Freeze\">Freeze</a> for <a class=\"struct\" href=\"transform/struct.Info.html\" title=\"struct transform::Info\">Info</a>","synthetic":true,"types":["transform::Info"]},{"text":"impl <a class=\"trait\" href=\"https://doc.rust-lang.org/1.54.0/core/marker/trait.Freeze.html\" title=\"trait core::marker::Freeze\">Freeze</a> for <a class=\"struct\" href=\"transform/struct.TestJob.html\" title=\"struct transform::TestJob\">TestJob</a>","synthetic":true,"types":["transform::TestJob"]},{"text":"impl <a class=\"trait\" href=\"https://doc.rust-lang.org/1.54.0/core/marker/trait.Freeze.html\" title=\"trait core::marker::Freeze\">Freeze</a> for <a class=\"struct\" href=\"transform/struct.TestJobState.html\" title=\"struct transform::TestJobState\">TestJobState</a>","synthetic":true,"types":["transform::TestJobState"]}];
if (window.register_implementors) {window.register_implementors(implementors);} else {window.pending_implementors = implementors;}})()