(function() {var implementors = {};
implementors["etl_aws_utils"] = [{"text":"impl <a class=\"trait\" href=\"etl_core/datastore/trait.DataSource.html\" title=\"trait etl_core::datastore::DataSource\">DataSource</a>&lt;Bytes&gt; for <a class=\"struct\" href=\"etl_aws_utils/s3_datastore/struct.S3Storage.html\" title=\"struct etl_aws_utils::s3_datastore::S3Storage\">S3Storage</a>","synthetic":false,"types":["etl_aws_utils::s3_datastore::S3Storage"]}];
implementors["etl_core"] = [];
implementors["etl_mysql"] = [{"text":"impl&lt;T:&nbsp;<a class=\"trait\" href=\"https://doc.rust-lang.org/1.58.1/core/fmt/trait.Debug.html\" title=\"trait core::fmt::Debug\">Debug</a> + 'static + <a class=\"trait\" href=\"https://doc.rust-lang.org/1.58.1/core/marker/trait.Send.html\" title=\"trait core::marker::Send\">Send</a> + <a class=\"trait\" href=\"https://doc.rust-lang.org/1.58.1/core/marker/trait.Sync.html\" title=\"trait core::marker::Sync\">Sync</a> + <a class=\"trait\" href=\"https://doc.rust-lang.org/1.58.1/core/marker/trait.Unpin.html\" title=\"trait core::marker::Unpin\">Unpin</a>&gt; <a class=\"trait\" href=\"etl_core/datastore/trait.DataSource.html\" title=\"trait etl_core::datastore::DataSource\">DataSource</a>&lt;T&gt; for <a class=\"struct\" href=\"etl_mysql/datastore/struct.MySqlSelect.html\" title=\"struct etl_mysql::datastore::MySqlSelect\">MySqlSelect</a>&lt;T&gt; <span class=\"where fmt-newline\">where<br>&nbsp;&nbsp;&nbsp;&nbsp;for&lt;'a&gt; T: FromRow&lt;'a, MySqlRow&gt;,&nbsp;</span>","synthetic":false,"types":["etl_mysql::datastore::MySqlSelect"]}];
if (window.register_implementors) {window.register_implementors(implementors);} else {window.pending_implementors = implementors;}})()