/// create the following tests
/// TODO: 1. run parallel jobs with one failing

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn run_two_jobs_fail_one() {
    panic!("not implemented");
}
