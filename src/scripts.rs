use std::sync::LazyLock;

use redis::Script;

static DROP_CONSUMER_JOBS_SRC: &str = include_str!("../scripts/drop_consumer_jobs.lua");
pub static DROP_CONSUMER_JOBS: LazyLock<Script> =
    LazyLock::new(|| Script::new(DROP_CONSUMER_JOBS_SRC));

static JOB_SEARCH_SRC: &str = include_str!("../scripts/job_search.lua");
pub static JOB_SEARCH: LazyLock<Script> = LazyLock::new(|| Script::new(JOB_SEARCH_SRC));

static JOB_SEARCH_GROUP_SRC: &str = include_str!("../scripts/job_search_group.lua");
pub static JOB_SEARCH_GROUP: LazyLock<Script> = LazyLock::new(|| Script::new(JOB_SEARCH_GROUP_SRC));
