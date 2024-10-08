airflow_table_config = [
    {
      "table_name": "dag",
      "columns": [
        "dag_id",
        "is_paused",
        "is_subdag",
        "is_active",
        "last_scheduler_run",
        "last_pickled",
        "last_expired",
        "scheduler_lock",
        "pickle_id",
        "fileloc",
        "owners"
      ],
      "formatter": '"%s", %s, %s, %s, "%s", "%s", "%s", %s, %s, "%s", "%s"'
    },
    {
      "table_name": "dag_run",
      "columns": [
        "dag_id",
        "execution_date",
        "state",
        "run_id",
        "external_trigger",
        "end_date",
        "start_date"
      ],
      "formatter": '"%s", "%s", "%s", "%s", %s, "%s", "%s"'
    },
    {
      "table_name": "sla_miss",
      "columns": [
        "task_id",
        "dag_id",
        "execution_date",
        "email_sent",
        "timestamp",
        "description",
        "notification_sent"
      ],
      "formatter": '"%s", "%s", "%s", %s, "%s", "%s", %s'
    },
    {
      "table_name": "log",
      "columns": [
        "dttm",
        "dag_id",
        "task_id",
        "event",
        "execution_date",
        "owner",
        "extra"
      ],
      "formatter": '"%s", "%s", "%s", "%s", "%s", "%s", "%s"'
    },
    {
      "table_name": "task_instance",
      "columns": [
        "task_id",
        "dag_id",
        "execution_date",
        "start_date",
        "end_date",
        "duration",
        "state",
        "try_number",
        "hostname",
        "unixname",
        "job_id",
        "pool",
        "queue",
        "priority_weight",
        "operator",
        "queued_dttm"
      ],
      "formatter": '"%s", "%s", "%s", "%s", "%s", %s, "%s", %s, "%s", "%s", %s, "%s", "%s", %s, "%s", "%s"'
    },
    {
      "table_name": "dag_stats",
      "columns": [
        "dag_id",
        "state",
        "count",
        "dirty"
      ],
      "formatter": '"%s", "%s", %s, %s'
    },
    {
      "table_name": "job",
      "columns": [
        "dag_id",
        "state",
        "job_type",
        "start_date",
        "end_date",
        "latest_heartbeat",
        "executor_class",
        "hostname",
        "unixname"
      ],
      "formatter": '"%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s"'
    }
  ]
