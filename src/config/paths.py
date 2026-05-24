import os


DBConfigFile = os.getenv("TASKSHIFT_DB_CONFIG_FILE", "configs/.env")
schedulerConfigFile = os.getenv("TASKSHIFT_SCHEDULER_CONFIG_FILE", "configs/scheduler.yaml")
serverConfigFile = os.getenv("TASKSHIFT_SERVER_CONFIG_FILE", "configs/server.yaml")
clusterConfigFile = os.getenv("TASKSHIFT_CLUSTER_CONFIG_FILE", "configs/cluster.yaml")
clusterConfigBackupRoot = os.getenv("TASKSHIFT_CLUSTER_CONFIG_BACKUP_ROOT", "configs/cluster_backups")
academicCalendarRoot = os.getenv("TASKSHIFT_ACADEMIC_CALENDAR_ROOT", "configs/calendar")
