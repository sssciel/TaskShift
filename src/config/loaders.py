from datetime import datetime
from pathlib import Path
import shutil

from .calendar import AcademicCalendarConfig, ConferenceCalendarConfig
from .models import AdminPanelAccessConfig, ClusterConfig, DBConfig, SchedulerConfig, ServerConfig
from .paths import (
    DBConfigFile,
    academicCalendarRoot,
    clusterConfigBackupRoot,
    clusterConfigFile,
    schedulerConfigFile,
    serverConfigFile,
)


def getDBConfig():
    return DBConfig().loadConfig(DBConfigFile)


def getSchedulerConfig():
    return SchedulerConfig().loadConfig(schedulerConfigFile)


def getServerConfig():
    return ServerConfig().loadConfig(serverConfigFile)


def getAdminPanelAccessConfig():
    return AdminPanelAccessConfig().loadConfig(DBConfigFile)


def getClusterConfig():
    return ClusterConfig().loadConfig(getLatestClusterConfigFile())


def getAcademicSchedule(year: int):
    return AcademicCalendarConfig(academicCalendarRoot).loadYear(year).toDataFrame()


def getConferenceDates(year: int):
    return ConferenceCalendarConfig(academicCalendarRoot).loadYear(year).toList()


def refreshClusterConfig(command=None, filePath=clusterConfigFile):
    clusterConfig = ClusterConfig().loadFromCommand(command)
    clusterConfig.saveConfig(filePath)
    backupPath = buildClusterConfigBackupPath()
    backupPath.parent.mkdir(parents=True, exist_ok=True)
    clusterConfig.saveConfig(backupPath)
    return {
        "current_file": str(Path(filePath)),
        "backup_file": str(backupPath),
    }


def saveClusterConfigBackupFromFile(sourceFilePath: str | Path, timestamp: datetime | None = None) -> Path:
    sourcePath = Path(sourceFilePath)
    if not sourcePath.exists():
        raise FileNotFoundError(f"Cluster config file not found for backup: {sourcePath}")

    backupPath = buildClusterConfigBackupPath(timestamp=timestamp)
    backupPath.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(sourcePath, backupPath)
    return backupPath


def refreshClusterConfigIfDue(
    *,
    command=None,
    filePath=clusterConfigFile,
    snapshotIntervalHours: int | float | None = None,
    now: datetime | None = None,
):
    if snapshotIntervalHours is not None and snapshotIntervalHours <= 0:
        return {
            "refreshed": False,
            "reason": "disabled",
            "current_file": str(Path(filePath)),
            "backup_file": None,
            "latest_backup_file": None,
            "latest_backup_at": None,
        }

    latestBackup = getLatestClusterConfigBackupFile()
    effectiveNow = datetime.now() if now is None else now
    latestBackupTimestamp = None
    if latestBackup is not None and snapshotIntervalHours is not None:
        latestBackupTimestamp = datetime.fromtimestamp(latestBackup.stat().st_mtime)
        elapsedSeconds = (effectiveNow - latestBackupTimestamp).total_seconds()
        if elapsedSeconds < float(snapshotIntervalHours) * 3600.0:
            return {
                "refreshed": False,
                "reason": "window_not_elapsed",
                "current_file": str(Path(filePath)),
                "backup_file": str(latestBackup),
                "latest_backup_file": str(latestBackup),
                "latest_backup_at": latestBackupTimestamp.isoformat(timespec="seconds"),
            }

    try:
        savedPaths = refreshClusterConfig(command=command, filePath=filePath)
    except Exception as error:
        currentFilePath = Path(filePath)
        if latestBackup is not None:
            return {
                "refreshed": False,
                "reason": "refresh_failed_using_latest_backup",
                "current_file": str(currentFilePath),
                "backup_file": str(latestBackup),
                "latest_backup_file": str(latestBackup),
                "latest_backup_at": latestBackupTimestamp.isoformat(timespec="seconds") if latestBackupTimestamp is not None else None,
                "error": str(error),
            }

        if currentFilePath.exists():
            backupPath = saveClusterConfigBackupFromFile(currentFilePath, timestamp=effectiveNow)
            return {
                "refreshed": False,
                "reason": "refresh_failed_seeded_from_current_file",
                "current_file": str(currentFilePath),
                "backup_file": str(backupPath),
                "latest_backup_file": str(latestBackup) if latestBackup is not None else None,
                "latest_backup_at": latestBackupTimestamp.isoformat(timespec="seconds") if latestBackupTimestamp is not None else None,
                "error": str(error),
            }

        raise

    return {
        "refreshed": True,
        "reason": "missing_backup" if latestBackup is None else "window_elapsed",
        "latest_backup_file": str(latestBackup) if latestBackup is not None else None,
        "latest_backup_at": latestBackupTimestamp.isoformat(timespec="seconds") if latestBackupTimestamp is not None else None,
        **savedPaths,
    }


def getLatestClusterConfigFile() -> str:
    latestBackup = getLatestClusterConfigBackupFile()
    if latestBackup is not None:
        return str(latestBackup)

    return clusterConfigFile


def getLatestClusterConfigBackupFile() -> Path | None:
    backupFiles = getClusterConfigBackupFiles()
    if not backupFiles:
        return None

    return backupFiles[-1]


def getClusterConfigBackupFiles() -> list[Path]:
    backupRoot = Path(clusterConfigBackupRoot)
    return sorted(
        path
        for path in backupRoot.rglob("*.yaml")
        if path.is_file()
    )


def loadClusterConfigTimelineSnapshots(
    *,
    currentTimestamp: int | float | None = None,
    currentFilePath: str | Path | None = None,
) -> list[dict]:
    snapshots = []

    for backupPath in getClusterConfigBackupFiles():
        snapshots.append(
            {
                "timestamp": int(backupPath.stat().st_mtime),
                "kind": "backup",
                "path": str(backupPath.resolve()),
                "config": ClusterConfig().loadConfig(str(backupPath)),
            }
        )

    currentPath = Path(clusterConfigFile if currentFilePath is None else currentFilePath)
    if currentPath.exists():
        effectiveNowTimestamp = datetime.now().timestamp() if currentTimestamp is None else float(currentTimestamp)
        snapshots.append(
            {
                "timestamp": int(max(effectiveNowTimestamp, currentPath.stat().st_mtime)),
                "kind": "current",
                "path": str(currentPath.resolve()),
                "config": ClusterConfig().loadConfig(str(currentPath)),
            }
        )

    if not snapshots:
        raise FileNotFoundError(
            f"Cluster config timeline is empty: neither backups in '{clusterConfigBackupRoot}' "
            f"nor current file '{currentPath}' were found."
        )

    snapshots.sort(
        key=lambda item: (
            int(item["timestamp"]),
            1 if item["kind"] == "current" else 0,
            str(item["path"]),
        )
    )
    return snapshots


def buildClusterConfigBackupPath(timestamp: datetime | None = None) -> Path:
    backupTimestamp = datetime.now() if timestamp is None else timestamp
    return (
        Path(clusterConfigBackupRoot)
        / backupTimestamp.strftime("%Y")
        / backupTimestamp.strftime("%m")
        / backupTimestamp.strftime("%d")
        / f"{backupTimestamp.strftime('%H%M%S')}.yaml"
    )


def setSchedulerForecastDataDir(dataDir: str, filePath: str = schedulerConfigFile):
    schedulerConfig = getSchedulerConfig()
    schedulerConfig.forecast_data_dir = dataDir
    schedulerConfig.saveConfig(filePath)
    return schedulerConfig
