from .calendar import AcademicCalendarConfig
from .models import ClusterConfig, DBConfig, SchedulerConfig
from .paths import DBConfigFile, academicCalendarRoot, clusterConfigFile, schedulerConfigFile


def getDBConfig():
    return DBConfig().loadConfig(DBConfigFile)


def getSchedulerConfig():
    return SchedulerConfig().loadConfig(schedulerConfigFile)


def getClusterConfig():
    return ClusterConfig().loadConfig(clusterConfigFile)


def getAcademicSchedule(year: int):
    return AcademicCalendarConfig(academicCalendarRoot).loadYear(year).toDataFrame()


def refreshClusterConfig(command=None, filePath=clusterConfigFile):
    return ClusterConfig().loadFromCommand(command).saveConfig(filePath)
