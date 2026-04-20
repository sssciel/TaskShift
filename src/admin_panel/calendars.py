import shutil
from pathlib import Path

from config.models import get_yaml_module
from config.paths import academicCalendarRoot


def get_calendar_root() -> Path:
    return Path(academicCalendarRoot).resolve()


def get_calendar_catalog_payload() -> dict:
    calendarRoot = get_calendar_root()
    calendarRoot.mkdir(parents=True, exist_ok=True)
    yearDirs = sorted(
        [
            path
            for path in calendarRoot.iterdir()
            if path.is_dir()
        ],
        key=lambda path: path.name,
    )
    knownFiles = sorted(
        {
            filePath.name
            for yearDir in yearDirs
            for filePath in yearDir.glob("*.yaml")
        }
    )

    years = []
    for yearDir in yearDirs:
        files = sorted(filePath.name for filePath in yearDir.glob("*.yaml"))
        years.append(
            {
                "year": yearDir.name,
                "path": str(yearDir.resolve()),
                "files": files,
                "missing_known_files": sorted(set(knownFiles) - set(files)),
            }
        )

    return {
        "root": str(calendarRoot),
        "known_files": knownFiles,
        "years": years,
    }


def read_calendar_file(year: str, filename: str) -> dict:
    normalizedYear = normalize_calendar_year(year)
    normalizedFilename = normalize_calendar_filename(filename)
    filePath = get_calendar_root() / normalizedYear / normalizedFilename
    if not filePath.exists():
        raise FileNotFoundError(f"Calendar file not found: {filePath}")

    return {
        "year": normalizedYear,
        "filename": normalizedFilename,
        "path": str(filePath.resolve()),
        "content": filePath.read_text(encoding="utf-8"),
    }


def write_calendar_file(year: str, filename: str, content: str) -> dict:
    normalizedYear = normalize_calendar_year(year)
    normalizedFilename = normalize_calendar_filename(filename)
    get_yaml_module().safe_load(content or "")
    filePath = get_calendar_root() / normalizedYear / normalizedFilename
    filePath.parent.mkdir(parents=True, exist_ok=True)
    filePath.write_text(content, encoding="utf-8")
    return {
        "year": normalizedYear,
        "filename": normalizedFilename,
        "path": str(filePath.resolve()),
        "content": content,
    }


def create_calendar_year(year: str, copyFromYear: str | None = None) -> dict:
    normalizedYear = normalize_calendar_year(year)
    targetDir = get_calendar_root() / normalizedYear
    if targetDir.exists():
        raise ValueError(f"Calendar year already exists: {normalizedYear}")

    targetDir.mkdir(parents=True, exist_ok=False)
    copiedFiles = []
    sourceYear = None

    if copyFromYear:
        sourceYear = normalize_calendar_year(copyFromYear)
        sourceDir = get_calendar_root() / sourceYear
        if not sourceDir.exists():
            raise FileNotFoundError(f"Source calendar year not found: {sourceYear}")

        for sourceFile in sorted(sourceDir.glob("*.yaml")):
            destinationFile = targetDir / sourceFile.name
            shutil.copyfile(sourceFile, destinationFile)
            copiedFiles.append(sourceFile.name)
    else:
        catalog = get_calendar_catalog_payload()
        for filename in catalog["known_files"]:
            destinationFile = targetDir / filename
            destinationFile.write_text(calendar_file_template(filename), encoding="utf-8")
            copiedFiles.append(filename)

    return {
        "year": normalizedYear,
        "path": str(targetDir.resolve()),
        "copied_from_year": sourceYear,
        "files": copiedFiles,
    }


def create_calendar_file(year: str, filename: str, copyFromYear: str | None = None) -> dict:
    normalizedYear = normalize_calendar_year(year)
    normalizedFilename = normalize_calendar_filename(filename)
    yearDir = get_calendar_root() / normalizedYear
    yearDir.mkdir(parents=True, exist_ok=True)
    filePath = yearDir / normalizedFilename
    if filePath.exists():
        raise ValueError(f"Calendar file already exists: {normalizedFilename}")

    sourceYear = None
    if copyFromYear:
        sourceYear = normalize_calendar_year(copyFromYear)
        sourceFile = get_calendar_root() / sourceYear / normalizedFilename
        if not sourceFile.exists():
            raise FileNotFoundError(f"Template file not found in year {sourceYear}: {normalizedFilename}")

        shutil.copyfile(sourceFile, filePath)
    else:
        filePath.write_text(calendar_file_template(normalizedFilename), encoding="utf-8")

    return {
        "year": normalizedYear,
        "filename": normalizedFilename,
        "path": str(filePath.resolve()),
        "copied_from_year": sourceYear,
        "content": filePath.read_text(encoding="utf-8"),
    }


def normalize_calendar_year(yearValue: str) -> str:
    year = str(yearValue).strip()
    if not year.isdigit() or len(year) != 4:
        raise ValueError("Calendar year must be a four-digit year like 2027")

    return year


def normalize_calendar_filename(filenameValue: str) -> str:
    filename = Path(str(filenameValue).strip()).name
    if not filename or filename in {".", ".."}:
        raise ValueError("Calendar filename is required")
    if "/" in filename or "\\" in filename:
        raise ValueError("Calendar filename must not contain path separators")
    if not filename.endswith((".yaml", ".yml")):
        raise ValueError("Calendar filename must end with .yaml or .yml")

    return filename


def calendar_file_template(filename: str) -> str:
    if filename == "russian_holidays.yaml":
        return "dates: []\n"
    if filename == "university_calendar.yaml":
        return "session_dates: []\nvacation_dates: []\n"

    return "{}\n"
