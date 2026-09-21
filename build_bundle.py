import argparse
import ast
import json
import re
from pathlib import Path
from zipfile import ZIP_DEFLATED, ZipFile


ROOT = Path(__file__).resolve().parent
BUNDLE_FILES = (
    "README.md", "ingest.py", "config.example.json", "setup.template.sql", "setup.sql",
    "admin_prerequisites.sql", "operations.sql", "test_run.sql", "activate.sql",
    "requirements.txt", "test_ingest.py", "sample_response.json", "build_bundle.py", "config.se.json",
)


def build(output=None, config_file="config.example.json"):
    config_text = (ROOT / config_file).read_text()
    config = json.loads(config_text)
    source = (ROOT / "ingest.py").read_text()
    ast.parse(source)
    if "$$" in config_text or "$$" in source:
        raise ValueError("Dollar-quote delimiter cannot appear inside embedded source/configuration")
    hosts = config["allowed_hosts"]
    if not hosts or not all(re.fullmatch(r"[a-zA-Z0-9.-]+", host) for host in hosts):
        raise ValueError("allowed_hosts must contain DNS hostnames only")
    worksheet = (ROOT / "setup.template.sql").read_text()
    worksheet = worksheet.replace("{{CONFIG_JSON}}", config_text)
    worksheet = worksheet.replace("{{HOST_LIST}}", ", ".join("'" + host + "'" for host in hosts))
    worksheet = worksheet.replace("{{PYTHON_SOURCE}}", source)
    (ROOT / "setup.sql").write_text(worksheet)
    print("Generated", ROOT / "setup.sql")
    if output:
        destination = Path(output).expanduser().resolve()
        if not destination.parent.is_dir():
            raise ValueError("Output parent directory must exist")
        if destination.exists():
            raise FileExistsError("Refusing to overwrite an existing ZIP: " + str(destination))
        with ZipFile(destination, "x", ZIP_DEFLATED) as archive:
            for name in BUNDLE_FILES:
                archive.write(ROOT / name, "query-api-starter/" + name)
        with ZipFile(destination) as archive:
            bad_file = archive.testzip()
            if bad_file:
                raise ValueError("ZIP verification failed for " + bad_file)
        print("Created and verified", destination)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", help="Optional ZIP destination; never include real credentials in this project")
    parser.add_argument("--config", default="config.example.json", choices=("config.example.json", "config.se.json"))
    arguments = parser.parse_args()
    build(arguments.output, arguments.config)
