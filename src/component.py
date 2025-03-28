"""
Template Component main class.

"""

import os
import gzip
import shutil
import logging
import duckdb
from duckdb import DuckDBPyConnection
import requests


from keboola.component.base import ComponentBase, sync_action
from keboola.component.exceptions import UserException
from keboola.component.sync_actions import SelectElement
from keboola.component.dao import BaseType, ColumnDefinition, SupportedDataTypes


from configuration import Configuration
from client.app_store_connect import AppStoreConnectClient


DUCK_DB_MAX_MEMORY = "128MB"
DUCK_DB_DIR = os.path.join(os.environ.get("TMPDIR", "/tmp"), "duckdb")
FILES_TEMP_DIR = os.path.join(os.environ.get("TMPDIR", "/tmp"), "files")


class Component(ComponentBase):
    def __init__(self):
        super().__init__()
        self.params = Configuration(**self.configuration.parameters)
        os.makedirs(os.path.dirname(FILES_TEMP_DIR), exist_ok=True)
        os.makedirs(FILES_TEMP_DIR, exist_ok=True)
        self.duck = self.init_duckdb()
        self.client = AppStoreConnectClient(
            key_id=self.params.key_id, issuer_id=self.params.issuer_id, key_string=self.params.key_string
        )
        self.state = None

    def run(self):
        self.state = self.get_state_file()

        for app in self.params.source.app_ids:
            logging.info(f"Download reports for app_id: {app}")
            self.download_reports_for_app(app)

        subdirectories = [f for f in os.listdir(FILES_TEMP_DIR) if os.path.isdir(os.path.join(FILES_TEMP_DIR, f))]

        for subdirectory in subdirectories:
            path = os.path.join(FILES_TEMP_DIR, subdirectory)
            self.ungzip_files_in_folder(path)
            self.create_table_from_report(subdirectory)

        self.write_state_file(self.state)

    def download_reports_for_app(self, app_id: str) -> None:
        report_requests = list(self.client.get_reports_requests(app_id))
        relevant_requests = self.get_relevant_report_requests(report_requests)
        if not relevant_requests:
            logging.info(
                f"No reports to download for app_id: {app_id}, creating report request."
                f"Data should be available in the next 48 hours."
            )
            self.client.create_report_request(app_id, self.params.source.access_type)
            return

        for request in relevant_requests:
            reports = list(self.client.get_reports(request))

            for report in reports:
                if report.get("attributes").get("name") in self.params.source.report_names:
                    report_name = report.get("attributes").get("name").replace(" ", "_")
                    instances = list(self.client.get_report_instances(report.get("id"), self.params.source.granularity))
                    for instance in instances:

                        last = self.state.get("last_processed", {}).get(app_id, {}).get(report_name, "2000-01-01")
                        current = instance.get("attributes", {}).get("processingDate")

                        if current > last:
                            segments = []
                            try:
                                segments = list(self.client.get_instance_segments(instance.get("id")))
                            except requests.exceptions.HTTPError:
                                logging.warning(f"Error downloading segment data from the {report_name} report "
                                                f"from the {instance.get('attributes', {}).get('processingDate')}: the "
                                                f"data has either expired or is not yet ready.")

                            for segment in segments:
                                if not os.path.exists(os.path.join(FILES_TEMP_DIR, report_name)):
                                    os.makedirs(os.path.join(FILES_TEMP_DIR, report_name))

                                path = os.path.join(FILES_TEMP_DIR, report_name, f"{segment.get('id')}.csv.gz")

                                self.client.get_segment_data(segment.get("attributes").get("url"), path)

                                self.state.setdefault("last_processed", {}).setdefault(app_id, {})[report_name] = (
                                    instance.get("attributes", {}).get("processingDate")
                                )

    def get_relevant_report_requests(self, report_requests):
        """
        Keep only the report requests with the selected access type that are not stopped due to inactivity.
        """
        relevant_requests = [
            r.get("id")
            for r in report_requests
            if r.get("attributes").get("accessType") == self.params.source.access_type
               and r.get("attributes").get("stoppedDueToInactivity") is False
        ]
        return relevant_requests

    @staticmethod
    def init_duckdb() -> DuckDBPyConnection:
        os.makedirs(DUCK_DB_DIR, exist_ok=True)
        config = dict(temp_directory=DUCK_DB_DIR, threads="1", max_memory=DUCK_DB_MAX_MEMORY)
        conn = duckdb.connect(config=config)

        return conn

    @staticmethod
    def ungzip_files_in_folder(folder_path: str) -> None:
        output_folder = os.path.join(folder_path, "ungzipped")
        os.makedirs(output_folder, exist_ok=True)

        for file_name in os.listdir(folder_path):
            file_path = os.path.join(folder_path, file_name)

            if file_name.endswith(".gz") and os.path.isfile(file_path):
                output_file_name = file_name[:-3]  # Remove .gz extension
                output_file_path = os.path.join(output_folder, output_file_name)

                with gzip.open(file_path, "rb") as gz_file, open(output_file_path, "wb") as out_file:
                    shutil.copyfileobj(gz_file, out_file)

    def create_table_from_report(self, folder_path: str) -> None:
        path = os.path.join(FILES_TEMP_DIR, folder_path, "ungzipped")

        self.duck.execute(f"CREATE VIEW {folder_path} AS SELECT * FROM read_csv('{path}/*.csv')")

        table_meta = self.duck.execute(f"""DESCRIBE {folder_path};""").fetchall()
        schema = {c[0]: ColumnDefinition(data_types=BaseType(dtype=self.convert_base_types(c[1]))) for c in table_meta}

        out_table = self.create_out_table_definition(
            f"{folder_path}.csv",
            schema=schema,
            primary_key=list(schema.keys()), # all columns to deduplicate date in case of incremental load
            incremental=self.params.destination.incremental,
            has_header=True,
        )

        try:
            self.duck.execute(f"COPY {folder_path} TO '{out_table.full_path}' (HEADER, DELIMITER ',', FORCE_QUOTE *)")
        except duckdb.ConversionException as e:
            raise UserException(f"Error during query execution: {e}")

        self.write_manifest(out_table)

    @staticmethod
    def convert_base_types(dtype: str) -> SupportedDataTypes:
        if dtype in [
            "TINYINT",
            "SMALLINT",
            "INTEGER",
            "BIGINT",
            "HUGEINT",
            "UTINYINT",
            "USMALLINT",
            "UINTEGER",
            "UBIGINT",
            "UHUGEINT",
        ]:
            return SupportedDataTypes.INTEGER
        elif dtype in ["REAL", "DECIMAL"]:
            return SupportedDataTypes.NUMERIC
        elif dtype == "DOUBLE":
            return SupportedDataTypes.FLOAT
        elif dtype == "BOOLEAN":
            return SupportedDataTypes.BOOLEAN
        elif dtype in ["TIMESTAMP", "TIMESTAMP WITH TIME ZONE"]:
            return SupportedDataTypes.TIMESTAMP
        elif dtype == "DATE":
            return SupportedDataTypes.DATE
        else:
            return SupportedDataTypes.STRING

    @sync_action("list_apps")
    def list_apps(self) -> list[SelectElement]:
        apps = self.client.get_apps()
        return [SelectElement(value=f"{val['id']}-{val['attributes']['name']}") for val in apps]

    @sync_action("list_reports")
    def list_reports(self) -> list[SelectElement]:
        report_requests = list(self.client.get_reports_requests(self.params.source.app_ids[0]))

        relevant_requests = self.get_relevant_report_requests(report_requests)

        all_reports = []
        for request in relevant_requests:
            all_reports.extend(self.client.get_reports(request))

        report_names = [
            SelectElement(value=val.get("attributes").get("name"))
            for val in all_reports
            if val.get("attributes").get("category") in self.params.source.report_categories
        ]

        return report_names


"""
        Main entrypoint
"""
if __name__ == "__main__":
    try:
        comp = Component()
        # this triggers the run method by default and is controlled by the configuration.action parameter
        comp.execute_action()
    except UserException as exc:
        logging.exception(exc)
        exit(1)
    except Exception as exc:
        logging.exception(exc)
        exit(2)
