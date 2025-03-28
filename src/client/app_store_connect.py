import time
from typing import Literal, Optional

import backoff
import jwt
import requests
from keboola.component.exceptions import UserException
from keboola.http_client import HttpClient

BASE_URL = "https://api.appstoreconnect.apple.com/v1"


class AppStoreConnectClient(HttpClient):
    def __init__(self, key_id: str, issuer_id: str, key_string: str, timeout: int = 30):
        super().__init__(base_url=BASE_URL)

        self.key_id = key_id
        self.issuer_id = issuer_id
        self.key_string = key_string
        self.timeout = timeout

        self.update_auth_header({"Authorization": f"Bearer {self._generate_token()}"})

    def _generate_token(self) -> str:
        payload = {
            "iss": self.issuer_id,
            "exp": int(time.time()) + 1200,  # maximum expiration time is 20 minutes
            "aud": "appstoreconnect-v1",
        }

        token = jwt.encode(
            payload=payload, key=self.key_string, algorithm="ES256", headers={"kid": self.key_id, "typ": "JWT"}
        )

        return token

    def _paginate(self, endpoint: str, params: dict = None):
        while True:
            response = self.get(endpoint, params=params)
            for item in response.get("data"):
                yield item

            endpoint = response.get("links", {}).get("next")
            if not endpoint:
                break

    def get_apps(self):
        yield from self._paginate("apps")

    def create_report_request(self, app_id: str, access_type: Literal["ONGOING", "ONE_TIME_SNAPSHOT"]):
        payload = {
            "data": {
                "type": "analyticsReportRequests",
                "attributes": {"accessType": access_type},
                "relationships": {"app": {"data": {"type": "apps", "id": app_id}}},
            }
        }

        response = self.post("analyticsReportRequests", data=payload)

        if response.status_code != 200:
            raise UserException(f"Failed to create report request: {response.text}")

        return response

    def get_reports_requests(self, app_id: str):
        yield from self._paginate(f"apps/{app_id}/analyticsReportRequests")

    def get_reports(self, request_id: str, filter_category: Optional[str] = None):
        params = {}
        if filter_category:
            params["filter[category]"] = filter_category
        yield from self._paginate(f"analyticsReportRequests/{request_id}/reports", params)

    def get_report_instances(self, report_id: str, granularity: Literal["DAILY", "WEEKLY", "MONTHLY"] = "DAILY"):
        params = {}
        if granularity:
            params["filter[granularity]"] = granularity
        yield from self._paginate(f"analyticsReports/{report_id}/instances", params)

    def get_instance_segments(self, instance_id: str):
        yield from self._paginate(f"analyticsReportInstances/{instance_id}/segments")

    @backoff.on_exception(backoff.expo, requests.exceptions.RequestException, max_tries=5)
    def get_segment_data(self, url: str, path: str):
        with requests.get(url, stream=True, timeout=20) as r:
            r.raise_for_status()
            with open(path, "wb") as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
