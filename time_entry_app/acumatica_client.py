import os
from urllib.parse import quote

import requests


class AcumaticaError(RuntimeError):
    pass


def entity_value(entity, key, default=None):
    value = (entity or {}).get(key, default)
    if isinstance(value, dict) and "value" in value:
        return value.get("value", default)
    return value


def wrapped(value):
    return {"value": value}


class AcumaticaClient:
    def __init__(self, session=None):
        self.base_url = os.getenv("ACUMATICA_BASE_URL", "https://benoit-inc.acumatica.com").rstrip("/")
        self.tenant = os.getenv("ACUMATICA_TENANT", "Test")
        self.branch = os.getenv("ACUMATICA_BRANCH", "MAIN")
        self.endpoint = os.getenv("ACUMATICA_ENDPOINT", "Labor")
        self.endpoint_version = os.getenv("ACUMATICA_ENDPOINT_VERSION", "25.100.001")
        self.username = os.getenv("ACUMATICA_USERNAME", "itbenoit")
        self.password = os.getenv("ACUMATICA_PASSWORD", "")
        self.timeout = int(os.getenv("ACUMATICA_TIMEOUT_SECONDS", "45"))
        self.session = session or requests.Session()
        self._logged_in = False

    @property
    def entity_url(self):
        return f"{self.base_url}/entity/{self.endpoint}/{self.endpoint_version}/LaborEntry"

    def _request(self, method, url, **kwargs):
        try:
            response = self.session.request(method, url, timeout=self.timeout, **kwargs)
        except requests.RequestException as exc:
            raise AcumaticaError(f"Acumatica request failed: {exc}") from exc
        if response.status_code not in {200, 201, 202, 204}:
            detail = response.text.strip()
            if len(detail) > 800:
                detail = f"{detail[:800]}..."
            raise AcumaticaError(
                f"Acumatica returned HTTP {response.status_code} for {method} {url}: {detail or 'No response body'}"
            )
        if response.status_code == 204 or not response.text.strip():
            return {}
        try:
            return response.json()
        except ValueError as exc:
            raise AcumaticaError("Acumatica returned an invalid JSON response.") from exc

    def login(self):
        if not self.password:
            raise AcumaticaError("ACUMATICA_PASSWORD is not configured.")
        payload = {
            "name": self.username,
            "password": self.password,
            "company": self.tenant,
            "branch": self.branch,
        }
        self._request("POST", f"{self.base_url}/entity/auth/login", json=payload)
        self._logged_in = True

    def logout(self):
        if not self._logged_in:
            return
        try:
            self._request("POST", f"{self.base_url}/entity/auth/logout")
        finally:
            self._logged_in = False

    def __enter__(self):
        self.login()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.logout()

    def get_batch(self, batch_nbr):
        encoded = quote(str(batch_nbr), safe="")
        return self._request("GET", f"{self.entity_url}/{encoded}?$expand=Details")

    def get_production_order(self, order_type, production_nbr):
        url = f"{self.base_url}/entity/{self.endpoint}/{self.endpoint_version}/ProductionOrder"
        order_type = str(order_type or "EN").replace("'", "''")
        production_nbr = str(production_nbr or "").replace("'", "''")
        rows = self._request(
            "GET",
            url,
            params={
                "$filter": f"OrderType eq '{order_type}' and ProductionNbr eq '{production_nbr}'",
                "$top": 1,
            },
        )
        return rows[0] if rows else None

    def put_batch(self, payload):
        return self._request("PUT", self.entity_url, json=payload)

    def patch_batch(self, payload):
        return self._request("PATCH", self.entity_url, json=payload)
