#!/usr/bin/env python3.11
"""
OpenShift Cost Management - OS Daily Usage (somente)
✔ Gera APENAS a aba "OS Daily Usage"
✔ Lógica baseada 100% no Power Query (combine Project/Cluster/Node/Tag)
✔ Endpoints corretos (compute/memory/volumes) — evita 404 de /cpu/
✔ Colunas e ordem iguais ao template OpenShift_Daily_Usage.xlsx
"""

import os
import sys
import logging
import argparse
import requests
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter

# ---------------------------------------------------------------------
# LOG
# ---------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - __main__ - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------
class APIConfig:
    def __init__(self):
        self.client_id = os.getenv("OPENSHIFT_CLIENT_ID", "")
        self.client_secret = os.getenv("OPENSHIFT_CLIENT_SECRET", "")
        self.auth_url = (
            "https://sso.redhat.com/auth/realms/redhat-external/"
            "protocol/openid-connect/token"
        )
        self.api_base_url = "https://console.redhat.com/api/cost-management/v1"
        self.timeout = 30
        self.max_retries = 3
        self.backoff_factor = 0.5

# ---------------------------------------------------------------------
# API CLIENT
# ---------------------------------------------------------------------
class OpenShiftCostAPIClient:
    def __init__(self, config: APIConfig, logger: logging.Logger):
        self.config = config
        self.logger = logger
        self.session = self._create_session()
        self.access_token: Optional[str] = None
        self.token_expires_at: Optional[datetime] = None

    def _create_session(self) -> requests.Session:
        s = requests.Session()
        retry = Retry(
            total=self.config.max_retries,
            backoff_factor=self.config.backoff_factor,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET", "POST", "HEAD", "OPTIONS"],
        )
        s.mount("https://", HTTPAdapter(max_retries=retry))
        s.mount("http://", HTTPAdapter(max_retries=retry))
        return s

    def _get_token(self) -> str:
        # Reusa token se ainda válido
        if self.access_token and self.token_expires_at and datetime.now() < self.token_expires_at:
            return self.access_token

        self.logger.info("Obtendo novo access token...")
        resp = self.session.post(
            self.config.auth_url,
            data={
                "grant_type": "client_credentials",
                "client_id": self.config.client_id,
                "client_secret": self.config.client_secret,
            },
            timeout=self.config.timeout,
        )
        resp.raise_for_status()
        data = resp.json()
        self.access_token = data["access_token"]
        expires_in = int(data.get("expires_in", 900))
        self.token_expires_at = datetime.now() + timedelta(seconds=expires_in - 60)
        self.logger.info("Token obtido com sucesso")
        return self.access_token

    def _headers(self) -> Dict[str, str]:
        return {
            "Authorization": f"Bearer {self._get_token()}",
            "Accept": "application/json",
        }

    def get_tag_keys(self, limit: int = 1000) -> List[str]:
        """
        Mesma fonte de keys do PQ (usa tags/openshift e extrai 'key').
        """
        url = f"{self.config.api_base_url}/tags/openshift"
        resp = self.session.get(
            url,
            headers=self._headers(),
            params={"limit": limit},
            timeout=self.config.timeout,
        )
        resp.raise_for_status()
        data = resp.json().get("data", [])
        keys = []
        for t in data:
            k = t.get("key")
            if k:
                keys.append(k)
        # fallback
        return sorted(set(keys)) if keys else ["produto"]

    def get_usage_report(
        self,
        usage_code: str,
        start_date: str,
        end_date: str,
        group_by_code: str,
        tag_key: Optional[str] = None,
        limit: int = 200,
    ) -> Dict[str, Any]:
        """
        Implementa o get_usage_loop_data do Power Query:
        /reports/openshift/{usage_code}/?filter[limit]=...&filter[offset]=...&filter[resolution]=daily...
        &group_by[project|cluster|node|tag:key]=*
        """
        url = f"{self.config.api_base_url}/reports/openshift/{usage_code}/"

        # monta o parâmetro group_by
        if group_by_code == "tag":
            if not tag_key:
                raise ValueError("tag_key é obrigatório quando group_by_code='tag'")
            group_by_param = {f"group_by[tag:{tag_key}]": "*"}
        else:
            group_by_param = {f"group_by[{group_by_code}]": "*"}

        all_items: List[Dict[str, Any]] = []
        meta_first: Optional[Dict[str, Any]] = None

        offset = 0
        while True:
            params = {
                "filter[limit]": limit,
                "filter[offset]": offset,
                "filter[resolution]": "daily",
                "start_date": start_date,
                "end_date": end_date,
            }
            params.update(group_by_param)

            resp = self.session.get(
                url,
                headers=self._headers(),
                params=params,
                timeout=self.config.timeout,
            )
            resp.raise_for_status()
            payload = resp.json()

            data = payload.get("data", []) or []
            meta = payload.get("meta", {}) or {}
            if meta_first is None:
                meta_first = meta

            if not data:
                break

            all_items.extend(data)

            count = int(meta.get("count", 0) or 0)
            offset += limit
            if offset >= count:
                break

        return {
            "meta": meta_first or {},
            "data": all_items,
        }

# ---------------------------------------------------------------------
# FORMATADOR: OS DAILY USAGE
# ---------------------------------------------------------------------
class ExcelDailyUsageFormatter:
    def __init__(self, logger: logging.Logger, currency: str = "BRL"):
        self.logger = logger
        self.currency = currency

        # Ordem/colunas iguais ao template enviado (OpenShift_Daily_Usage.xlsx)
        self.TEMPLATE_COLUMNS = [
            "Group By",
            "Group By Code",
            "Usage Code",
            "Usage Name",
            "Key",
            "meta.count",
            "meta.currency",
            "date",
            "Name",
            "values.usage.value",
            "values.usage.units",
            "values.request.value",
            "values.request.units",
            "values.request.unused",
            "values.request.unused_percent",
            "values.limit.value",
            "values.limit.units",
            "values.capacity.value",
            "values.capacity.units",
            "values.capacity.unused",
            "values.capacity.unused_percent",
            "values.capacity.count",
            "values.capacity.count_units",
        ]

        # mapping do teu template
        self.USAGE_CODE_NAME = {
            "compute": "CPU",
            "memory": "Memory",
            "volumes": "Volume Claims",
        }

    @staticmethod
    def _safe_num(x):
        try:
            if x is None:
                return None
            return float(x)
        except Exception:
            return None

    def _flatten_metric(self, metric: Dict[str, Any], prefix: str) -> Dict[str, Any]:
        """
        metric exemplo:
          usage: {value, units}
          request: {value, units, unused, unused_percent}
          capacity: {value, units, unused, unused_percent, count, count_units}
        """
        out = {}

        if not isinstance(metric, dict):
            # preenche campos esperados com None
            if prefix in ("request", "capacity"):
                out[f"values.{prefix}.value"] = None
                out[f"values.{prefix}.units"] = None
                out[f"values.{prefix}.unused"] = None
                out[f"values.{prefix}.unused_percent"] = None
                if prefix == "capacity":
                    out["values.capacity.count"] = None
                    out["values.capacity.count_units"] = None
            else:
                out[f"values.{prefix}.value"] = None
                out[f"values.{prefix}.units"] = None
            return out

        out[f"values.{prefix}.value"] = metric.get("value")
        out[f"values.{prefix}.units"] = metric.get("units")

        if prefix in ("request", "capacity"):
            out[f"values.{prefix}.unused"] = metric.get("unused")
            out[f"values.{prefix}.unused_percent"] = metric.get("unused_percent")

        if prefix == "capacity":
            out["values.capacity.count"] = metric.get("count")
            out["values.capacity.count_units"] = metric.get("count_units")

        return out

    def _extract_group_list(self, day_item: Dict[str, Any], group_by_code: str, tag_key: Optional[str]) -> List[Dict[str, Any]]:
        """
        No PQ:
          - project => day_item["projects"]
          - cluster => day_item["clusters"]
          - node   => day_item["nodes"]
          - tag    => day_item[f"{Key}s"] (ex: "produtos")
        """
        if group_by_code == "project":
            return day_item.get("projects", []) or []
        if group_by_code == "cluster":
            return day_item.get("clusters", []) or []
        if group_by_code == "node":
            return day_item.get("nodes", []) or []
        if group_by_code == "tag":
            plural = f"{tag_key}s" if tag_key else "tags"
            return day_item.get(plural, []) or []
        return []

    def _get_name_from_group_record(self, group_by_code: str, group_record: Dict[str, Any], tag_key: Optional[str]) -> Optional[str]:
        if group_by_code == "project":
            return group_record.get("project")
        if group_by_code == "cluster":
            return group_record.get("cluster")
        if group_by_code == "node":
            return group_record.get("node")
        if group_by_code == "tag":
            # normalmente vem "tag"
            return group_record.get("tag") or group_record.get(tag_key) or group_record.get("key")
        return None

    def _rows_from_usage_payload(
        self,
        payload: Dict[str, Any],
        group_by_label: str,
        group_by_code: str,
        usage_code: str,
        usage_name: str,
        key: Optional[str],
    ) -> List[Dict[str, Any]]:
        meta = payload.get("meta", {}) or {}
        meta_count = meta.get("count", None)
        meta_currency = meta.get("currency", self.currency)

        rows: List[Dict[str, Any]] = []

        for day_item in payload.get("data", []) or []:
            date_str = day_item.get("date")
            if not date_str:
                continue
            date_val = pd.to_datetime(date_str).date()

            group_list = self._extract_group_list(day_item, group_by_code, key)

            for group_record in group_list:
                name = self._get_name_from_group_record(group_by_code, group_record, key)
                if name is None:
                    continue

                values_list = group_record.get("values", []) or []
                for v in values_list:
                    # cada v contém usage/request/limit/capacity
                    row = {
                        "Group By": group_by_label,
                        "Group By Code": group_by_code,
                        "Usage Code": usage_code,
                        "Usage Name": usage_name,
                        "Key": key if group_by_code == "tag" else None,
                        "meta.count": meta_count,
                        "meta.currency": meta_currency,
                        "date": pd.to_datetime(date_val),
                        "Name": name,
                    }
                    row.update(self._flatten_metric(v.get("usage"), "usage"))
                    row.update(self._flatten_metric(v.get("request"), "request"))
                    row.update(self._flatten_metric(v.get("limit"), "limit"))
                    row.update(self._flatten_metric(v.get("capacity"), "capacity"))

                    rows.append(row)

        return rows

    def create_os_daily_usage(
        self,
        client: OpenShiftCostAPIClient,
        start_date: str,
        end_date: str,
        usage_codes: List[str],
        tag_keys: List[str],
    ) -> pd.DataFrame:
        self.logger.info("Gerando OS Daily Usage (Power Query mode)...")

        group_bys = [
            ("Project", "project"),
            ("Cluster", "cluster"),
            ("Node", "node"),
            ("Tag", "tag"),
        ]

        all_rows: List[Dict[str, Any]] = []

        for usage_code in usage_codes:
            usage_name = self.USAGE_CODE_NAME.get(usage_code, usage_code)

            for group_label, group_code in group_bys:
                if group_code == "tag":
                    for k in tag_keys:
                        payload = client.get_usage_report(
                            usage_code=usage_code,
                            start_date=start_date,
                            end_date=end_date,
                            group_by_code="tag",
                            tag_key=k,
                            limit=200,
                        )
                        all_rows.extend(
                            self._rows_from_usage_payload(
                                payload,
                                group_by_label=group_label,
                                group_by_code="tag",
                                usage_code=usage_code,
                                usage_name=usage_name,
                                key=k,
                            )
                        )
                else:
                    payload = client.get_usage_report(
                        usage_code=usage_code,
                        start_date=start_date,
                        end_date=end_date,
                        group_by_code=group_code,
                        tag_key=None,
                        limit=200,
                    )
                    all_rows.extend(
                        self._rows_from_usage_payload(
                            payload,
                            group_by_label=group_label,
                            group_by_code=group_code,
                            usage_code=usage_code,
                            usage_name=usage_name,
                            key=None,
                        )
                    )

        df = pd.DataFrame(all_rows)

        # garante todas colunas do template e ordem
        for c in self.TEMPLATE_COLUMNS:
            if c not in df.columns:
                df[c] = None
        df = df[self.TEMPLATE_COLUMNS]

        # tipos (como o PQ faz "Changed Type")
        # não forçar demais pra não quebrar dados vazios, mas garantir numéricos onde faz sentido
        numeric_cols = [
            "values.usage.value",
            "values.request.value",
            "values.request.unused",
            "values.request.unused_percent",
            "values.limit.value",
            "values.capacity.value",
            "values.capacity.unused",
            "values.capacity.unused_percent",
            "values.capacity.count",
        ]
        for col in numeric_cols:
            df[col] = pd.to_numeric(df[col], errors="coerce")

        df["date"] = pd.to_datetime(df["date"], errors="coerce")

        return df

    def write_excel(self, df_usage: pd.DataFrame, output_file: str):
        # workbook com UMA aba visível => evita erro openpyxl
        with pd.ExcelWriter(output_file, engine="openpyxl") as writer:
            df_usage.to_excel(writer, sheet_name="OS Daily Usage", index=False)

# ---------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="Gerar somente a aba OS Daily Usage")
    parser.add_argument("--start-date", type=str, required=False, default=None, help="YYYY-MM-DD")
    parser.add_argument("--end-date", type=str, required=False, default=None, help="YYYY-MM-DD")
    parser.add_argument("--output", type=str, required=False, default="OpenShift_Daily_Usage.xlsx")
    parser.add_argument("--currency", type=str, required=False, default="BRL")
    parser.add_argument(
        "--usage-codes",
        type=str,
        required=False,
        default="compute,memory,volumes",
        help="Lista separada por vírgula. Ex: compute,memory,volumes",
    )
    args = parser.parse_args()

    if not args.end_date:
        args.end_date = datetime.now().strftime("%Y-%m-%d")
    if not args.start_date:
        args.start_date = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")

    usage_codes = [u.strip() for u in (args.usage_codes or "").split(",") if u.strip()]
    if not usage_codes:
        usage_codes = ["compute", "memory", "volumes"]

    logger.info("Iniciando extração OS Daily Usage")
    logger.info(f"Período: {args.start_date} a {args.end_date}")
    logger.info(f"Usage Codes: {', '.join(usage_codes)}")

    try:
        config = APIConfig()
        client = OpenShiftCostAPIClient(config, logger)

        # Tag keys (PQ: merge com Cost_Tag_Keys)
        tag_keys = client.get_tag_keys()

        formatter = ExcelDailyUsageFormatter(logger, currency=args.currency)
        df_usage = formatter.create_os_daily_usage(
            client=client,
            start_date=args.start_date,
            end_date=args.end_date,
            usage_codes=usage_codes,
            tag_keys=tag_keys,
        )

        formatter.write_excel(df_usage, args.output)
        logger.info(f"Excel gerado com sucesso: {args.output}")

    except Exception as e:
        logger.error(f"Erro durante a execução: {e}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main()
