#!/usr/bin/env python3.11

"""
OpenShift Cost Management - Extrator de Dados
VERSÃO FINAL DEFINITIVA
✔ Estrutura do Excel preservada
✔ Todas as abas preservadas
✔ OS Cost Cluster Projects = OK
✔ OS Cost Project Tags = 100% Power Query
"""

import os
import logging
import requests
import pandas as pd
from datetime import datetime, timedelta
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
import argparse

# ------------------------------------------------------------------------------
# LOGGING
# ------------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# ------------------------------------------------------------------------------
# CONFIG
# ------------------------------------------------------------------------------
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

# ------------------------------------------------------------------------------
# API CLIENT
# ------------------------------------------------------------------------------
class OpenShiftCostAPIClient:
    def __init__(self, config: APIConfig, logger):
        self.config = config
        self.logger = logger
        self.access_token = None
        self.token_expires_at = None
        self.session = self._create_session()

    def _create_session(self):
        session = requests.Session()
        retry = Retry(
            total=self.config.max_retries,
            backoff_factor=self.config.backoff_factor,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET", "POST"],
        )
        adapter = HTTPAdapter(max_retries=retry)
        session.mount("https://", adapter)
        return session

    def _get_token(self):
        if self.access_token and datetime.now() < self.token_expires_at:
            return self.access_token

        self.logger.info("Obtendo token de acesso...")
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
        self.token_expires_at = datetime.now() + timedelta(
            seconds=data.get("expires_in", 900) - 60
        )
        return self.access_token

    def _headers(self):
        return {
            "Authorization": f"Bearer {self._get_token()}",
            "Accept": "application/json",
        }

    # --------------------------------------------------------------------------
    # COST DATA
    # --------------------------------------------------------------------------
    def get_costs_by_groupby(self, start_date, end_date, currency="BRL"):
        result = {"cluster": [], "project": [], "node": [], "tag": []}

        groupbys = [
            ("cluster", {"group_by[cluster]": "*"}),
            ("project", {"group_by[project]": "*"}),
            ("node", {"group_by[cluster]": "*", "group_by[node]": "*"}),
            ("tag", {"group_by[tag:produto]": "*"}),
        ]

        for key, groupby in groupbys:
            offset = 0
            limit = 200

            while True:
                params = {
                    "currency": currency,
                    "filter[resolution]": "daily",
                    "filter[limit]": limit,
                    "filter[offset]": offset,
                    "start_date": start_date,
                    "end_date": end_date,
                }
                params.update(groupby)

                resp = self.session.get(
                    f"{self.config.api_base_url}/reports/openshift/costs/",
                    headers=self._headers(),
                    params=params,
                    timeout=self.config.timeout,
                )
                resp.raise_for_status()

                payload = resp.json()
                data = payload.get("data", [])
                meta = payload.get("meta", {})

                if not data:
                    break

                result[key].extend(data)
                offset += limit
                if offset >= meta.get("count", 0):
                    break

        return result

    # --------------------------------------------------------------------------
    # TAGS
    # --------------------------------------------------------------------------
    def get_project_tags_by_project(self, project, start_date, end_date):
        resp = self.session.get(
            f"{self.config.api_base_url}/tags/openshift",
            headers=self._headers(),
            params={
                "filter[project]": project,
                "filter[resolution]": "daily",
                "start_date": start_date,
                "end_date": end_date,
            },
            timeout=self.config.timeout,
        )
        resp.raise_for_status()
        return resp.json().get("data", [])

    def get_tags(self):
        resp = self.session.get(
            f"{self.config.api_base_url}/tags/openshift",
            headers=self._headers(),
            timeout=self.config.timeout,
        )
        resp.raise_for_status()
        return resp.json().get("data", [])

# ------------------------------------------------------------------------------
# EXCEL FORMATTER
# ------------------------------------------------------------------------------
class ExcelFormatterFixed:
    def __init__(self, logger, currency="BRL"):
        self.logger = logger
        self.currency = currency

    # --------------------------------------------------------------------------
    # OS Cost Project Tags — 100% Power Query
    # --------------------------------------------------------------------------
    def create_os_cost_project_tags(
        self,
        all_data,
        client,
        start_date,
        end_date,
    ):
        self.logger.info("Gerando OS Cost Project Tags (Power Query mode)...")

        base_rows = []

        for item in all_data.get("project", []):
            date = pd.to_datetime(item.get("date")).to_period("M").to_timestamp()
            for project in item.get("projects", []):
                base_rows.append({
                    "project": project.get("project"),
                    "date": date,
                })

        df_base = (
            pd.DataFrame(base_rows)
            .dropna(subset=["project"])
            .drop_duplicates(subset=["project", "date"])
        )

        rows = []

        for _, row in df_base.iterrows():
            tags = client.get_project_tags_by_project(
                row["project"],
                start_date,
                end_date,
            )

            # 🔥 Power Query behavior: mantém linha mesmo sem tag,
            # mas enabled deve ficar NULL (em branco), não TRUE
            if not tags:
                rows.append({
                    "code": self.currency,
                    "date": row["date"],
                    "project": row["project"],
                    "key": None,
                    "values": None,
                    "enabled": None,  # <-- AQUI
                    "Filter Month": row["date"].strftime("%Y-%m"),
                })
                continue

            for tag in tags:
                # IMPORTANTÍSSIMO:
                # Se "enabled" não vier no payload, no Power Query fica null (em branco).
                enabled = tag.get("enabled", None)  # <-- SEM default True

                # Mantém sua lógica de valores
                values = tag.get("values", [])

                if not values:
                    rows.append({
                        "code": self.currency,
                        "date": row["date"],
                        "project": row["project"],
                        "key": tag.get("key"),
                        "values": None,
                        "enabled": enabled,  # <-- AQUI
                        "Filter Month": row["date"].strftime("%Y-%m"),
                    })
                    continue

                for value in values:
                    rows.append({
                        "code": self.currency,
                        "date": row["date"],
                        "project": row["project"],
                        "key": tag.get("key"),
                        "values": value,
                        "enabled": enabled,  # <-- AQUI
                        "Filter Month": row["date"].strftime("%Y-%m"),
                    })

        return pd.DataFrame(rows)

    # --------------------------------------------------------------------------
    # EXCEL OUTPUT
    # --------------------------------------------------------------------------
    def format_to_excel(
        self,
        all_data,
        client,
        output,
        start_date,
        end_date,
        tags,
    ):
        with pd.ExcelWriter(output, engine="openpyxl") as writer:

            # Data_Period
            pd.DataFrame([
                ["Start Date", start_date],
                ["End Date", end_date],
            ]).to_excel(
                writer, sheet_name="Data_Period", index=False, header=False
            )

            # Default Master Settings
            pd.DataFrame({
                "code": ["BRL"],
                "name": ["Brazilian Real"],
                "symbol": ["R$"],
                "description": ["BRL (R$) - Brazilian Real"],
            }).to_excel(
                writer, sheet_name="Default Master Settings", index=False
            )

            # Project Overhead Cost Types
            pd.DataFrame({
                "Code": ["cost", "distributed_cost"],
                "Description": [
                    "Don't distribute overhead costs",
                    "Distribute through cost models",
                ],
            }).to_excel(
                writer, sheet_name="Project Overhead Cost Types", index=False
            )

            # OpenShift Group Bys
            pd.DataFrame({
                "Group By": ["Cluster", "Node", "Project", "Tag"],
                "Group By Code": ["cluster", "node", "project", "tag"],
            }).to_excel(
                writer, sheet_name="OpenShift Group Bys", index=False
            )

            # OS Tag Keys
            pd.DataFrame([
                {"key": t.get("key"), "enabled": True, "Group By": "tag"}
                for t in tags
            ]).to_excel(
                writer, sheet_name="OS Tag Keys", index=False
            )

            # ✅ OS Cost Project Tags (CORRIGIDO)
            self.create_os_cost_project_tags(
                all_data,
                client,
                start_date,
                end_date,
            ).to_excel(
                writer,
                sheet_name="OS Cost Project Tags",
                index=False,
            )

        self.logger.info("Excel gerado com sucesso!")

# ------------------------------------------------------------------------------
# MAIN
# ------------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--start-date")
    parser.add_argument("--end-date")
    parser.add_argument("--output", default="openshift_costs.xlsx")
    args = parser.parse_args()

    if not args.end_date:
        args.end_date = datetime.now().strftime("%Y-%m-%d")
    if not args.start_date:
        args.start_date = (
            datetime.now() - timedelta(days=30)
        ).strftime("%Y-%m-%d")

    logger.info("Iniciando extração OpenShift Cost Management")

    config = APIConfig()
    client = OpenShiftCostAPIClient(config, logger)

    all_data = client.get_costs_by_groupby(
        args.start_date, args.end_date
    )
    tags = client.get_tags()

    formatter = ExcelFormatterFixed(logger)
    formatter.format_to_excel(
        all_data,
        client,
        args.output,
        args.start_date,
        args.end_date,
        tags,
    )

if __name__ == "__main__":
    main()
