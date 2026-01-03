#!/usr/bin/env python3.11

"""
OpenShift Cost Management - Extrator de Dados
VERSÃO FINAL
✔ Estrutura do Excel preservada
✔ Abas preservadas
✔ Correção baseada 100% no Power Query
"""

import os
import sys
import logging
import requests
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List, Any
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

    def _create_session(self) -> requests.Session:
        session = requests.Session()
        retry = Retry(
            total=self.config.max_retries,
            backoff_factor=self.config.backoff_factor,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET", "POST"]
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
    # DADOS BASE (mantidos)
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
    # 🔥 CORREÇÃO BASEADA NO POWER QUERY
    # OS Cost Cluster Projects
    # --------------------------------------------------------------------------
    def get_cluster_project_costs(self, start_date, end_date, currency="BRL"):
        self.logger.info("Coletando dados Cluster x Project (Power Query mode)...")

        cluster_data = self.get_costs_by_groupby(
            start_date, end_date, currency
        ).get("cluster", [])

        results = []
        seen = set()

        for item in cluster_data:
            date = item.get("date")
            month = pd.to_datetime(date).strftime("%Y-%m")

            for cluster in item.get("clusters", []):
                cluster_name = cluster.get("cluster")
                key = (cluster_name, month)

                if key in seen:
                    continue
                seen.add(key)

                offset = 0
                limit = 200

                while True:
                    params = {
                        "currency": currency,
                        "filter[cluster]": cluster_name,
                        "filter[resolution]": "daily",
                        "filter[limit]": limit,
                        "filter[offset]": offset,
                        "start_date": start_date,
                        "end_date": end_date,
                        "group_by[project]": "*",
                    }

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

                    for row in data:
                        row["_cluster"] = cluster_name
                        results.append(row)

                    offset += limit
                    if offset >= meta.get("count", 0):
                        break

        return results

    def get_tags(self):
        resp = self.session.get(
            f"{self.config.api_base_url}/tags/openshift",
            headers=self._headers(),
            timeout=self.config.timeout,
        )
        resp.raise_for_status()
        return resp.json().get("data", [])

# ------------------------------------------------------------------------------
# FORMATADOR EXCEL
# ------------------------------------------------------------------------------
class ExcelFormatterFixed:
    def __init__(self, logger, currency="BRL"):
        self.logger = logger
        self.currency = currency

    # --------------------------------------------------------------------------
    # OS Cost Cluster Projects (corrigido)
    # --------------------------------------------------------------------------
    def create_os_cost_cluster_projects(self, data):
        rows = []

        for item in data:
            date = item.get("date")
            cluster = item.get("_cluster")

            for project in item.get("projects", []):
                project_name = project.get("project")

                for value in project.get("values", []):
                    total = (
                        value.get("cost", {})
                        .get("total", {})
                        .get("value", 0)
                    )

                    rows.append({
                        "code": self.currency,
                        "Group By Code": "cluster",
                        "cluster": cluster,
                        "date": pd.to_datetime(date),
                        "project": project_name,
                        "value": total or 0,
                        "units": self.currency,
                        "Filter Month": pd.to_datetime(date).strftime("%Y-%m"),
                    })

        return pd.DataFrame(rows)

    # --------------------------------------------------------------------------
    # GERA EXCEL (estrutura preservada)
    # --------------------------------------------------------------------------
    def format_to_excel(
        self,
        all_data,
        cluster_project_data,
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

            # ✅ OS Cost Cluster Projects (CORRIGIDO)
            self.create_os_cost_cluster_projects(
                cluster_project_data
            ).to_excel(
                writer,
                sheet_name="OS Cost Cluster Projects",
                index=False,
            )

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

    config = APIConfig()
    client = OpenShiftCostAPIClient(config, logger)

    all_data = client.get_costs_by_groupby(
        args.start_date, args.end_date
    )
    cluster_project_data = client.get_cluster_project_costs(
        args.start_date, args.end_date
    )
    tags = client.get_tags()

    formatter = ExcelFormatterFixed(logger)
    formatter.format_to_excel(
        all_data,
        cluster_project_data,
        args.output,
        args.start_date,
        args.end_date,
        tags,
    )

    logger.info("Excel gerado com sucesso!")

if __name__ == "__main__":
    main()
