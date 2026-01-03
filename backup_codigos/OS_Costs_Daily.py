#!/usr/bin/env python3.11
"""
OpenShift Cost Management - Extrator de Dados
VERSÃO FINAL - PQ MODE

✔ Estrutura do Excel preservada
✔ Abas preservadas
✔ Correção baseada 100% no Power Query
✔ OS Cost Cluster Projects OK
✔ OS Cost Project Tags OK
✔ OS Costs Daily OK (Table.Combine Projects/Clusters/Nodes/Tags)
"""

import os
import sys
import logging
import requests
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
import argparse

# ------------------------------------------------------------------------------
# LOGGING
# ------------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
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
        retry_strategy = Retry(
            total=self.config.max_retries,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET", "POST"],
            backoff_factor=self.config.backoff_factor,
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        return session

    def _get_token(self) -> str:
        if self.access_token and self.token_expires_at and datetime.now() < self.token_expires_at:
            return self.access_token

        self.logger.info("Obtendo novo access token...")
        auth_data = {
            "grant_type": "client_credentials",
            "client_id": self.config.client_id,
            "client_secret": self.config.client_secret,
        }
        resp = self.session.post(
            self.config.auth_url,
            data=auth_data,
            timeout=self.config.timeout,
        )
        resp.raise_for_status()
        token_response = resp.json()
        self.access_token = token_response["access_token"]
        expires_in = token_response.get("expires_in", 900)
        self.token_expires_at = datetime.now() + timedelta(seconds=expires_in - 60)
        self.logger.info("Token obtido com sucesso")
        return self.access_token

    def _headers(self) -> Dict[str, str]:
        return {
            "Authorization": f"Bearer {self._get_token()}",
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

    # --------------------------------------------------------------------------
    # BASE: /reports/openshift/costs (group_by)
    # --------------------------------------------------------------------------
    def get_costs_by_groupby(self, start_date: str, end_date: str, currency: str = "BRL") -> Dict[str, Any]:
        all_data: Dict[str, Any] = {}

        group_by_configs = [
            {"type": "cluster", "params": {"group_by[cluster]": "*"}},
            {"type": "node", "params": {"group_by[cluster]": "*", "group_by[node]": "*"}},
            {"type": "project", "params": {"group_by[project]": "*"}},
            {"type": "tag:produto", "params": {"group_by[tag:produto]": "*"}},
        ]

        for cfg in group_by_configs:
            group_type = cfg["type"]
            group_params = cfg["params"]

            try:
                self.logger.info(f"Buscando dados agrupados por {group_type}...")
                all_items: List[Dict[str, Any]] = []
                limit = 250
                offset = 0
                page = 1

                while True:
                    url = f"{self.config.api_base_url}/reports/openshift/costs/"
                    params = {
                        "currency": currency,
                        "filter[limit]": limit,
                        "filter[offset]": offset,
                        "filter[resolution]": "daily",
                        "start_date": start_date,
                        "end_date": end_date,
                        "order_by[cost]": "desc",
                    }
                    params.update(group_params)

                    resp = self.session.get(
                        url,
                        params=params,
                        headers=self._headers(),
                        timeout=self.config.timeout,
                    )
                    resp.raise_for_status()
                    payload = resp.json()

                    items = payload.get("data", [])
                    meta = payload.get("meta", {})

                    if not items:
                        break

                    all_items.extend(items)

                    count = meta.get("count", 0)
                    offset_next = offset + limit

                    if count <= offset_next:
                        break

                    offset = offset_next
                    page += 1

                # tag:produto → armazenar em all_data["tag"] e guardar nome da key
                if "tag:" in group_type:
                    tag_key_name = group_type.split(":", 1)[1]
                    all_data["_tag_key_name"] = tag_key_name
                    all_data.setdefault("tag", [])
                    all_data["tag"].extend(all_items)
                else:
                    all_data.setdefault(group_type, [])
                    all_data[group_type].extend(all_items)

                self.logger.info(f"Obtidos {len(all_items)} registros para {group_type} ({page} paginas)")
            except Exception as e:
                self.logger.warning(f"Aviso: Falha ao obter dados para {group_type}: {e}")

        for key in ["cluster", "node", "project", "tag"]:
            all_data.setdefault(key, [])

        return all_data

    # --------------------------------------------------------------------------
    # TAG KEYS
    # --------------------------------------------------------------------------
    def get_tags(self, limit: int = 1000) -> List[Dict[str, Any]]:
        self.logger.info("Buscando dados de tags...")
        url = f"{self.config.api_base_url}/tags/openshift"
        resp = self.session.get(
            url,
            params={"limit": limit},
            headers=self._headers(),
            timeout=self.config.timeout,
        )
        resp.raise_for_status()
        tags = resp.json().get("data", [])
        self.logger.info(f"Tags obtidas: {len(tags)}")
        return tags

    # --------------------------------------------------------------------------
    # POWER QUERY MODE - OS Cost Cluster Projects
    # (já estava ok no seu fluxo)
    # --------------------------------------------------------------------------
    def get_cluster_project_costs(self, start_date: str, end_date: str, currency: str = "BRL") -> List[Dict[str, Any]]:
        self.logger.info("Coletando dados Cluster x Project (Power Query mode)...")

        cluster_data = self.get_costs_by_groupby(start_date, end_date, currency).get("cluster", [])

        results: List[Dict[str, Any]] = []
        seen: set = set()

        for item in cluster_data:
            date = item.get("date")
            month = pd.to_datetime(date).strftime("%Y-%m")

            for cluster in item.get("clusters", []):
                cluster_name = cluster.get("cluster")
                if not cluster_name:
                    continue

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

    # --------------------------------------------------------------------------
    # POWER QUERY MODE - OS Cost Project Tags (por projeto)
    # --------------------------------------------------------------------------
    def get_project_tags_by_project(self, project: str, start_date: str, end_date: str) -> List[Dict[str, Any]]:
        # Power Query: get_no_loop_data("?filter[project]=...&filter[resolution]=daily&start_date=...&end_date=...")
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


# ------------------------------------------------------------------------------
# FORMATADOR EXCEL (estrutura preservada)
# ------------------------------------------------------------------------------
class ExcelFormatterFixed:
    def __init__(self, logger, currency: str = "BRL"):
        self.logger = logger
        self.currency = currency

    # --------------------------------------------------------------------------
    # Helpers - flatten (igual PQ “Expanded values ... Expanded cost ...”)
    # --------------------------------------------------------------------------
    def _safe_join_csv(self, v: Any) -> str:
        if isinstance(v, list):
            return ",".join([str(x) for x in v])
        return ""

    def _flatten_values_record(
        self,
        values_record: Dict[str, Any],
        group_by_code: str,
        name: str,
        date: str,
        tag_key: Optional[str] = None,
        delta_value_override: Optional[Any] = None,
        delta_percent_override: Optional[Any] = None,
    ) -> Dict[str, Any]:
        """
        Power Query:
        Expand values → (values.date, classification, source_uuid, clusters, infrastructure, supplementary, cost, delta_*)
        Expand infra/supp/cost subrecords
        """
        row: Dict[str, Any] = {
            "code": self.currency,
            "Group By Code": group_by_code,
            "meta.distributed_overhead": True,  # PQ traz do extract; aqui mantemos compatível
            "date": pd.to_datetime(date),
            "Name": name,
            "values.date": pd.to_datetime(values_record.get("date", date)),
            "values.classification": values_record.get("classification", ""),
            "values.source_uuid": self._safe_join_csv(values_record.get("source_uuid")),
            "values.clusters": self._safe_join_csv(values_record.get("clusters")),
        }

        # Infrastructure
        infra = values_record.get("infrastructure", {}) or {}
        row["values.infrastructure.raw.value"] = (infra.get("raw", {}) or {}).get("value", 0) or 0
        row["values.infrastructure.raw.units"] = (infra.get("raw", {}) or {}).get("units", self.currency) or self.currency
        row["values.infrastructure.markup.value"] = (infra.get("markup", {}) or {}).get("value", 0) or 0
        row["values.infrastructure.markup.units"] = (infra.get("markup", {}) or {}).get("units", self.currency) or self.currency
        row["values.infrastructure.usage.value"] = (infra.get("usage", {}) or {}).get("value", 0) or 0
        row["values.infrastructure.usage.units"] = (infra.get("usage", {}) or {}).get("units", self.currency) or self.currency
        row["values.infrastructure.total.value"] = (infra.get("total", {}) or {}).get("value", 0) or 0
        row["values.infrastructure.total.units"] = (infra.get("total", {}) or {}).get("units", self.currency) or self.currency

        # Supplementary
        supp = values_record.get("supplementary", {}) or {}
        row["values.supplementary.raw.value"] = (supp.get("raw", {}) or {}).get("value", 0) or 0
        row["values.supplementary.raw.units"] = (supp.get("raw", {}) or {}).get("units", self.currency) or self.currency
        row["values.supplementary.markup.value"] = (supp.get("markup", {}) or {}).get("value", 0) or 0
        row["values.supplementary.markup.units"] = (supp.get("markup", {}) or {}).get("units", self.currency) or self.currency
        row["values.supplementary.usage.value"] = (supp.get("usage", {}) or {}).get("value", 0) or 0
        row["values.supplementary.usage.units"] = (supp.get("usage", {}) or {}).get("units", self.currency) or self.currency
        row["values.supplementary.total.value"] = (supp.get("total", {}) or {}).get("value", 0) or 0
        row["values.supplementary.total.units"] = (supp.get("total", {}) or {}).get("units", self.currency) or self.currency

        # Cost
        cost = values_record.get("cost", {}) or {}
        row["values.cost.raw.value"] = (cost.get("raw", {}) or {}).get("value", 0) or 0
        row["values.cost.raw.units"] = (cost.get("raw", {}) or {}).get("units", self.currency) or self.currency
        row["values.cost.markup.value"] = (cost.get("markup", {}) or {}).get("value", 0) or 0
        row["values.cost.markup.units"] = (cost.get("markup", {}) or {}).get("units", self.currency) or self.currency
        row["values.cost.usage.value"] = (cost.get("usage", {}) or {}).get("value", 0) or 0
        row["values.cost.usage.units"] = (cost.get("usage", {}) or {}).get("units", self.currency) or self.currency

        # Project PQ possui também estes campos (se não existir, vira 0)
        row["values.cost.platform_distributed.value"] = (cost.get("platform_distributed", {}) or {}).get("value", 0) or 0
        row["values.cost.platform_distributed.units"] = (cost.get("platform_distributed", {}) or {}).get("units", self.currency) or self.currency
        row["values.cost.worker_unallocated_distributed.value"] = (cost.get("worker_unallocated_distributed", {}) or {}).get("value", 0) or 0
        row["values.cost.worker_unallocated_distributed.units"] = (cost.get("worker_unallocated_distributed", {}) or {}).get("units", self.currency) or self.currency
        row["values.cost.distributed.value"] = (cost.get("distributed", {}) or {}).get("value", 0) or 0
        row["values.cost.distributed.units"] = (cost.get("distributed", {}) or {}).get("units", self.currency) or self.currency

        row["values.cost.total.value"] = (cost.get("total", {}) or {}).get("value", 0) or 0
        row["values.cost.total.units"] = (cost.get("total", {}) or {}).get("units", self.currency) or self.currency

        # Delta fields
        if delta_percent_override is not None:
            row["values.delta_percent"] = delta_percent_override
        else:
            row["values.delta_percent"] = values_record.get("delta_percent", 0) or 0

        if delta_value_override is not None:
            row["values.delta_value"] = delta_value_override
        else:
            row["values.delta_value"] = values_record.get("delta_value", 0) or 0

        # key (só tags)
        row["key"] = tag_key

        return row

    # --------------------------------------------------------------------------
    # ✅ OS Cost Cluster Projects (já corrigido, mantém)
    # --------------------------------------------------------------------------
    def create_os_cost_cluster_projects(self, cluster_project_data: List[Dict[str, Any]]) -> pd.DataFrame:
        rows = []

        for item in cluster_project_data:
            date = item.get("date")
            cluster = item.get("_cluster")

            for project in item.get("projects", []):
                project_name = project.get("project")

                for value in project.get("values", []):
                    total = (value.get("cost", {}) or {}).get("total", {}) or {}
                    total_value = total.get("value", 0) or 0

                    rows.append({
                        "code": self.currency,
                        "Group By Code": "cluster",
                        "cluster": cluster,
                        "date": pd.to_datetime(date),
                        "project": project_name,
                        "value": total_value,
                        "units": self.currency,
                        "Filter Month": pd.to_datetime(date).strftime("%Y-%m"),
                    })

        return pd.DataFrame(rows)

    # --------------------------------------------------------------------------
    # ✅ OS Cost Project Tags (PQ 100% fiel)
    # --------------------------------------------------------------------------
    def create_os_cost_project_tags(
        self,
        all_data: Dict[str, Any],
        client: OpenShiftCostAPIClient,
        start_date: str,
        end_date: str,
    ) -> pd.DataFrame:
        self.logger.info("Gerando OS Cost Project Tags (Power Query mode)...")

        base_rows = []

        # Power Query: Source = Cost_Data_Master_Projects_Daily
        # -> select code/date/project
        # -> date = Date.StartOfMonth
        # -> distinct
        for item in all_data.get("project", []):
            item_date = item.get("date")
            if not item_date:
                continue

            # Date.StartOfMonth
            month_start = pd.to_datetime(item_date).to_period("M").to_timestamp()

            for proj in item.get("projects", []):
                base_rows.append({
                    "project": proj.get("project"),
                    "date": month_start,
                })

        df_base = (
            pd.DataFrame(base_rows)
            .dropna(subset=["project"])
            .drop_duplicates(subset=["project", "date"])
        )

        rows = []

        # PQ: Invoked Custom Function get_no_loop_data(filter[project]=...)
        for _, r in df_base.iterrows():
            proj = r["project"]
            month_start = r["date"]

            tag_data = client.get_project_tags_by_project(proj, start_date, end_date)

            for tag in tag_data:
                tag_key = tag.get("key")
                enabled = tag.get("enabled", True)

                for val in tag.get("values", []) or []:
                    rows.append({
                        "code": self.currency,
                        "date": month_start,
                        "project": proj,
                        "key": tag_key,
                        "values": val,
                        "enabled": enabled,
                        "Filter Month": month_start.strftime("%Y-%m"),
                    })

        return pd.DataFrame(rows)

    # --------------------------------------------------------------------------
    # ✅ OS Costs Daily (Power Query fiel)
    # Table.Combine({Projects, Clusters, Nodes, Tags})
    # --------------------------------------------------------------------------
    def create_os_costs_daily(self, all_data: Dict[str, Any]) -> pd.DataFrame:
        self.logger.info("Gerando OS Costs Daily (Power Query Table.Combine mode)...")

        rows: List[Dict[str, Any]] = []

        # -----------------------------
        # Projects Daily
        # -----------------------------
        for item in all_data.get("project", []):
            date = item.get("date")
            if not date:
                continue

            for proj in item.get("projects", []):
                proj_name = proj.get("project") or ""
                for values_record in proj.get("values", []) or []:
                    row = self._flatten_values_record(
                        values_record=values_record,
                        group_by_code="project",
                        name=proj_name,
                        date=date,
                        tag_key=None,
                        delta_value_override=values_record.get("delta_value", 0),
                        delta_percent_override=values_record.get("delta_percent", 0),
                    )
                    row["Filter Month"] = pd.to_datetime(date).strftime("%Y-%m")
                    rows.append(row)

        # -----------------------------
        # Clusters Daily
        # -----------------------------
        for item in all_data.get("cluster", []):
            date = item.get("date")
            if not date:
                continue

            for cluster in item.get("clusters", []):
                cluster_name = cluster.get("cluster") or ""
                for values_record in cluster.get("values", []) or []:
                    row = self._flatten_values_record(
                        values_record=values_record,
                        group_by_code="cluster",
                        name=cluster_name,
                        date=date,
                        tag_key=None,
                    )
                    row["Filter Month"] = pd.to_datetime(date).strftime("%Y-%m")
                    rows.append(row)

        # -----------------------------
        # Nodes Daily
        # -----------------------------
        for item in all_data.get("node", []):
            date = item.get("date")
            if not date:
                continue

            for cluster in item.get("clusters", []):
                for node in cluster.get("nodes", []) or []:
                    node_name = node.get("node") or ""
                    for values_record in node.get("values", []) or []:
                        row = self._flatten_values_record(
                            values_record=values_record,
                            group_by_code="node",
                            name=node_name,
                            date=date,
                            tag_key=None,
                        )
                        row["Filter Month"] = pd.to_datetime(date).strftime("%Y-%m")
                        rows.append(row)

        # -----------------------------
        # Tags Daily (⚠️ aqui é onde mais dá divergência)
        # PQ faz renome dinâmico do campo da tag → Name
        # e mantém coluna key
        # -----------------------------
        tag_key_name = all_data.get("_tag_key_name", "produto")
        for item in all_data.get("tag", []):
            date = item.get("date")
            if not date:
                continue

            # API geralmente devolve algo como item["produtos"] (plural)
            possible_lists = [
                item.get(f"{tag_key_name}s"),  # ex: produtos
                item.get("tags"),              # fallback
            ]
            tags_list = None
            for lst in possible_lists:
                if isinstance(lst, list):
                    tags_list = lst
                    break
            if not tags_list:
                continue

            for tag_entry in tags_list:
                # o "nome da tag" pode vir em várias chaves dependendo do endpoint:
                # produto, tag, key, value...
                tag_name = (
                    tag_entry.get(tag_key_name)
                    or tag_entry.get("tag")
                    or tag_entry.get("value")
                    or tag_entry.get("key")
                    or ""
                )

                for values_record in tag_entry.get("values", []) or []:
                    row = self._flatten_values_record(
                        values_record=values_record,
                        group_by_code="tag",
                        name=tag_name,
                        date=date,
                        tag_key=tag_key_name,  # PQ: coluna 'key'
                        delta_value_override=values_record.get("delta_value", 0),
                        delta_percent_override=values_record.get("delta_percent", 0),
                    )
                    row["Filter Month"] = pd.to_datetime(date).strftime("%Y-%m")
                    rows.append(row)

        df = pd.DataFrame(rows)

        # (Opcional defensivo) garantir colunas mínimas mesmo se alguma fonte vier vazia
        if df.empty:
            df = pd.DataFrame(columns=[
                "code", "Group By Code", "meta.distributed_overhead", "date", "Name",
                "values.date", "values.classification", "values.source_uuid", "values.clusters",
                "values.infrastructure.raw.value", "values.infrastructure.raw.units",
                "values.infrastructure.markup.value", "values.infrastructure.markup.units",
                "values.infrastructure.usage.value", "values.infrastructure.usage.units",
                "values.infrastructure.total.value", "values.infrastructure.total.units",
                "values.supplementary.raw.value", "values.supplementary.raw.units",
                "values.supplementary.markup.value", "values.supplementary.markup.units",
                "values.supplementary.usage.value", "values.supplementary.usage.units",
                "values.supplementary.total.value", "values.supplementary.total.units",
                "values.cost.raw.value", "values.cost.raw.units",
                "values.cost.markup.value", "values.cost.markup.units",
                "values.cost.usage.value", "values.cost.usage.units",
                "values.cost.platform_distributed.value", "values.cost.platform_distributed.units",
                "values.cost.worker_unallocated_distributed.value", "values.cost.worker_unallocated_distributed.units",
                "values.cost.distributed.value", "values.cost.distributed.units",
                "values.cost.total.value", "values.cost.total.units",
                "values.delta_value", "values.delta_percent",
                "key", "Filter Month",
            ])

        return df

    # --------------------------------------------------------------------------
    # GERA EXCEL (estrutura preservada)
    # --------------------------------------------------------------------------
    def format_to_excel(
        self,
        all_data: Dict[str, Any],
        cluster_project_data: List[Dict[str, Any]],
        output_file: str,
        start_date: str,
        end_date: str,
        tags: List[Dict[str, Any]],
        client: OpenShiftCostAPIClient,
    ):
        self.logger.info("Formatando dados para Excel...")

        with pd.ExcelWriter(output_file, engine="openpyxl") as writer:
            # Data_Period (estrutura original preservada)
            df_period = pd.DataFrame({
                "Start Date": ["Start Date", start_date, None, None, None],
                "End Date": ["End Date", end_date, None, None, None],
                "Col3": [None, None, None, None, None],
                "Col4": [None, None, None, None, None],
                "Col5": [None, None, None, None, None],
                "Col6": [None, None, None, None, None],
                "Col7": [None, None, None, None, None],
                "Guidelines": [
                    None,
                    "Guidelines",
                    "Enter start and end dates from the same month.",
                    "If the need is to get data from multiple months, make copies of the file to ensure start and end date are from the same month.",
                    "The date range should be no earlier to 4 months prior to the current month.",
                ],
            })
            df_period.to_excel(writer, sheet_name="Data_Period", index=False, header=False)

            # Default Master Settings
            df_settings = pd.DataFrame({
                "code": ["BRL"],
                "name": ["Brazilian Real"],
                "symbol": ["R$"],
                "description": ["BRL (R$) - Brazilian Real"],
                "Default_Configurations.data.currency": ["BRL"],
                "Default_Configurations.data.cost_type": ["calculated_amortized_cost"],
            })
            df_settings.to_excel(writer, sheet_name="Default Master Settings", index=False)

            # Project Overhead Cost Types
            df_cost_types = pd.DataFrame({
                "Code": ["cost", "distributed_cost"],
                "Description": ["Don't distribute overhead costs", "Distribute through cost models"],
            })
            df_cost_types.to_excel(writer, sheet_name="Project Overhead Cost Types", index=False)

            # OpenShift Group Bys
            df_group_bys = pd.DataFrame({
                "Group By": ["Cluster", "Node", "Project", "Tag"],
                "Group By Code": ["cluster", "node", "project", "tag"],
            })
            df_group_bys.to_excel(writer, sheet_name="OpenShift Group Bys", index=False)

            # OS Tag Keys (estrutura preservada)
            tag_data = []
            for t in tags:
                tag_data.append({"count": 1, "key": t.get("key", "produto"), "enabled": True, "Group By": "tag"})
            if not tag_data:
                tag_data = [{"count": 1, "key": "produto", "enabled": True, "Group By": "tag"}]
            pd.DataFrame(tag_data).to_excel(writer, sheet_name="OS Tag Keys", index=False)

            # OS Cost Cluster Projects (CORRIGIDO)
            self.create_os_cost_cluster_projects(cluster_project_data).to_excel(
                writer, sheet_name="OS Cost Cluster Projects", index=False
            )

            # OS Cost Project Tags (CORRIGIDO)
            self.create_os_cost_project_tags(all_data, client, start_date, end_date).to_excel(
                writer, sheet_name="OS Cost Project Tags", index=False
            )

            # OS Costs Daily (CORRIGIDO)
            self.create_os_costs_daily(all_data).to_excel(
                writer, sheet_name="OS Costs Daily", index=False
            )

        self.logger.info(f"Excel gerado com sucesso: {output_file}")


# ------------------------------------------------------------------------------
# MAIN
# ------------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="OpenShift Cost Management - Extrator de Dados (PQ MODE)")
    parser.add_argument("--start-date", type=str, help="Data inicial (YYYY-MM-DD)", default=None)
    parser.add_argument("--end-date", type=str, help="Data final (YYYY-MM-DD)", default=None)
    parser.add_argument("--output", type=str, help="Arquivo de saída", default="openshift_costs.xlsx")
    parser.add_argument("--currency", type=str, help="Moeda", default="BRL")
    args = parser.parse_args()

    if not args.end_date:
        args.end_date = datetime.now().strftime("%Y-%m-%d")
    if not args.start_date:
        args.start_date = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")

    logger.info("Iniciando extração de dados do OpenShift Cost Management")
    logger.info(f"Período: {args.start_date} a {args.end_date}")
    logger.info(f"Moeda: {args.currency}")

    try:
        config = APIConfig()
        client = OpenShiftCostAPIClient(config, logger)

        all_data = client.get_costs_by_groupby(args.start_date, args.end_date, args.currency)
        cluster_project_data = client.get_cluster_project_costs(args.start_date, args.end_date, args.currency)
        tags = client.get_tags()

        formatter = ExcelFormatterFixed(logger, args.currency)
        formatter.format_to_excel(
            all_data=all_data,
            cluster_project_data=cluster_project_data,
            output_file=args.output,
            start_date=args.start_date,
            end_date=args.end_date,
            tags=tags,
            client=client,  # ✅ agora existe e não dá NameError
        )

        logger.info("Processo concluído com sucesso!")
    except Exception as e:
        logger.error(f"Erro durante a execução: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
