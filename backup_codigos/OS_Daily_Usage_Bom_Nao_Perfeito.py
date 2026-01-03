#!/usr/bin/env python3.11
"""
OpenShift Cost Management - Extrator de Dados CORRIGIDO
✔ Estrutura do Excel preservada
✔ Abas preservadas
✔ Correções baseadas 100% no Power Query
✔ Inclui OS Daily Usage (Project/Cluster/Node/Tag) conforme PQ
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
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
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
        self.access_token: Optional[str] = None
        self.token_expires_at: Optional[datetime] = None
        self.session = self._create_session()

    def _create_session(self) -> requests.Session:
        session = requests.Session()
        retry = Retry(
            total=self.config.max_retries,
            backoff_factor=self.config.backoff_factor,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["HEAD", "GET", "OPTIONS", "POST"]
        )
        adapter = HTTPAdapter(max_retries=retry)
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        return session

    def _get_token(self) -> str:
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
        self.token_expires_at = datetime.now() + timedelta(seconds=data.get("expires_in", 900) - 60)
        self.logger.info("Token obtido com sucesso")
        return self.access_token

    def _headers(self) -> Dict[str, str]:
        return {
            "Authorization": f"Bearer {self._get_token()}",
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

    # --------------------------------------------------------------------------
    # COSTS (mantido)
    # --------------------------------------------------------------------------
    def get_costs_by_groupby(self, start_date: str, end_date: str, currency: str = "BRL") -> Dict[str, List[Dict[str, Any]]]:
        all_data: Dict[str, List[Dict[str, Any]]] = {}
        group_by_configs = [
            {"type": "cluster", "params": {"group_by[cluster]": "*"}},
            {"type": "node", "params": {"group_by[cluster]": "*", "group_by[node]": "*"}},
            {"type": "project", "params": {"group_by[project]": "*"}},
            {"type": "tag:produto", "params": {"group_by[tag:produto]": "*"}},
        ]

        for group_by_config in group_by_configs:
            group_by_type = group_by_config["type"]
            group_by_params = group_by_config["params"]

            try:
                self.logger.info(f"Buscando dados agrupados por {group_by_type}...")
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
                    params.update(group_by_params)

                    response = self.session.get(
                        url,
                        params=params,
                        headers=self._headers(),
                        timeout=self.config.timeout
                    )
                    response.raise_for_status()
                    data = response.json()

                    items = data.get("data", [])
                    meta = data.get("meta", {})

                    if not items:
                        break

                    all_items.extend(items)

                    count = meta.get("count", 0)
                    offset_next = offset + limit

                    if count <= offset_next:
                        break

                    offset = offset_next
                    page += 1

                if "tag" in group_by_type:
                    key = "tag"
                    tag_key_name = group_by_type.split(":")[1]
                    all_data.setdefault(key, [])
                    all_data["_tag_key_name"] = tag_key_name
                    all_data[key].extend(all_items)
                else:
                    key = group_by_type
                    all_data.setdefault(key, [])
                    all_data[key].extend(all_items)

                self.logger.info(f"Obtidos {len(all_items)} registros para {group_by_type} ({page} paginas)")
            except Exception as e:
                self.logger.warning(f"Aviso: Falha ao obter dados para {group_by_type}: {e}")

        for k in ["cluster", "node", "project", "tag"]:
            all_data.setdefault(k, [])

        return all_data

    def get_tags(self, limit: int = 1000) -> List[Dict[str, Any]]:
        try:
            self.logger.info("Buscando dados de tags...")
            url = f"{self.config.api_base_url}/tags/openshift"
            resp = self.session.get(
                url,
                params={"limit": limit},
                headers=self._headers(),
                timeout=self.config.timeout,
            )
            resp.raise_for_status()
            data = resp.json()
            tags = data.get("data", [])
            self.logger.info(f"Tags obtidas: {len(tags)}")
            return tags
        except Exception as e:
            self.logger.error(f"Erro ao obter tags: {e}")
            return []

    # --------------------------------------------------------------------------
    # USAGE (POWER QUERY MODE)
    # Endpoint base (por padrão): /reports/openshift/{usage_code}/
    # --------------------------------------------------------------------------
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
        Busca usage paginado, devolvendo:
          { "meta": {...}, "data": [...] }
        """
        all_items: List[Dict[str, Any]] = []
        offset = 0
        last_meta: Dict[str, Any] = {}

        # group_by[tag:key]=* ou group_by[project]=*
        if group_by_code == "tag":
            if not tag_key:
                raise ValueError("tag_key é obrigatório quando group_by_code == 'tag'")
            group_by_param_key = f"group_by[tag:{tag_key}]"
        else:
            group_by_param_key = f"group_by[{group_by_code}]"

        while True:
            url = f"{self.config.api_base_url}/reports/openshift/{usage_code}/"
            params = {
                "filter[limit]": limit,
                "filter[offset]": offset,
                "filter[resolution]": "daily",
                "start_date": start_date,
                "end_date": end_date,
                group_by_param_key: "*",
            }

            resp = self.session.get(
                url,
                headers=self._headers(),
                params=params,
                timeout=self.config.timeout,
            )
            resp.raise_for_status()
            payload = resp.json()

            page_data = payload.get("data", [])
            meta = payload.get("meta", {})

            if not page_data:
                last_meta = meta
                break

            all_items.extend(page_data)
            last_meta = meta

            count = meta.get("count", 0)
            offset += limit
            if offset >= count:
                break

        return {"meta": last_meta or {}, "data": all_items}

# ------------------------------------------------------------------------------
# FORMATADOR EXCEL
# ------------------------------------------------------------------------------
class ExcelFormatterFixed:
    def __init__(self, logger, currency: str = "BRL"):
        self.logger = logger
        self.currency = currency

    # --------------------------------------------------------------------------
    # Utils
    # --------------------------------------------------------------------------
    def _to_list_str(self, v) -> str:
        if isinstance(v, list):
            return ",".join([str(x) for x in v])
        return ""

    # --------------------------------------------------------------------------
    # COSTS DAILY (já existia no seu código)
    # --------------------------------------------------------------------------
    def _flatten_cost_data(self, cost_item: Dict, group_by_type: str, item_name: str, date: str) -> Dict:
        row = {
            "code": self.currency,
            "Group By Code": group_by_type,
            "meta.distributed_overhead": True,
            "date": pd.to_datetime(date),
            "Name": item_name,
            "values.date": pd.to_datetime(date),
            "values.classification": cost_item.get("classification", ""),
            "values.source_uuid": self._to_list_str(cost_item.get("source_uuid", [])),
            "values.clusters": self._to_list_str(cost_item.get("clusters", [])),
        }

        infra = cost_item.get("infrastructure", {})
        row["values.infrastructure.raw.value"] = infra.get("raw", {}).get("value", 0)
        row["values.infrastructure.raw.units"] = infra.get("raw", {}).get("units", "BRL")
        row["values.infrastructure.markup.value"] = infra.get("markup", {}).get("value", 0)
        row["values.infrastructure.markup.units"] = infra.get("markup", {}).get("units", "BRL")
        row["values.infrastructure.usage.value"] = infra.get("usage", {}).get("value", 0)
        row["values.infrastructure.usage.units"] = infra.get("usage", {}).get("units", "BRL")
        row["values.infrastructure.total.value"] = infra.get("total", {}).get("value", 0)
        row["values.infrastructure.total.units"] = infra.get("total", {}).get("units", "BRL")

        supp = cost_item.get("supplementary", {})
        row["values.supplementary.raw.value"] = supp.get("raw", {}).get("value", 0)
        row["values.supplementary.raw.units"] = supp.get("raw", {}).get("units", "BRL")
        row["values.supplementary.markup.value"] = supp.get("markup", {}).get("value", 0)
        row["values.supplementary.markup.units"] = supp.get("markup", {}).get("units", "BRL")
        row["values.supplementary.usage.value"] = supp.get("usage", {}).get("value", 0)
        row["values.supplementary.usage.units"] = supp.get("usage", {}).get("units", "BRL")
        row["values.supplementary.total.value"] = supp.get("total", {}).get("value", 0)
        row["values.supplementary.total.units"] = supp.get("total", {}).get("units", "BRL")

        cost = cost_item.get("cost", {})
        row["values.cost.raw.value"] = cost.get("raw", {}).get("value", 0)
        row["values.cost.raw.units"] = cost.get("raw", {}).get("units", "BRL")
        row["values.cost.markup.value"] = cost.get("markup", {}).get("value", 0)
        row["values.cost.markup.units"] = cost.get("markup", {}).get("units", "BRL")
        row["values.cost.usage.value"] = cost.get("usage", {}).get("value", 0)
        row["values.cost.usage.units"] = cost.get("usage", {}).get("units", "BRL")
        row["values.cost.platform_distributed.value"] = cost.get("platform_distributed", {}).get("value", 0)
        row["values.cost.platform_distributed.units"] = cost.get("platform_distributed", {}).get("units", "BRL")
        row["values.cost.worker_unallocated_distributed.value"] = cost.get("worker_unallocated_distributed", {}).get("value", 0)
        row["values.cost.worker_unallocated_distributed.units"] = cost.get("worker_unallocated_distributed", {}).get("units", "BRL")
        row["values.cost.distributed.value"] = cost.get("distributed", {}).get("value", 0)
        row["values.cost.distributed.units"] = cost.get("distributed", {}).get("units", "BRL")
        row["values.cost.total.value"] = cost.get("total", {}).get("value", 0)
        row["values.cost.total.units"] = cost.get("total", {}).get("units", "BRL")

        row["values.delta_percent"] = cost_item.get("delta_percent", 0)
        row["key"] = cost_item.get("key", None)
        row["values.delta_value"] = cost_item.get("delta_value", 0)
        return row

    def _create_os_costs_daily(self, all_data: Dict) -> pd.DataFrame:
        rows = []

        for cost_item in all_data.get("project", []):
            date = cost_item.get("date", "")
            for project in cost_item.get("projects", []):
                project_name = project.get("project", "")
                for value_item in project.get("values", []):
                    row = self._flatten_cost_data(value_item, "project", project_name, date)
                    row["Filter Month"] = pd.to_datetime(date).strftime("%Y-%m")
                    rows.append(row)

        for cost_item in all_data.get("cluster", []):
            date = cost_item.get("date", "")
            for cluster in cost_item.get("clusters", []):
                cluster_name = cluster.get("cluster", "")
                for value_item in cluster.get("values", []):
                    row = self._flatten_cost_data(value_item, "cluster", cluster_name, date)
                    row["Filter Month"] = pd.to_datetime(date).strftime("%Y-%m")
                    rows.append(row)

        for cost_item in all_data.get("node", []):
            date = cost_item.get("date", "")
            for cluster in cost_item.get("clusters", []):
                for node in cluster.get("nodes", []):
                    node_name = node.get("node", "No-node")
                    for value_item in node.get("values", []):
                        row = self._flatten_cost_data(value_item, "node", node_name, date)
                        row["Filter Month"] = pd.to_datetime(date).strftime("%Y-%m")
                        rows.append(row)

        tag_key_name = all_data.get("_tag_key_name", "produto")
        for cost_item in all_data.get("tag", []):
            date = cost_item.get("date", "")
            tags_list = cost_item.get(f"{tag_key_name}s", [])
            for tag in tags_list:
                tag_name = tag.get("tag", "No-tag")
                for value_item in tag.get("values", []):
                    row = self._flatten_cost_data(value_item, "tag", tag_name, date)
                    row["Filter Month"] = pd.to_datetime(date).strftime("%Y-%m")
                    rows.append(row)

        return pd.DataFrame(rows)

    # --------------------------------------------------------------------------
    # ✅ OS DAILY USAGE (Power Query 100% fiel)
    # --------------------------------------------------------------------------
    USAGE_CATALOG: List[Tuple[str, str]] = [
        ("compute", "Compute"),
        ("memory", "Memory"),
        ("volumes", "Volumes"),
    ]

    def _flatten_usage_value(self, v: Dict[str, Any], prefix: str) -> Dict[str, Any]:
        if not isinstance(v, dict):
            return {
                f"{prefix}.value": None,
                f"{prefix}.units": None,
            }
        return {
            f"{prefix}.value": v.get("value"),
            f"{prefix}.units": v.get("units"),
        }

    def _flatten_usage_request(self, v: Dict[str, Any]) -> Dict[str, Any]:
        if not isinstance(v, dict):
            return {
                "values.request.value": None,
                "values.request.units": None,
                "values.request.unused": None,
                "values.request.unused_percent": None,
            }
        return {
            "values.request.value": v.get("value"),
            "values.request.units": v.get("units"),
            "values.request.unused": v.get("unused"),
            "values.request.unused_percent": v.get("unused_percent"),
        }

    def _flatten_usage_capacity(self, v: Dict[str, Any]) -> Dict[str, Any]:
        if not isinstance(v, dict):
            return {
                "values.capacity.value": None,
                "values.capacity.units": None,
                "values.capacity.unused": None,
                "values.capacity.unused_percent": None,
                "values.capacity.count": None,
                "values.capacity.count_units": None,
            }
        return {
            "values.capacity.value": v.get("value"),
            "values.capacity.units": v.get("units"),
            "values.capacity.unused": v.get("unused"),
            "values.capacity.unused_percent": v.get("unused_percent"),
            "values.capacity.count": v.get("count"),
            "values.capacity.count_units": v.get("count_units"),
        }

    def _usage_rows_from_report(
        self,
        report: Dict[str, Any],
        group_by: str,
        group_by_code: str,
        usage_code: str,
        usage_name: str,
        key: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """
        Converte o payload do /reports/openshift/{usage_code}/ em linhas “master”
        no estilo Usage_Data_Daily_Extract (antes do expand do PQ).
        """
        meta = report.get("meta", {}) or {}
        data = report.get("data", []) or []

        return [{
            "Group By": group_by,
            "Group By Code": group_by_code,
            "Usage Code": usage_code,
            "Usage Name": usage_name,
            "Key": key,
            "meta.count": meta.get("count"),
            "meta.currency": meta.get("currency"),
            "data": data,
        }]

    def _expand_usage_project(self, df_extract: pd.DataFrame) -> pd.DataFrame:
        rows = []
        for _, r in df_extract.iterrows():
            for day in r["data"]:
                day_date = pd.to_datetime(day.get("date"))
                for proj in day.get("projects", []) or []:
                    name = proj.get("project")
                    if name is None:
                        continue
                    for val in proj.get("values", []) or []:
                        base = {
                            "Group By": r["Group By"],
                            "Group By Code": r["Group By Code"],
                            "Usage Code": r["Usage Code"],
                            "Usage Name": r["Usage Name"],
                            "Key": r["Key"],
                            "meta.count": r["meta.count"],
                            "meta.currency": r["meta.currency"],
                            "date": day_date,
                            "Name": name,
                        }
                        base.update(self._flatten_usage_value(val.get("usage"), "values.usage"))
                        base.update(self._flatten_usage_request(val.get("request")))
                        base.update(self._flatten_usage_value(val.get("limit"), "values.limit"))
                        base.update(self._flatten_usage_capacity(val.get("capacity")))
                        rows.append(base)
        return pd.DataFrame(rows)

    def _expand_usage_cluster(self, df_extract: pd.DataFrame) -> pd.DataFrame:
        rows = []
        for _, r in df_extract.iterrows():
            for day in r["data"]:
                day_date = pd.to_datetime(day.get("date"))
                for cl in day.get("clusters", []) or []:
                    name = cl.get("cluster")
                    if name is None:
                        continue
                    for val in cl.get("values", []) or []:
                        base = {
                            "Group By": r["Group By"],
                            "Group By Code": r["Group By Code"],
                            "Usage Code": r["Usage Code"],
                            "Usage Name": r["Usage Name"],
                            "Key": r["Key"],
                            "meta.count": r["meta.count"],
                            "meta.currency": r["meta.currency"],
                            "date": day_date,
                            "Name": name,
                        }
                        base.update(self._flatten_usage_value(val.get("usage"), "values.usage"))
                        base.update(self._flatten_usage_request(val.get("request")))
                        base.update(self._flatten_usage_value(val.get("limit"), "values.limit"))
                        base.update(self._flatten_usage_capacity(val.get("capacity")))
                        rows.append(base)
        return pd.DataFrame(rows)

    def _expand_usage_node(self, df_extract: pd.DataFrame) -> pd.DataFrame:
        rows = []
        for _, r in df_extract.iterrows():
            for day in r["data"]:
                day_date = pd.to_datetime(day.get("date"))
                for nd in day.get("nodes", []) or []:
                    name = nd.get("node")
                    if name is None:
                        continue
                    for val in nd.get("values", []) or []:
                        base = {
                            "Group By": r["Group By"],
                            "Group By Code": r["Group By Code"],
                            "Usage Code": r["Usage Code"],
                            "Usage Name": r["Usage Name"],
                            "Key": r["Key"],
                            "meta.count": r["meta.count"],
                            "meta.currency": r["meta.currency"],
                            "date": day_date,
                            "Name": name,
                        }
                        base.update(self._flatten_usage_value(val.get("usage"), "values.usage"))
                        base.update(self._flatten_usage_request(val.get("request")))
                        base.update(self._flatten_usage_value(val.get("limit"), "values.limit"))
                        base.update(self._flatten_usage_capacity(val.get("capacity")))
                        rows.append(base)
        return pd.DataFrame(rows)

    def _expand_usage_tag(self, df_extract: pd.DataFrame) -> pd.DataFrame:
        rows = []
        for _, r in df_extract.iterrows():
            key = r["Key"]
            if not key:
                continue
            plural = f"{key}s"

            for day in r["data"]:
                day_date = pd.to_datetime(day.get("date"))

                # PQ: replace_field_name(data, key+'s', 'Tag Record') -> aqui é só pegar plural
                tag_records = day.get(plural)

                # fallback (se API devolver num nome diferente)
                if tag_records is None:
                    # tenta achar o único campo listável além de 'date'
                    for k, v in day.items():
                        if k != "date" and isinstance(v, list):
                            tag_records = v
                            break

                if not tag_records:
                    continue

                for tag_rec in tag_records:
                    if tag_rec is None:
                        continue

                    # PQ: replace_field_name(Tag Record, Key, 'Tag Name')
                    # Pode ser {produto: 'x', values:[...]} ou {tag:'x', values:[...]}
                    tag_name = tag_rec.get(key)
                    if tag_name is None:
                        tag_name = tag_rec.get("tag")  # compat com costs antigo

                    if tag_name is None:
                        continue

                    for val in tag_rec.get("values", []) or []:
                        base = {
                            "Group By": r["Group By"],
                            "Group By Code": r["Group By Code"],
                            "Usage Code": r["Usage Code"],
                            "Usage Name": r["Usage Name"],
                            "Key": key,
                            "meta.count": r["meta.count"],
                            "meta.currency": r["meta.currency"],
                            "date": day_date,
                            "Name": tag_name,
                        }
                        base.update(self._flatten_usage_value(val.get("usage"), "values.usage"))
                        base.update(self._flatten_usage_request(val.get("request")))
                        base.update(self._flatten_usage_value(val.get("limit"), "values.limit"))
                        base.update(self._flatten_usage_capacity(val.get("capacity")))
                        rows.append(base)

        return pd.DataFrame(rows)

    def create_os_daily_usage(
        self,
        client: OpenShiftCostAPIClient,
        start_date: str,
        end_date: str,
        tag_keys: List[str],
    ) -> pd.DataFrame:
        """
        Replica:
          OS Daily Usage = Combine({Projects, Clusters, Nodes, Tags})
          Remove Filter_Start/Filter_End (não existem no output final)
        """
        self.logger.info("Gerando OS Daily Usage (Power Query mode)...")

        extract_rows = []

        # PROJECT
        for usage_code, usage_name in self.USAGE_CATALOG:
            report = client.get_usage_report(
                usage_code=usage_code,
                start_date=start_date,
                end_date=end_date,
                group_by_code="project",
                tag_key=None,
            )
            extract_rows.extend(self._usage_rows_from_report(
                report=report,
                group_by="Project",
                group_by_code="project",
                usage_code=usage_code,
                usage_name=usage_name,
                key=None
            ))

        # CLUSTER
        for usage_code, usage_name in self.USAGE_CATALOG:
            report = client.get_usage_report(
                usage_code=usage_code,
                start_date=start_date,
                end_date=end_date,
                group_by_code="cluster",
                tag_key=None,
            )
            extract_rows.extend(self._usage_rows_from_report(
                report=report,
                group_by="Cluster",
                group_by_code="cluster",
                usage_code=usage_code,
                usage_name=usage_name,
                key=None
            ))

        # NODE
        for usage_code, usage_name in self.USAGE_CATALOG:
            report = client.get_usage_report(
                usage_code=usage_code,
                start_date=start_date,
                end_date=end_date,
                group_by_code="node",
                tag_key=None,
            )
            extract_rows.extend(self._usage_rows_from_report(
                report=report,
                group_by="Node",
                group_by_code="node",
                usage_code=usage_code,
                usage_name=usage_name,
                key=None
            ))

        # TAG
        for tag_key in tag_keys or ["produto"]:
            for usage_code, usage_name in self.USAGE_CATALOG:
                report = client.get_usage_report(
                    usage_code=usage_code,
                    start_date=start_date,
                    end_date=end_date,
                    group_by_code="tag",
                    tag_key=tag_key,
                )
                extract_rows.extend(self._usage_rows_from_report(
                    report=report,
                    group_by="Tag",
                    group_by_code="tag",
                    usage_code=usage_code,
                    usage_name=usage_name,
                    key=tag_key
                ))

        df_extract = pd.DataFrame(extract_rows)

        # Se por algum motivo não veio nada, retorna DF vazio com colunas finais
        final_cols = [
            "Group By", "Group By Code", "Usage Code", "Usage Name", "Key",
            "meta.count", "meta.currency", "date", "Name",
            "values.usage.value", "values.usage.units",
            "values.request.value", "values.request.units", "values.request.unused", "values.request.unused_percent",
            "values.limit.value", "values.limit.units",
            "values.capacity.value", "values.capacity.units", "values.capacity.unused", "values.capacity.unused_percent",
            "values.capacity.count", "values.capacity.count_units",
        ]
        if df_extract.empty:
            return pd.DataFrame(columns=final_cols)

        # Expand conforme PQ
        df_projects = self._expand_usage_project(df_extract[df_extract["Group By"] == "Project"].reset_index(drop=True))
        df_clusters = self._expand_usage_cluster(df_extract[df_extract["Group By"] == "Cluster"].reset_index(drop=True))
        df_nodes = self._expand_usage_node(df_extract[df_extract["Group By"] == "Node"].reset_index(drop=True))
        df_tags = self._expand_usage_tag(df_extract[df_extract["Group By"] == "Tag"].reset_index(drop=True))

        df_final = pd.concat([df_projects, df_clusters, df_nodes, df_tags], ignore_index=True)

        # Garantir colunas e ordem exatamente como no Excel modelo
        for c in final_cols:
            if c not in df_final.columns:
                df_final[c] = None
        df_final = df_final[final_cols]

        return df_final

    # --------------------------------------------------------------------------
    # (Mantido no seu código) - OS Cost Cluster Projects / OS Cost Project Tags
    # OBS: aqui você já tinha versões “corrigidas” em outras etapas; mantive as suas
    # --------------------------------------------------------------------------
    def _create_os_cost_cluster_projects(self, all_data: Dict) -> pd.DataFrame:
        rows = []
        for cost_item in all_data.get("project", []):
            date = cost_item.get("date", "")
            projects = cost_item.get("projects", [])
            for project in projects:
                project_name = project.get("project", "")
                for value_item in project.get("values", []):
                    total_cost = value_item.get("cost", {}).get("total", {}).get("value", 0)
                    rows.append({
                        "code": self.currency,
                        "Group By Code": "cluster",
                        "cluster": cost_item.get("cluster", ""),  # se você já corrigiu isso antes, ok
                        "date": pd.to_datetime(date),
                        "project": project_name,
                        "value": total_cost,
                        "units": self.currency,
                        "Filter Month": pd.to_datetime(date).strftime("%Y-%m"),
                    })
        return pd.DataFrame(rows)

    def _create_os_cost_project_tags(self, all_data: Dict) -> pd.DataFrame:
        rows = []
        tag_key_name = all_data.get("_tag_key_name", "produto")

        for cost_item in all_data.get("tag", []):
            date = cost_item.get("date", "")
            tags_list = cost_item.get(f"{tag_key_name}s", [])
            for tag in tags_list:
                tag_name = tag.get("tag", "")
                for value_item in tag.get("values", []):
                    project_name = value_item.get("project", "")
                    rows.append({
                        "code": self.currency,
                        "date": pd.to_datetime(date),
                        "project": project_name,
                        "key": tag_key_name,
                        "values": tag_name,
                        "enabled": True,
                        "Filter Month": pd.to_datetime(date).strftime("%Y-%m"),
                    })

        return pd.DataFrame(rows)

    # --------------------------------------------------------------------------
    # GERA EXCEL (estrutura preservada)
    # --------------------------------------------------------------------------
    def format_to_excel(
        self,
        all_data: Dict,
        output_file: str,
        start_date: str,
        end_date: str,
        tags: List[Dict[str, Any]],
        client: OpenShiftCostAPIClient,  # ✅ agora passa client (sem NameError)
    ):
        self.logger.info("Formatando dados para Excel...")

        # Tag keys (para usage tag)
        tag_keys = [t.get("key") for t in (tags or []) if t.get("key")] or ["produto"]

        with pd.ExcelWriter(output_file, engine="openpyxl") as writer:
            # Data_Period (modelo completo)
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
                "code": [self.currency],
                "name": ["Brazilian Real" if self.currency == "BRL" else self.currency],
                "symbol": ["R$" if self.currency == "BRL" else self.currency],
                "description": [f"{self.currency} - Currency"],
                "Default_Configurations.data.currency": [self.currency],
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
                "Join_Seq": [1, 1, 1, 1],  # ajuda se você tiver joins no Excel
            })
            df_group_bys.to_excel(writer, sheet_name="OpenShift Group Bys", index=False)

            # OS Tag Keys
            tag_data = [{"count": 1, "key": k, "enabled": True, "Group By": "tag"} for k in tag_keys]
            df_tags = pd.DataFrame(tag_data)
            df_tags.to_excel(writer, sheet_name="OS Tag Keys", index=False)

            # Usage (sheet auxiliar do seu modelo)
            df_usage = pd.DataFrame({
                "code": [c for c, _ in self.USAGE_CATALOG],
                "Name": [n for _, n in self.USAGE_CATALOG],
                "Join Seq": [1] * len(self.USAGE_CATALOG),
            })
            df_usage.to_excel(writer, sheet_name="Usage", index=False)

            # OS Cost Cluster Projects
            df_cluster_projects = self._create_os_cost_cluster_projects(all_data)
            df_cluster_projects.to_excel(writer, sheet_name="OS Cost Cluster Projects", index=False)

            # OS Cost Project Tags
            df_project_tags = self._create_os_cost_project_tags(all_data)
            df_project_tags.to_excel(writer, sheet_name="OS Cost Project Tags", index=False)

            # OS Costs Daily
            df_daily = self._create_os_costs_daily(all_data)
            df_daily.to_excel(writer, sheet_name="OS Costs Daily", index=False)

            # ✅ OS Daily Usage (NOVO - Power Query)
            df_usage_daily = self.create_os_daily_usage(
                client=client,
                start_date=start_date,
                end_date=end_date,
                tag_keys=tag_keys,
            )
            df_usage_daily.to_excel(writer, sheet_name="OS Daily Usage", index=False)

        self.logger.info(f"Excel gerado com sucesso: {output_file}")

# ------------------------------------------------------------------------------
# MAIN
# ------------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="OpenShift Cost Management - Extrator de Dados")
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
        tags = client.get_tags()

        formatter = ExcelFormatterFixed(logger, args.currency)
        formatter.format_to_excel(
            all_data=all_data,
            output_file=args.output,
            start_date=args.start_date,
            end_date=args.end_date,
            tags=tags,
            client=client,  # ✅ passa o client aqui
        )

        logger.info("Processo concluído com sucesso!")
    except Exception as e:
        logger.error(f"Erro durante a execução: {e}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main()
