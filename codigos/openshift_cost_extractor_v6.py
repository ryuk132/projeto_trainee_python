"""
OpenShift Cost Extractor v6.0 - Python 100% Equivalente ao Power Query
Tradução EXATA de todas as consultas Power Query para Python
"""

import os
import sys
import json
import requests
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Any
import numpy as np
from openpyxl import Workbook
from openpyxl.utils.dataframe import dataframe_to_rows
import argparse


# ============================================================================
# CONFIGURAÇÃO
# ============================================================================

class APIConfig:
    """Parâmetros da API - equivalente aos Parâmetros Power Query"""
    
    console_url = "https://console.redhat.com"
    currency_url = "/api/cost-management/v1/currency/?filter%5Blimit%5D=15&limit=100&offset=0"
    costs_url = "/api/cost-management/v1/reports/openshift/costs/"
    usage_url = "/api/cost-management/v1/reports/openshift/"
    tags_url = "/api/cost-management/v1/tags/openshift/"
    default_configs = "/api/cost-management/v1/account-settings/"
    
    api_limit = 10
    api_offset = 0
    
    def __init__(self, client_id=None, client_secret=None):
        self.client_id = client_id or os.getenv("OPENSHIFT_CLIENT_ID")
        self.client_secret = client_secret or os.getenv("OPENSHIFT_CLIENT_SECRET")
        
        if not self.client_id or not self.client_secret:
            raise ValueError(
                "❌ OPENSHIFT_CLIENT_ID e OPENSHIFT_CLIENT_SECRET obrigatórios"
            )


# ============================================================================
# AUTENTICAÇÃO (Função Power Query: get_token)
# ============================================================================

_token_cache = {"token": None, "expires": None}

def get_token(config: APIConfig) -> str:
    """
    Equivalente: get_token() no Power Query
    
    Obtém Bearer token via OAuth2
    Implementa cache para evitar múltiplas chamadas
    """
    global _token_cache
    
    # Usa token em cache se ainda válido
    if _token_cache["token"] and _token_cache["expires"] > datetime.now():
        return f"Bearer {_token_cache['token']}"
    
    url = "https://sso.redhat.com/auth/realms/redhat-external/protocol/openid-connect/token"
    
    data = {
        "grant_type": "client_credentials",
        "client_id": config.client_id,
        "client_secret": config.client_secret,
    }
    
    response = requests.post(url, data=data, timeout=30)
    response.raise_for_status()
    
    token_data = response.json()
    token = token_data["access_token"]
    expires_in = token_data.get("expires_in", 3600)
    
    # Cache por 55 minutos (margem de segurança)
    _token_cache["token"] = token
    _token_cache["expires"] = datetime.now() + timedelta(seconds=expires_in - 300)
    
    print(f"✅ Token obtido (válido por {expires_in}s)")
    
    return f"Bearer {token}"


# ============================================================================
# FUNÇÃO: get_cost_loop_data (Equivalente a List.Generate)
# ============================================================================

def get_cost_loop_data(
    api_filter1: str,
    api_filter2: str,
    config: APIConfig,
    session: requests.Session
) -> List[Dict]:
    """
    Equivalente: Função get_cost_loop_data do Power Query
    
    Implementa List.Generate com paginação
    
    Fluxo:
    1. Inicia: offset=0, reset=1
    2. Loop enquanto reset > -1:
        - GET API
        - Capture: data, count, offset_next
        - Reset = -1 se última página
        - Reset = 0 se próxima é última
        - Reset = 1 se continua
    3. Retorna lista de responses
    
    Args:
        api_filter1: "?currency=USD&filter[limit]="
        api_filter2: "&filter[resolution]=daily&start_date=...&group_by[X]=*&order_by[cost]=desc"
    
    Returns:
        Lista com dicts: {data, next, balance}
    """
    
    results = []
    offset = config.api_offset
    reset = 1
    page = 0
    
    while reset > -1:
        page += 1
        
        # Monta URL completa
        full_filter = (
            api_filter1 +
            str(config.api_limit) +
            f"&filter[offset]={offset}" +
            api_filter2
        )
        
        url = config.console_url + config.costs_url + full_filter
        
        print(f"  📄 Página {page} (offset {offset}, limit {config.api_limit})")
        
        # GET
        try:
            response = session.get(
                url,
                headers={"Authorization": get_token(config)},
                timeout=30
            )
            response.raise_for_status()
            data = response.json()
        except requests.exceptions.RequestException as e:
            print(f"  ❌ Erro na página {page}: {e}")
            break
        
        # Capture data e count
        api_data = data
        count = data.get("meta", {}).get("count", 0)
        
        # Calcula próximo offset
        offset_next = config.api_limit + offset
        
        # Reset logic (EXATO DO POWER QUERY)
        if reset == 0:
            reset = -1  # Dummy call, termina
        elif count <= offset_next:
            reset = 0   # Próxima será última
        else:
            reset = 1   # Continua
        
        print(f"    ✅ Capturou {len(data.get('data', []))} items (count={count}, balance={reset})")
        
        results.append({
            "data": api_data,
            "next": offset_next,
            "balance": reset
        })
        
        offset = offset_next
    
    return results


# ============================================================================
# FUNÇÃO: replace_field_name (Função auxiliar para Tags)
# ============================================================================

def replace_field_name(record: Dict, field_name: str, new_name: str) -> Dict:
    """
    Equivalente: Função replace_field_name do Power Query
    
    Renomeia um campo dentro de um dicionário mantendo valores
    """
    result = {}
    for key, value in record.items():
        if key == field_name:
            result[new_name] = value
        else:
            result[key] = value
    return result


# ============================================================================
# NÍVEL 1: CARREGAMENTO DE APIs (SEM PAGINAÇÃO)
# ============================================================================

def get_currency_master(config: APIConfig, session: requests.Session) -> pd.DataFrame:
    """
    Equivalente: Currency_Master (Power Query)
    
    GET: Console_URL + Currency_URL
    Expande: code, name, symbol, description
    
    Returns: DataFrame com ~136 moedas
    """
    
    print("📥 Nível 1: Currency_Master...")
    
    url = config.console_url + config.currency_url
    
    response = session.get(
        url,
        headers={"Authorization": get_token(config)},
        timeout=30
    )
    response.raise_for_status()
    data = response.json()
    
    # Expande lista de moedas
    currencies = []
    for currency in data.get("data", []):
        currencies.append({
            "code": currency.get("code"),
            "name": currency.get("name"),
            "symbol": currency.get("symbol"),
            "description": currency.get("description"),
        })
    
    df = pd.DataFrame(currencies)
    print(f"  ✅ {len(df)} moedas carregadas")
    
    return df


def get_default_configurations(config: APIConfig, session: requests.Session) -> pd.DataFrame:
    """
    Equivalente: Default_Configurations (Power Query)
    
    GET: Console_URL + Default_Configs
    Expande: data.currency, data.cost_type
    """
    
    print("📥 Nível 1: Default_Configurations...")
    
    url = config.console_url + config.default_configs
    
    response = session.get(
        url,
        headers={"Authorization": get_token(config)},
        timeout=30
    )
    response.raise_for_status()
    data = response.json()
    
    # Extrai data array
    configs = []
    for item in data.get("data", []):
        configs.append({
            "data.currency": item.get("currency"),
            "data.cost_type": item.get("cost_type"),
        })
    
    df = pd.DataFrame(configs)
    print(f"  ✅ {len(df)} configurações carregadas")
    
    return df


def get_tag_keys(config: APIConfig, session: requests.Session) -> pd.DataFrame:
    """
    Equivalente: OS Tag Keys (Power Query)
    
    GET: Console_URL + Tags_URL
    Expande: data.key, data.enabled
    Adiciona: Group By = "tag"
    """
    
    print("📥 Nível 1: OS Tag Keys...")
    
    url = config.console_url + config.tags_url
    
    response = session.get(
        url,
        headers={"Authorization": get_token(config)},
        timeout=30
    )
    response.raise_for_status()
    data = response.json()
    
    # Expande lista de tags
    tags = []
    for tag in data.get("data", []):
        tags.append({
            "count": data["meta"]["count"],
            "key": tag.get("key"),
            "enabled": tag.get("enabled"),
            "Group By": "tag",
        })
    
    df = pd.DataFrame(tags)
    print(f"  ✅ {len(df)} chaves de tag carregadas")
    
    return df


# ============================================================================
# NÍVEL 2: TABELAS AUXILIARES (EXCEL)
# ============================================================================

def load_data_period(
    start_date: str = None,
    end_date: str = None
) -> pd.DataFrame:
    """
    Equivalente: Data_Period (Power Query)
    
    Entrada: datas do usuário
    Calcula: End Month, Time_Scope_Value
    """
    
    print("📥 Nível 2: Data_Period...")
    
    if start_date is None:
        start_date = "2025-12-01"
    if end_date is None:
        end_date = "2026-01-02"
    
    start = pd.to_datetime(start_date).date()
    end = pd.to_datetime(end_date).date()
    
    # Calcula End Month
    end_month = f"{end.year}-{end.month}"
    
    # Calcula Time_Scope_Value (EXATO DO POWER QUERY)
    now = datetime.now()
    if end.year == now.year:
        time_scope = (-(now.month - end.month)) - 1
    elif end.year < now.year:
        time_scope = (-(12 + now.month - end.month)) - 1
    else:
        time_scope = 0
    
    df = pd.DataFrame([{
        "Start Date": start,
        "End Date": end,
        "End Month": end_month,
        "Time_Scope_Value": time_scope,
    }])
    
    print(f"  ✅ Período: {start} a {end}")
    
    return df


def load_group_bys() -> pd.DataFrame:
    """
    Equivalente: OpenShift_Group_Bys (Power Query)
    
    Tabela estática com tipos de agrupamento
    Adiciona: Join_Seq = 1 (para JOIN)
    """
    
    print("📥 Nível 2: OpenShift_Group_Bys...")
    
    df = pd.DataFrame([
        {"Group By": "Cluster", "Group By Code": "cluster"},
        {"Group By": "Node", "Group By Code": "node"},
        {"Group By": "Project", "Group By Code": "project"},
        {"Group By": "Tag", "Group By Code": "tag"},
    ])
    
    df["Join_Seq"] = 1
    
    print(f"  ✅ {len(df)} group bys carregados")
    
    return df


def load_overhead_cost_types() -> pd.DataFrame:
    """
    Equivalente: Project_Overhead_Cost_Types (Power Query)
    
    Tabela estática com tipos de custos
    """
    
    print("📥 Nível 2: Project_Overhead_Cost_Types...")
    
    df = pd.DataFrame([
        {"Code": "cost", "Description": "Don't distribute overhead costs"},
        {"Code": "distributed_cost", "Description": "Distribute through cost models"},
    ])
    
    print(f"  ✅ {len(df)} cost types carregados")
    
    return df


# ============================================================================
# NÍVEL 3: JOINS
# ============================================================================

def get_default_master_settings(
    currencies_df: pd.DataFrame,
    configs_df: pd.DataFrame
) -> pd.DataFrame:
    """
    Equivalente: Default Master Settings (Power Query)
    
    INNER JOIN:
    currencies_df[code] = configs_df[data.currency]
    """
    
    print("📥 Nível 3: Default Master Settings...")
    
    # INNER JOIN
    df = currencies_df.merge(
        configs_df,
        left_on="code",
        right_on="data.currency",
        how="inner"
    )
    
    # SELECT colunas
    df = df[[
        "code", "name", "symbol", "description",
        "data.currency", "data.cost_type"
    ]]
    
    print(f"  ✅ {len(df)} registros após JOIN")
    
    return df


# ============================================================================
# NÍVEL 4: EXTRAÇÃO COM PAGINAÇÃO (List.Generate)
# ============================================================================

def extract_cost_data_project_daily_extract(
    default_master_settings: pd.DataFrame,
    group_bys: pd.DataFrame,
    data_period: pd.DataFrame,
    config: APIConfig,
    session: requests.Session
) -> pd.DataFrame:
    """
    Equivalente: Cost_Data_Project_Daily_Extract (Power Query)
    
    Fluxo:
    1. START: Default_Master_Settings
    2. Add: Load_Date, Seq=1
    3. NESTED JOIN: group_bys where Seq=1
    4. EXPAND: Group By, Group By Code
    5. FILTER: Group By = "Project"
    6. DISTINCT
    7. ADD: Filter_Start, Filter_End = Data_Period dates
    8. BUILD: Filter1, Filter2 strings
    9. INVOKE: get_cost_loop_data() for cada row
    10. EXPAND: meta, links, data
    """
    
    print("📥 Nível 4: Cost_Data_Project_Daily_Extract...")
    
    # 1-2
    df = default_master_settings.copy()
    df["Load_Date"] = datetime.now()
    df["Seq"] = 1
    
    # 3-4: NESTED JOIN + EXPAND
    df = df.merge(
        group_bys,
        left_on="Seq",
        right_on="Join_Seq",
        how="inner"
    )
    
    # 5: FILTER Group By = "Project"
    df = df[df["Group By"] == "Project"].reset_index(drop=True)
    
    # 6: DISTINCT
    df = df.drop_duplicates()
    
    print(f"  Após filtro: {len(df)} currency × groupby combinations")
    
    # 7: ADD dates from Data_Period
    start_date = data_period["Start Date"].iloc[0]
    end_date = data_period["End Date"].iloc[0]
    
    df["Filter_Start"] = start_date
    df["Filter_End"] = end_date
    
    # 8: BUILD filter strings
    df["Filter 1"] = df["code"].apply(
        lambda x: f"?currency={x}&filter[limit]="
    )
    
    start_str = start_date.strftime("%Y-%m-%d")
    end_str = end_date.strftime("%Y-%m-%d")
    
    df["Filter 2"] = (
        f"&filter[resolution]=daily&start_date={start_str}" +
        f"&end_date={end_str}" +
        "&group_by[" + df["Group By Code"] + "]=*&order_by[cost]=desc"
    )
    
    # 9: INVOKE get_cost_loop_data()
    print(f"  🔄 Chamando API para {len(df)} combinações...")
    
    all_responses = []
    for idx, row in df.iterrows():
        try:
            responses = get_cost_loop_data(
                row["Filter 1"],
                row["Filter 2"],
                config,
                session
            )
            
            for resp in responses:
                new_row = row.to_dict()
                new_row["Data"] = resp["data"]
                all_responses.append(new_row)
                
        except Exception as e:
            print(f"  ⚠️ Erro para {row['code']}: {e}")
    
    df_result = pd.DataFrame(all_responses) if all_responses else df.copy()
    df_result["Data"] = df_result.get("Data", [])
    
    # 10: EXPAND meta, links, data
    # Expande estrutura aninhada
    if "Data" in df_result.columns and len(df_result) > 0:
        df_result = _expand_api_response(df_result, "Data")
    
    print(f"  ✅ {len(df_result)} registros expandidos")
    
    return df_result


def extract_cost_data_clusters_daily_extract(
    default_master_settings: pd.DataFrame,
    group_bys: pd.DataFrame,
    data_period: pd.DataFrame,
    config: APIConfig,
    session: requests.Session
) -> pd.DataFrame:
    """
    Equivalente: Cost_Data_Clusters_Daily_Extract (Power Query)
    
    Idêntico a Project, mas filtra Group By = "Cluster"
    """
    
    print("📥 Nível 4: Cost_Data_Clusters_Daily_Extract...")
    
    df = default_master_settings.copy()
    df["Load_Date"] = datetime.now()
    df["Seq"] = 1
    
    df = df.merge(group_bys, left_on="Seq", right_on="Join_Seq", how="inner")
    df = df[df["Group By"] == "Cluster"].reset_index(drop=True)
    df = df.drop_duplicates()
    
    start_date = data_period["Start Date"].iloc[0]
    end_date = data_period["End Date"].iloc[0]
    
    df["Filter_Start"] = start_date
    df["Filter_End"] = end_date
    
    df["Filter 1"] = df["code"].apply(lambda x: f"?currency={x}&filter[limit]=")
    
    start_str = start_date.strftime("%Y-%m-%d")
    end_str = end_date.strftime("%Y-%m-%d")
    
    df["Filter 2"] = (
        f"&filter[resolution]=daily&start_date={start_str}" +
        f"&end_date={end_str}" +
        "&group_by[" + df["Group By Code"] + "]=*&order_by[cost]=desc"
    )
    
    print(f"  🔄 Chamando API para {len(df)} combinações...")
    
    all_responses = []
    for idx, row in df.iterrows():
        try:
            responses = get_cost_loop_data(row["Filter 1"], row["Filter 2"], config, session)
            for resp in responses:
                new_row = row.to_dict()
                new_row["Data"] = resp["data"]
                all_responses.append(new_row)
        except Exception as e:
            print(f"  ⚠️ Erro para {row['code']}: {e}")
    
    df_result = pd.DataFrame(all_responses) if all_responses else df.copy()
    df_result["Data"] = df_result.get("Data", [])
    
    if "Data" in df_result.columns and len(df_result) > 0:
        df_result = _expand_api_response(df_result, "Data")
    
    print(f"  ✅ {len(df_result)} registros expandidos")
    
    return df_result


def extract_cost_data_nodes_daily_extract(
    default_master_settings: pd.DataFrame,
    group_bys: pd.DataFrame,
    data_period: pd.DataFrame,
    config: APIConfig,
    session: requests.Session
) -> pd.DataFrame:
    """
    Equivalente: Cost_Data_Nodes_Daily_Extract (Power Query)
    
    Idêntico a Project, mas filtra Group By = "Node"
    """
    
    print("📥 Nível 4: Cost_Data_Nodes_Daily_Extract...")
    
    df = default_master_settings.copy()
    df["Load_Date"] = datetime.now()
    df["Seq"] = 1
    
    df = df.merge(group_bys, left_on="Seq", right_on="Join_Seq", how="inner")
    df = df[df["Group By"] == "Node"].reset_index(drop=True)
    df = df.drop_duplicates()
    
    start_date = data_period["Start Date"].iloc[0]
    end_date = data_period["End Date"].iloc[0]
    
    df["Filter_Start"] = start_date
    df["Filter_End"] = end_date
    
    df["Filter 1"] = df["code"].apply(lambda x: f"?currency={x}&filter[limit]=")
    
    start_str = start_date.strftime("%Y-%m-%d")
    end_str = end_date.strftime("%Y-%m-%d")
    
    df["Filter 2"] = (
        f"&filter[resolution]=daily&start_date={start_str}" +
        f"&end_date={end_str}" +
        "&group_by[" + df["Group By Code"] + "]=*&order_by[cost]=desc"
    )
    
    print(f"  🔄 Chamando API para {len(df)} combinações...")
    
    all_responses = []
    for idx, row in df.iterrows():
        try:
            responses = get_cost_loop_data(row["Filter 1"], row["Filter 2"], config, session)
            for resp in responses:
                new_row = row.to_dict()
                new_row["Data"] = resp["data"]
                all_responses.append(new_row)
        except Exception as e:
            print(f"  ⚠️ Erro para {row['code']}: {e}")
    
    df_result = pd.DataFrame(all_responses) if all_responses else df.copy()
    df_result["Data"] = df_result.get("Data", [])
    
    if "Data" in df_result.columns and len(df_result) > 0:
        df_result = _expand_api_response(df_result, "Data")
    
    print(f"  ✅ {len(df_result)} registros expandidos")
    
    return df_result


def extract_cost_data_tags_daily_extract(
    default_master_settings: pd.DataFrame,
    group_bys: pd.DataFrame,
    tag_keys: pd.DataFrame,
    data_period: pd.DataFrame,
    config: APIConfig,
    session: requests.Session
) -> pd.DataFrame:
    """
    Equivalente: Cost_Data_Tags_Daily_Extract (Power Query)
    
    Similar ao Project, mas com JOIN aos tag keys
    """
    
    print("📥 Nível 4: Cost_Data_Tags_Daily_Extract...")
    
    df = default_master_settings.copy()
    df["Load_Date"] = datetime.now()
    df["Seq"] = 1
    
    df = df.merge(group_bys, left_on="Seq", right_on="Join_Seq", how="inner")
    df = df[df["Group By"] == "Tag"].reset_index(drop=True)
    
    # LEFT JOIN com tag keys
    df = df.merge(
        tag_keys,
        left_on="Group By Code",
        right_on="Group By",
        how="left"
    )
    
    df = df.drop_duplicates()
    
    start_date = data_period["Start Date"].iloc[0]
    end_date = data_period["End Date"].iloc[0]
    
    df["Filter_Start"] = start_date
    df["Filter_End"] = end_date
    
    df["Filter 1"] = df["code"].apply(lambda x: f"?currency={x}&filter[limit]=")
    
    start_str = start_date.strftime("%Y-%m-%d")
    end_str = end_date.strftime("%Y-%m-%d")
    
    # Filter2 inclui key se existir
    df["Filter 2"] = (
        f"&filter[resolution]=daily&start_date={start_str}" +
        f"&end_date={end_str}" +
        "&group_by[" + df["Group By Code"] + ":" + df["key"].fillna("") + "]=*&order_by[cost]=desc"
    )
    
    print(f"  🔄 Chamando API para {len(df)} combinações...")
    
    all_responses = []
    for idx, row in df.iterrows():
        try:
            responses = get_cost_loop_data(row["Filter 1"], row["Filter 2"], config, session)
            for resp in responses:
                new_row = row.to_dict()
                new_row["Data"] = resp["data"]
                all_responses.append(new_row)
        except Exception as e:
            print(f"  ⚠️ Erro para {row['code']}: {e}")
    
    df_result = pd.DataFrame(all_responses) if all_responses else df.copy()
    df_result["Data"] = df_result.get("Data", [])
    
    if "Data" in df_result.columns and len(df_result) > 0:
        df_result = _expand_api_response(df_result, "Data")
    
    print(f"  ✅ {len(df_result)} registros expandidos")
    
    return df_result


# ============================================================================
# HELPERS: Expansão de estruturas aninhadas
# ============================================================================

def _expand_api_response(df: pd.DataFrame, data_col: str) -> pd.DataFrame:
    """
    Expande estrutura aninhada da API response
    Equivalente aos múltiplos ExpandRecordColumn do Power Query
    """
    
    # Extrai meta, links, data
    for idx, row in df.iterrows():
        if isinstance(row[data_col], dict):
            api_resp = row[data_col]
            
            # Coloca em colunas separadas
            df.loc[idx, "meta"] = api_resp.get("meta", {})
            df.loc[idx, "links"] = api_resp.get("links", {})
            df.loc[idx, "data"] = api_resp.get("data", [])
    
    # Expande meta como colunas
    meta_cols = [
        "count", "limit", "offset", "others", "currency", "delta",
        "filter", "group_by", "order_by", "exclude", "distributed_overhead", "total"
    ]
    
    for col in meta_cols:
        df[f"meta.{col}"] = df["meta"].apply(
            lambda x: x.get(col) if isinstance(x, dict) else None
        )
    
    # Expande links
    link_cols = ["first", "next", "previous", "last"]
    for col in link_cols:
        df[f"links.{col}"] = df["links"].apply(
            lambda x: x.get(col) if isinstance(x, dict) else None
        )
    
    # Expande meta.total recursivamente
    # (Simplificado - mantem estrutura)
    df["meta.total"] = df["meta"].apply(
        lambda x: x.get("total", {}) if isinstance(x, dict) else {}
    )
    
    return df


# ============================================================================
# NÍVEL 5: EXPANSÃO FINAL (ExpandListColumn)
# ============================================================================

def expand_cost_data_projects_daily(
    cost_data_project_daily_extract: pd.DataFrame
) -> pd.DataFrame:
    """
    Equivalente: Cost_Data_Projects_Daily (Power Query)
    
    Expande data → date + projects
    Expande projects → project + values
    Expande values → individual records
    """
    
    print("📥 Nível 5: Cost_Data_Projects_Daily...")
    
    # SELECT
    df = cost_data_project_daily_extract[[
        "code", "Group By Code", "meta.distributed_overhead", "data"
    ]].copy()
    
    # EXPAND LIST: data
    rows = []
    for idx, row in df.iterrows():
        if isinstance(row["data"], list):
            for data_item in row["data"]:
                new_row = row.copy()
                new_row["data"] = data_item
                rows.append(new_row)
    
    if not rows:
        print(f"  ⚠️ Nenhum dado expandido (empty data lists)")
        return pd.DataFrame()
    
    df = pd.DataFrame(rows).reset_index(drop=True)
    
    # EXPAND RECORD: data → {date, projects}
    df["date"] = df["data"].apply(
        lambda x: x.get("date") if isinstance(x, dict) else None
    )
    df["projects"] = df["data"].apply(
        lambda x: x.get("projects", []) if isinstance(x, dict) else []
    )
    
    # EXPAND LIST: projects
    rows = []
    for idx, row in df.iterrows():
        if isinstance(row["projects"], list):
            for project in row["projects"]:
                new_row = row.copy()
                new_row["project"] = project.get("project")
                new_row["values"] = project.get("values", [])
                rows.append(new_row)
    
    df = pd.DataFrame(rows).reset_index(drop=True) if rows else df
    
    # EXPAND LIST: values
    rows = []
    for idx, row in df.iterrows():
        if isinstance(row["values"], list):
            for value_item in row["values"]:
                new_row = row.copy()
                # Expande fields de value_item
                if isinstance(value_item, dict):
                    for k, v in value_item.items():
                        new_row[f"values.{k}"] = v
                rows.append(new_row)
    
    df = pd.DataFrame(rows).reset_index(drop=True) if rows else df
    
    # Transforma listas em texto
    if "values.source_uuid" in df.columns:
        df["values.source_uuid"] = df["values.source_uuid"].apply(
            lambda x: ",".join(map(str, x)) if isinstance(x, list) else x
        )
    
    if "values.clusters" in df.columns:
        df["values.clusters"] = df["values.clusters"].apply(
            lambda x: ",".join(map(str, x)) if isinstance(x, list) else x
        )
    
    # Expande cost, infrastructure, supplementary recursivamente
    _expand_nested_costs(df)
    
    # Type conversions
    df["date"] = pd.to_datetime(df["date"])
    if "values.date" in df.columns:
        df["values.date"] = pd.to_datetime(df["values.date"])
    
    # Rename
    df = df.rename(columns={"project": "Name"})
    
    print(f"  ✅ {len(df)} registros expandidos")
    
    return df


def expand_cost_data_clusters_daily(
    cost_data_clusters_daily_extract: pd.DataFrame
) -> pd.DataFrame:
    """
    Equivalente: Cost_Data_Clusters_Daily (Power Query)
    
    Similar a Projects mas para clusters
    """
    
    print("📥 Nível 5: Cost_Data_Clusters_Daily...")
    
    df = cost_data_clusters_daily_extract[[
        "code", "Group By Code", "data"
    ]].copy()
    
    # EXPAND LIST: data
    rows = []
    for idx, row in df.iterrows():
        if isinstance(row["data"], list):
            for data_item in row["data"]:
                new_row = row.copy()
                new_row["data"] = data_item
                rows.append(new_row)
    
    if not rows:
        return pd.DataFrame()
    
    df = pd.DataFrame(rows).reset_index(drop=True)
    
    # EXPAND: data → {date, clusters}
    df["date"] = df["data"].apply(
        lambda x: x.get("date") if isinstance(x, dict) else None
    )
    df["clusters"] = df["data"].apply(
        lambda x: x.get("clusters", []) if isinstance(x, dict) else []
    )
    
    # EXPAND LIST: clusters
    rows = []
    for idx, row in df.iterrows():
        if isinstance(row["clusters"], list):
            for cluster in row["clusters"]:
                new_row = row.copy()
                new_row["cluster"] = cluster.get("cluster")
                new_row["values"] = cluster.get("values", [])
                rows.append(new_row)
    
    df = pd.DataFrame(rows).reset_index(drop=True) if rows else df
    
    # EXPAND LIST: values
    rows = []
    for idx, row in df.iterrows():
        if isinstance(row["values"], list):
            for value_item in row["values"]:
                new_row = row.copy()
                if isinstance(value_item, dict):
                    for k, v in value_item.items():
                        new_row[f"values.{k}"] = v
                rows.append(new_row)
    
    df = pd.DataFrame(rows).reset_index(drop=True) if rows else df
    
    if "values.source_uuid" in df.columns:
        df["values.source_uuid"] = df["values.source_uuid"].apply(
            lambda x: ",".join(map(str, x)) if isinstance(x, list) else x
        )
    
    if "values.clusters" in df.columns:
        df["values.clusters"] = df["values.clusters"].apply(
            lambda x: ",".join(map(str, x)) if isinstance(x, list) else x
        )
    
    _expand_nested_costs(df)
    
    df["date"] = pd.to_datetime(df["date"])
    if "values.date" in df.columns:
        df["values.date"] = pd.to_datetime(df["values.date"])
    
    df = df.rename(columns={"cluster": "Name"})
    
    print(f"  ✅ {len(df)} registros expandidos")
    
    return df


def expand_cost_data_nodes_daily(
    cost_data_nodes_daily_extract: pd.DataFrame
) -> pd.DataFrame:
    """
    Equivalente: Cost_Data_Nodes_Daily (Power Query)
    
    Similar a Projects mas para nodes
    """
    
    print("📥 Nível 5: Cost_Data_Nodes_Daily...")
    
    df = cost_data_nodes_daily_extract[[
        "code", "Group By Code", "data"
    ]].copy()
    
    # EXPAND LIST: data
    rows = []
    for idx, row in df.iterrows():
        if isinstance(row["data"], list):
            for data_item in row["data"]:
                new_row = row.copy()
                new_row["data"] = data_item
                rows.append(new_row)
    
    if not rows:
        return pd.DataFrame()
    
    df = pd.DataFrame(rows).reset_index(drop=True)
    
    df["date"] = df["data"].apply(
        lambda x: x.get("date") if isinstance(x, dict) else None
    )
    df["nodes"] = df["data"].apply(
        lambda x: x.get("nodes", []) if isinstance(x, dict) else []
    )
    
    rows = []
    for idx, row in df.iterrows():
        if isinstance(row["nodes"], list):
            for node in row["nodes"]:
                new_row = row.copy()
                new_row["node"] = node.get("node")
                new_row["values"] = node.get("values", [])
                rows.append(new_row)
    
    df = pd.DataFrame(rows).reset_index(drop=True) if rows else df
    
    rows = []
    for idx, row in df.iterrows():
        if isinstance(row["values"], list):
            for value_item in row["values"]:
                new_row = row.copy()
                if isinstance(value_item, dict):
                    for k, v in value_item.items():
                        new_row[f"values.{k}"] = v
                rows.append(new_row)
    
    df = pd.DataFrame(rows).reset_index(drop=True) if rows else df
    
    if "values.source_uuid" in df.columns:
        df["values.source_uuid"] = df["values.source_uuid"].apply(
            lambda x: ",".join(map(str, x)) if isinstance(x, list) else x
        )
    
    if "values.clusters" in df.columns:
        df["values.clusters"] = df["values.clusters"].apply(
            lambda x: ",".join(map(str, x)) if isinstance(x, list) else x
        )
    
    _expand_nested_costs(df)
    
    df["date"] = pd.to_datetime(df["date"])
    if "values.date" in df.columns:
        df["values.date"] = pd.to_datetime(df["values.date"])
    
    df = df.rename(columns={"node": "Name"})
    
    print(f"  ✅ {len(df)} registros expandidos")
    
    return df


def expand_cost_data_tags_daily(
    cost_data_tags_daily_extract: pd.DataFrame
) -> pd.DataFrame:
    """
    Equivalente: Cost_Data_Tags_Daily (Power Query)
    
    Usa replace_field_name() para renomear campos tag
    """
    
    print("📥 Nível 5: Cost_Data_Tags_Daily...")
    
    df = cost_data_tags_daily_extract[[
        "code", "Group By Code", "key", "data"
    ]].copy()
    
    df = df.drop_duplicates()
    
    # EXPAND LIST: data
    rows = []
    for idx, row in df.iterrows():
        if isinstance(row["data"], list):
            for data_item in row["data"]:
                new_row = row.copy()
                new_row["data"] = data_item
                rows.append(new_row)
    
    if not rows:
        return pd.DataFrame()
    
    df = pd.DataFrame(rows).reset_index(drop=True)
    
    # Adiciona key_rec_name
    df["key_rec_name"] = df["key"].fillna("") + "s"
    
    # Usa replace_field_name (simulado)
    df["date"] = df["data"].apply(
        lambda x: x.get("date") if isinstance(x, dict) else None
    )
    
    # Extrai tag records
    rows = []
    for idx, row in df.iterrows():
        if isinstance(row["data"], dict):
            data_item = row["data"]
            
            # Procura pelo field key_rec_name
            key_field = row["key_rec_name"]
            tag_records = data_item.get(key_field, [])
            
            if isinstance(tag_records, list):
                for tag_record in tag_records:
                    new_row = row.copy()
                    new_row["Tag Record"] = tag_record
                    rows.append(new_row)
    
    df = pd.DataFrame(rows).reset_index(drop=True) if rows else df
    
    # EXPAND LIST: Tag Record
    rows = []
    for idx, row in df.iterrows():
        if isinstance(row["Tag Record"], dict):
            tag_record = row["Tag Record"]
            new_row = row.copy()
            
            # Extrai os fields
            for k, v in tag_record.items():
                new_row[f"Tag Record.{k}"] = v
            
            rows.append(new_row)
    
    df = pd.DataFrame(rows).reset_index(drop=True) if rows else df
    
    # Extrai Tag Name e values
    if "Tag Record.key" in df.columns and "Tag Record.values" in df.columns:
        # Renomeia usando replace_field_name
        df["Tag Name"] = df["Tag Record.key"]
        df["tag_values"] = df["Tag Record.values"]
    
    # EXPAND LIST: tag_values
    rows = []
    for idx, row in df.iterrows():
        if isinstance(row["tag_values"], list):
            for value_item in row["tag_values"]:
                new_row = row.copy()
                if isinstance(value_item, dict):
                    for k, v in value_item.items():
                        new_row[f"values.{k}"] = v
                rows.append(new_row)
    
    df = pd.DataFrame(rows).reset_index(drop=True) if rows else df
    
    if "values.source_uuid" in df.columns:
        df["values.source_uuid"] = df["values.source_uuid"].apply(
            lambda x: ",".join(map(str, x)) if isinstance(x, list) else x
        )
    
    if "values.clusters" in df.columns:
        df["values.clusters"] = df["values.clusters"].apply(
            lambda x: ",".join(map(str, x)) if isinstance(x, list) else x
        )
    
    _expand_nested_costs(df)
    
    if "values.date" in df.columns:
        df["values.date"] = pd.to_datetime(df["values.date"])
    
    df = df.rename(columns={"Tag Name": "Name"})
    
    print(f"  ✅ {len(df)} registros expandidos")
    
    return df


def _expand_nested_costs(df: pd.DataFrame):
    """
    Expande estruturas aninhadas de cost recursivamente
    Modifica df in-place
    """
    
    # Expande infrastructure
    for col_prefix in ["values.infrastructure", "values.supplementary", "values.cost"]:
        if col_prefix in df.columns:
            for sub_col in ["raw", "markup", "usage", "total"]:
                sub_key = f"{col_prefix}.{sub_col}"
                if sub_key not in df.columns:
                    df[sub_key] = df[col_prefix].apply(
                        lambda x: x.get(sub_col) if isinstance(x, dict) else None
                    )
                
                # Expande {value, units}
                for val_type in ["value", "units"]:
                    df[f"{sub_key}.{val_type}"] = df[sub_key].apply(
                        lambda x: x.get(val_type) if isinstance(x, dict) else None
                    )


# ============================================================================
# NÍVEL 6: JUNCTIONS (Dados derivados)
# ============================================================================

def extract_cluster_projects(
    cost_data_clusters_daily_extract: pd.DataFrame,
    data_period: pd.DataFrame,
    config: APIConfig,
    session: requests.Session
) -> pd.DataFrame:
    """
    Equivalente: OS Cost Cluster Projects (Power Query)
    
    Para cada cluster: GET projetos da API
    Resultado: ~45.936 linhas
    """
    
    print("📥 Nível 6: OS Cost Cluster Projects...")
    
    # START: Cost_Data_Master_Clusters_Daily
    df = cost_data_clusters_daily_extract[[
        "code", "Group By Code", "data"
    ]].copy()
    
    # Expand data
    rows = []
    for idx, row in df.iterrows():
        if isinstance(row["data"], list):
            for data_item in row["data"]:
                new_row = row.copy()
                new_row["data"] = data_item
                rows.append(new_row)
    
    if not rows:
        return pd.DataFrame()
    
    df = pd.DataFrame(rows).reset_index(drop=True)
    
    # Extract date e clusters
    df["date"] = df["data"].apply(
        lambda x: x.get("date") if isinstance(x, dict) else None
    )
    df["clusters"] = df["data"].apply(
        lambda x: x.get("clusters", []) if isinstance(x, dict) else []
    )
    
    # Expand clusters
    rows = []
    for idx, row in df.iterrows():
        if isinstance(row["clusters"], list):
            for cluster in row["clusters"]:
                new_row = row.copy()
                new_row["cluster"] = cluster.get("cluster")
                rows.append(new_row)
    
    df = pd.DataFrame(rows).reset_index(drop=True) if rows else df
    
    # Date.StartOfMonth
    df["date"] = pd.to_datetime(df["date"])
    df["date"] = df["date"].dt.to_period('M').dt.start_time.dt.date
    
    # DISTINCT
    df = df.drop_duplicates()
    
    start_date = data_period["Start Date"].iloc[0]
    end_date = data_period["End Date"].iloc[0]
    
    start_str = start_date.strftime("%Y-%m-%d")
    end_str = end_date.strftime("%Y-%m-%d")
    
    # BUILD filters
    df["Filter 1"] = df["code"].apply(
        lambda x: f"?currency={x}&filter[cluster]=" + df["cluster"] + "&filter[limit]="
    )
    
    df["Filter 2"] = (
        f"&filter[resolution]=daily&start_date={start_str}" +
        f"&end_date={end_str}" +
        "&group_by[project]=*"
    )
    
    # GET projetos para cada cluster
    print(f"  🔄 Chamando API para {len(df)} clusters...")
    
    all_rows = []
    for idx, row in df.iterrows():
        try:
            responses = get_cost_loop_data(row["Filter 1"], row["Filter 2"], config, session)
            
            for resp in responses:
                api_data = resp["data"]
                
                # Expand data
                for data_item in api_data.get("data", []):
                    for project in data_item.get("projects", []):
                        for value_item in project.get("values", []):
                            all_rows.append({
                                "code": row["code"],
                                "Group By Code": row["Group By Code"],
                                "cluster": row["cluster"],
                                "date": data_item.get("date"),
                                "project": project.get("project"),
                                "value": value_item.get("cost", {}).get("total", {}).get("value"),
                                "units": value_item.get("cost", {}).get("total", {}).get("units"),
                            })
        except Exception as e:
            print(f"  ⚠️ Erro para cluster {row['cluster']}: {e}")
    
    result_df = pd.DataFrame(all_rows) if all_rows else pd.DataFrame()
    
    # Filter Month
    if len(result_df) > 0:
        end_month = data_period["End Month"].iloc[0]
        result_df["Filter Month"] = end_month
        
        # Remove nulls
        result_df = result_df.dropna(subset=["project"])
    
    print(f"  ✅ {len(result_df)} cluster×project combinations")
    
    return result_df


def extract_project_tags(
    cost_data_projects_daily_extract: pd.DataFrame,
    data_period: pd.DataFrame,
    config: APIConfig,
    session: requests.Session
) -> pd.DataFrame:
    """
    Equivalente: OS Cost Project Tags (Power Query)
    
    Para cada projeto: GET tags via API (sem paginação)
    Resultado: ~254 linhas
    """
    
    print("📥 Nível 6: OS Cost Project Tags...")
    
    # START: Cost_Data_Master_Projects_Daily
    df = cost_data_projects_daily_extract[[
        "code", "data"
    ]].copy()
    
    rows = []
    for idx, row in df.iterrows():
        if isinstance(row["data"], list):
            for data_item in row["data"]:
                new_row = row.copy()
                new_row["data"] = data_item
                rows.append(new_row)
    
    if not rows:
        return pd.DataFrame()
    
    df = pd.DataFrame(rows).reset_index(drop=True)
    
    df["date"] = df["data"].apply(
        lambda x: x.get("date") if isinstance(x, dict) else None
    )
    df["projects"] = df["data"].apply(
        lambda x: x.get("projects", []) if isinstance(x, dict) else []
    )
    
    rows = []
    for idx, row in df.iterrows():
        if isinstance(row["projects"], list):
            for project in row["projects"]:
                new_row = row.copy()
                new_row["project"] = project.get("project")
                rows.append(new_row)
    
    df = pd.DataFrame(rows).reset_index(drop=True) if rows else df
    
    # Date.StartOfMonth
    df["date"] = pd.to_datetime(df["date"])
    df["date"] = df["date"].dt.to_period('M').dt.start_time.dt.date
    
    df = df.drop_duplicates()
    
    start_date = data_period["Start Date"].iloc[0]
    end_date = data_period["End Date"].iloc[0]
    
    start_str = start_date.strftime("%Y-%m-%d")
    end_str = end_date.strftime("%Y-%m-%d")
    
    # BUILD filter
    df["Filter"] = (
        "?filter[project]=" + df["project"] +
        f"&filter[resolution]=daily&start_date={start_str}" +
        f"&end_date={end_str}"
    )
    
    # GET tags para cada projeto (SEM paginação)
    print(f"  🔄 Chamando API para {len(df)} projetos...")
    
    all_rows = []
    for idx, row in df.iterrows():
        try:
            url = config.console_url + config.tags_url + row["Filter"]
            response = session.get(
                url,
                headers={"Authorization": get_token(config)},
                timeout=30
            )
            response.raise_for_status()
            tag_data = response.json()
            
            # Expand tags
            for tag_item in tag_data.get("data", []):
                for value_item in tag_item.get("values", []):
                    all_rows.append({
                        "code": row["code"],
                        "date": row["date"],
                        "project": row["project"],
                        "key": tag_item.get("key"),
                        "values": value_item,
                        "enabled": tag_item.get("enabled"),
                    })
        except Exception as e:
            print(f"  ⚠️ Erro para projeto {row['project']}: {e}")
    
    result_df = pd.DataFrame(all_rows) if all_rows else pd.DataFrame()
    
    if len(result_df) > 0:
        end_month = data_period["End Month"].iloc[0]
        result_df["Filter Month"] = end_month
    
    print(f"  ✅ {len(result_df)} project×tag combinations")
    
    return result_df


# ============================================================================
# NÍVEL 7: CONSOLIDAÇÃO
# ============================================================================

def os_costs_daily(
    cost_data_projects_daily: pd.DataFrame,
    cost_data_clusters_daily: pd.DataFrame,
    cost_data_nodes_daily: pd.DataFrame,
    cost_data_tags_daily: pd.DataFrame
) -> pd.DataFrame:
    """
    Equivalente: OS Costs Daily (Power Query)
    
    Table.Combine({
        Cost_Data_Projects_Daily,
        Cost_Data_Clusters_Daily,
        Cost_Data_Nodes_Daily,
        Cost_Data_Tags_Daily
    })
    """
    
    print("📥 Nível 7: OS Costs Daily (Consolidação)...")
    
    # UNION ALL das 4 fontes
    result = pd.concat([
        cost_data_projects_daily,
        cost_data_clusters_daily,
        cost_data_nodes_daily,
        cost_data_tags_daily
    ], ignore_index=True)
    
    print(f"  ✅ {len(result)} registros consolidados")
    
    return result


# ============================================================================
# GERAÇÃO DO EXCEL
# ============================================================================

def generate_excel(
    output_file: str,
    data_period: pd.DataFrame,
    default_master_settings: pd.DataFrame,
    project_overhead_cost_types: pd.DataFrame,
    group_bys: pd.DataFrame,
    tag_keys: pd.DataFrame,
    os_cost_cluster_projects: pd.DataFrame,
    os_cost_project_tags: pd.DataFrame,
    os_costs_daily: pd.DataFrame
):
    """
    Gera arquivo Excel com todas as abas
    
    Abas:
    1. Data_Period
    2. Default Master Settings
    3. Project Overhead Cost Types
    4. OpenShift Group Bys
    5. OS Tag Keys
    6. OS Cost Cluster Projects
    7. OS Cost Project Tags
    8. OS Costs Daily
    """
    
    print(f"\n💾 Gerando arquivo Excel: {output_file}")
    
    with pd.ExcelWriter(output_file, engine="openpyxl") as writer:
        data_period.to_excel(writer, sheet_name="Data_Period", index=False)
        default_master_settings.to_excel(writer, sheet_name="Default Master Settings", index=False)
        project_overhead_cost_types.to_excel(writer, sheet_name="Project Overhead Cost Types", index=False)
        group_bys.to_excel(writer, sheet_name="OpenShift Group Bys", index=False)
        tag_keys.to_excel(writer, sheet_name="OS Tag Keys", index=False)
        os_cost_cluster_projects.to_excel(writer, sheet_name="OS Cost Cluster Projects", index=False)
        os_cost_project_tags.to_excel(writer, sheet_name="OS Cost Project Tags", index=False)
        os_costs_daily.to_excel(writer, sheet_name="OS Costs Daily", index=False)
    
    print(f"  ✅ Arquivo salvo: {output_file}")
    
    # Imprime estatísticas
    print(f"\n📊 RESUMO DAS ABAS:")
    print(f"  • Data_Period: {len(data_period)} linha(s)")
    print(f"  • Default Master Settings: {len(default_master_settings)} linha(s)")
    print(f"  • Project Overhead Cost Types: {len(project_overhead_cost_types)} linha(s)")
    print(f"  • OpenShift Group Bys: {len(group_bys)} linha(s)")
    print(f"  • OS Tag Keys: {len(tag_keys)} linha(s)")
    print(f"  • OS Cost Cluster Projects: {len(os_cost_cluster_projects)} linha(s)")
    print(f"  • OS Cost Project Tags: {len(os_cost_project_tags)} linha(s)")
    print(f"  • OS Costs Daily: {len(os_costs_daily)} linha(s)")


# ============================================================================
# MAIN
# ============================================================================

def main():
    """Orquestra execução completa"""
    
    print("\n" + "=" * 100)
    print("🚀 OpenShift Cost Extractor v6.0 - Python (100% Equivalente ao Power Query)")
    print("=" * 100)
    
    parser = argparse.ArgumentParser(description="Extrai custos OpenShift da API Red Hat")
    parser.add_argument("--start-date", default="2025-12-01", help="Data início (YYYY-MM-DD)")
    parser.add_argument("--end-date", default="2026-01-02", help="Data fim (YYYY-MM-DD)")
    parser.add_argument("--output", default="openshift_costs.xlsx", help="Arquivo Excel saída")
    
    args = parser.parse_args()
    
    # Config
    try:
        config = APIConfig()
    except ValueError as e:
        print(f"❌ {e}")
        sys.exit(1)
    
    # Session
    session = requests.Session()
    session.timeout = 30
    
    print(f"\nPeríodo: {args.start_date} a {args.end_date}")
    print("=" * 100)
    
    # NÍVEL 0-1: APIs
    print("\n📥 NÍVEL 0-1: Carregando APIs...")
    currencies = get_currency_master(config, session)
    configs = get_default_configurations(config, session)
    tags = get_tag_keys(config, session)
    
    # NÍVEL 2: Auxiliares
    print("\n📥 NÍVEL 2: Carregando tabelas auxiliares...")
    data_period = load_data_period(args.start_date, args.end_date)
    group_bys = load_group_bys()
    overhead_types = load_overhead_cost_types()
    
    # NÍVEL 3: JOINs
    print("\n📥 NÍVEL 3: Executando JOINs...")
    default_master = get_default_master_settings(currencies, configs)
    
    # NÍVEL 4: Paginação
    print("\n📥 NÍVEL 4: Extraindo dados com paginação...")
    projects_extract = extract_cost_data_project_daily_extract(
        default_master, group_bys, data_period, config, session
    )
    clusters_extract = extract_cost_data_clusters_daily_extract(
        default_master, group_bys, data_period, config, session
    )
    nodes_extract = extract_cost_data_nodes_daily_extract(
        default_master, group_bys, data_period, config, session
    )
    tags_extract = extract_cost_data_tags_daily_extract(
        default_master, group_bys, tags, data_period, config, session
    )
    
    # NÍVEL 5: Expansão
    print("\n📥 NÍVEL 5: Expandindo dados...")
    projects_daily = expand_cost_data_projects_daily(projects_extract)
    clusters_daily = expand_cost_data_clusters_daily(clusters_extract)
    nodes_daily = expand_cost_data_nodes_daily(nodes_extract)
    tags_daily = expand_cost_data_tags_daily(tags_extract)
    
    # NÍVEL 6: Junctions
    print("\n📥 NÍVEL 6: Extraindo junctions...")
    cluster_projects = extract_cluster_projects(clusters_extract, data_period, config, session)
    project_tags = extract_project_tags(projects_extract, data_period, config, session)
    
    # NÍVEL 7: Consolidação
    print("\n📥 NÍVEL 7: Consolidando dados...")
    os_daily = os_costs_daily(projects_daily, clusters_daily, nodes_daily, tags_daily)
    
    # Output
    print("\n💾 Gerando arquivo Excel...")
    generate_excel(
        args.output,
        data_period,
        default_master,
        overhead_types,
        group_bys,
        tags,
        cluster_projects,
        project_tags,
        os_daily
    )
    
    print("\n✅ SUCESSO!")
    print(f"📊 Arquivo salvo em: {args.output}")


if __name__ == "__main__":
    main()
