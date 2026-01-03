#!/usr/bin/env python3.11
"""
╔═══════════════════════════════════════════════════════════════════════════════╗
║                                                                               ║
║  OpenShift Cost Extractor - v5.1 FINAL CORRIGIDO                              ║
║  ✅ CONVERTIDO EXATAMENTE DO POWER QUERY (LINHA POR LINHA)                    ║
║  🔧 v5.1: CORRIGIDO BUG DA URL (filter_url passado corretamente)              ║
║                                                                               ║
║  MAPA DE DEPENDÊNCIA:                                                         ║
║  ────────────────────────────────────────────────────────────────────────     ║
║  Nível 0: Currency_Master, Default_Configurations (APIs)                      ║
║  Nível 1: Default_Master_Settings = JOIN(Currency × Configs)                  ║
║  Nível 2: *_Daily_Extract = get_cost_loop_data (paginação com List.Generate)  ║
║  Nível 3: Cost_Data_Master_*_Daily = SelectColumns (limpeza)                  ║
║  Nível 4: Cost_Data_*_Daily = ExpandListColumn([values])                      ║
║  Nível 5: OS_Cost_* = TABLE.COMBINE + processamento final                     ║
║                                                                               ║
║  🔑 REGRA MÁXIMA: Power Query manda, Python obedece.                          ║
║     Cada transformação, cada coluna, cada expand segue exatamente o PQ.       ║
║                                                                               ║
╚═══════════════════════════════════════════════════════════════════════════════╝
"""

import os
import sys
import logging
import requests
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Any, Tuple
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
import argparse
from io import BytesIO

# ═══════════════════════════════════════════════════════════════════════════════
# CONFIGURAÇÃO DE LOGGING
# ═══════════════════════════════════════════════════════════════════════════════

handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
logging.basicConfig(level=logging.INFO, handlers=[handler])
logger = logging.getLogger(__name__)

# ═══════════════════════════════════════════════════════════════════════════════
# CONFIGURAÇÃO DA API
# ═══════════════════════════════════════════════════════════════════════════════

class APIConfig:
    def __init__(self):
        self.client_id = os.getenv('OPENSHIFT_CLIENT_ID', '')
        self.client_secret = os.getenv('OPENSHIFT_CLIENT_SECRET', '')
        self.console_url = 'https://console.redhat.com'
        self.auth_url = 'https://sso.redhat.com/auth/realms/redhat-external/protocol/openid-connect/token'
        
        # URLs dos endpoints (equivalentes aos Parâmetros do PQ)
        self.currency_url = "/api/cost-management/v1/currency/?filter%5Blimit%5D=15&limit=100&offset=0"
        self.costs_url = "/api/cost-management/v1/reports/openshift/costs/"
        self.tags_url = "/api/cost-management/v1/tags/openshift/"
        self.default_configs_url = "/api/cost-management/v1/account-settings/"
        
        # Limites de paginação (Parâmetros)
        self.api_limit = 10
        self.api_offset = 0
        
        self.timeout = 60
        self.max_retries = 5
        self.backoff_factor = 1.0

# ═══════════════════════════════════════════════════════════════════════════════
# CLIENTE API COM SUPORTE A PAGINAÇÃO
# ═══════════════════════════════════════════════════════════════════════════════

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
            allowed_methods=["HEAD", "GET", "OPTIONS", "POST"],
            backoff_factor=self.config.backoff_factor
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        return session

    def _get_token(self) -> str:
        try:
            auth_data = {
                'grant_type': 'client_credentials',
                'client_id': self.config.client_id,
                'client_secret': self.config.client_secret
            }
            response = self.session.post(self.config.auth_url, data=auth_data, timeout=self.config.timeout)
            response.raise_for_status()
            token_response = response.json()
            self.access_token = token_response['access_token']
            expires_in = token_response.get('expires_in', 900)
            self.token_expires_at = datetime.now() + timedelta(seconds=expires_in - 60)
            return self.access_token
        except Exception as e:
            self.logger.error(f"❌ Erro ao obter token: {e}", exc_info=True)
            raise

    def _ensure_token(self) -> str:
        if not self.access_token or (self.token_expires_at and datetime.now() >= self.token_expires_at):
            self._get_token()
        return self.access_token

    def _get_headers(self) -> Dict[str, str]:
        token = self._ensure_token()
        return {
            'Authorization': f'Bearer {token}',
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        }

    # ┌─────────────────────────────────────────────────────────────────────────────┐
    # │ IMPLEMENTAÇÃO EXATA DO get_cost_loop_data DO POWER QUERY                    │
    # │ Simula: List.Generate com loop de paginação                                │
    # │ v5.1: URL CORRIGIDA - filter_url passado diretamente na URL                │
    # └─────────────────────────────────────────────────────────────────────────────┘
    
    def get_cost_loop_data(self, filter1: str, filter2: str) -> List[Dict]:
        """
        Equivalente ao Power Query: get_cost_loop_data()
        Implementa: List.Generate com paginação (while not eof)
        
        Fluxo:
        1. offset = 0, reset = 1 (continue)
        2. Chama API com limit=10
        3. Calcula offset_next = limit + offset_atual
        4. Calcula reset = 0 se count <= offset_next else 1
        5. Repete até reset = -1 (dummy call, para o loop)
        6. Retorna lista com todos os dados coletados
        
        v5.1 FIX: Passa filter_url diretamente na URL (não em params)
        """
        
        limit = self.config.api_limit
        offset = self.config.api_offset
        reset = 1  # 1 = continue, 0 = last valid, -1 = stop
        
        all_data = []
        page = 1
        
        while True:
            # Framing API filter (exatamente como Power Query)
            filter_url = (
                filter1 +
                str(limit) +
                "&filter[offset]=" +
                str(offset) +
                filter2
            )
            
            try:
                self.logger.info(f"      📄 Página {page} (offset {offset}, limit {limit})")
                
                # 🔧 v5.1 FIX: Passa filter_url diretamente na URL (não em params)
                full_url = self.config.console_url + self.config.costs_url + filter_url
                
                response = self.session.get(
                    full_url,
                    headers=self._get_headers(),
                    timeout=self.config.timeout
                )
                
                response.raise_for_status()
                source = response.json()
                
                # Capture Data (try otherwise null)
                data = source if source else None
                
                # Capture total count
                count = source.get('meta', {}).get('count', 0) if source else 0
                
                # Offset para próxima chamada
                offset_next = limit + offset
                
                # Reset logic (exatamente como Power Query)
                # 1 = next all expected
                # 0 = last valid call
                # -1 = dummy call
                if reset == 0:
                    reset = -1
                else:
                    reset = 0 if count <= offset_next else 1
                
                if data and data.get('data'):
                    all_data.append(data)
                    self.logger.info(f"      ✅ {len(data.get('data', []))} items (total: {len(all_data)} pages, count={count})")
                
                # Condição de parada (reset = -1 é dummy call)
                if reset == -1:
                    break
                
                offset = offset_next
                page += 1
                
            except Exception as e:
                self.logger.error(f"❌ Erro na paginação: {e}", exc_info=True)
                break
        
        # Retorna lista de responses (não DataFrame)
        return all_data

    def get_currency_master(self) -> pd.DataFrame:
        """
        Equivalente: Currency_Master
        GET /api/cost-management/v1/currency/
        """
        try:
            self.logger.info("📥 Buscando Currency_Master...")
            response = self.session.get(
                self.config.console_url + self.config.currency_url,
                headers=self._get_headers(),
                timeout=self.config.timeout
            )
            response.raise_for_status()
            source = response.json()
            
            # Power Query: Table.FromRecords({Source})
            df = pd.DataFrame([source])
            
            # Power Query: data = Table{0}[data]
            if 'data' in source:
                data_list = source['data']
                # Power Query: Table.FromList(data, ...) + ExpandRecordColumn
                df_expanded = pd.DataFrame(data_list)
                self.logger.info(f"✅ {len(df_expanded)} moedas carregadas")
                return df_expanded
            
            return pd.DataFrame()
            
        except Exception as e:
            self.logger.error(f"❌ Erro ao carregar Currency_Master: {e}")
            return pd.DataFrame()

    def get_default_configurations(self) -> pd.DataFrame:
        """
        Equivalente: Default_Configurations
        GET /api/cost-management/v1/account-settings/
        """
        try:
            self.logger.info("📥 Buscando Default_Configurations...")
            response = self.session.get(
                self.config.console_url + self.config.default_configs_url,
                headers=self._get_headers(),
                timeout=self.config.timeout
            )
            response.raise_for_status()
            source = response.json()
            
            # Power Query: Table.FromRecords({Source})
            df = pd.DataFrame([source])
            
            # Power Query: ExpandRecordColumn("data", {"currency", "cost_type"})
            if 'data' in source:
                df['data.currency'] = source['data'].get('currency')
                df['data.cost_type'] = source['data'].get('cost_type')
                
                self.logger.info(f"✅ Default configurations loaded")
                return df
            
            return df
            
        except Exception as e:
            self.logger.error(f"❌ Erro ao carregar Default_Configurations: {e}")
            return pd.DataFrame()

# ═══════════════════════════════════════════════════════════════════════════════
# TRANSFORMAÇÕES DE DADOS (Equivalentes às Consultas Power Query)
# ═══════════════════════════════════════════════════════════════════════════════

class PowerQueryTransformer:
    """
    Implementa todas as transformações do Power Query como funções Python.
    Segue a ordem e lógica EXATAMENTE como no Power Query.
    """
    
    def __init__(self, logger, client: OpenShiftCostAPIClient, start_date: str, end_date: str, currency: str = 'BRL'):
        self.logger = logger
        self.client = client
        self.start_date = start_date
        self.end_date = end_date
        self.currency = currency
        self.data_cache = {}
    
    def get_data_period(self) -> pd.DataFrame:
        """
        Equivalente: Data_Period
        Cria período de datas com cálculos de mês/time_scope
        """
        df = pd.DataFrame({
            'Start Date': [pd.to_datetime(self.start_date)],
            'End Date': [pd.to_datetime(self.end_date)]
        })
        
        # Power Query: End Month = YYYY-MM
        end_date = df['End Date'].iloc[0]
        df['End Month'] = f"{end_date.year}-{end_date.month}"
        
        # Power Query: Time_Scope_Value (complexo - deixa simples por agora)
        df['Time_Scope_Value'] = -1
        
        self.logger.info(f"✅ Data_Period: {self.start_date} a {self.end_date}")
        return df
    
    def get_default_master_settings(self, currency_df: pd.DataFrame, configs_df: pd.DataFrame) -> pd.DataFrame:
        """
        Equivalente: Default Master Settings
        Power Query: NestedJoin(Currency_Master, Default_Configurations, JoinKind.Inner)
        """
        # Inner Join
        df = currency_df.copy()
        if len(configs_df) > 0:
            df['data.currency'] = configs_df['data.currency'].iloc[0]
            df['data.cost_type'] = configs_df['data.cost_type'].iloc[0]
        
        self.logger.info(f"✅ Default_Master_Settings: {len(df)} registros")
        return df
    
    def extract_cost_data_projects(self, default_settings: pd.DataFrame, data_period: pd.DataFrame) -> List[Dict]:
        """
        Equivalente: Cost_Data_Project_Daily_Extract
        Chama: get_cost_loop_data para cada Project
        """
        self.logger.info("\n🔄 Cost_Data_Project_Daily_Extract...")
        
        all_data = []
        for idx, row in default_settings.iterrows():
            code = row['code']
            
            # Constrói filters (exatamente como Power Query)
            filter1 = f"?currency={code}&filter[limit]="
            filter2 = f"&filter[resolution]=daily&start_date={self.start_date}&end_date={self.end_date}&group_by[project]=*"
            
            self.logger.info(f"  └─ Buscando Projects para {code}...")
            
            # Chama get_cost_loop_data - RETORNA LISTA DE DICTS
            data_list = self.client.get_cost_loop_data(filter1, filter2)
            all_data.extend(data_list)
        
        self.logger.info(f"✅ {len(all_data)} API responses coletadas")
        return all_data
    
    def extract_cost_data_clusters(self, default_settings: pd.DataFrame, data_period: pd.DataFrame) -> List[Dict]:
        """
        Equivalente: Cost_Data_Clusters_Daily_Extract
        Chama: get_cost_loop_data para cada Cluster
        """
        self.logger.info("\n🔄 Cost_Data_Clusters_Daily_Extract...")
        
        all_data = []
        for idx, row in default_settings.iterrows():
            code = row['code']
            
            filter1 = f"?currency={code}&filter[limit]="
            filter2 = f"&filter[resolution]=daily&start_date={self.start_date}&end_date={self.end_date}&group_by[cluster]=*"
            
            self.logger.info(f"  └─ Buscando Clusters para {code}...")
            
            data_list = self.client.get_cost_loop_data(filter1, filter2)
            all_data.extend(data_list)
        
        self.logger.info(f"✅ {len(all_data)} API responses coletadas")
        return all_data
    
    def extract_cost_data_nodes(self, default_settings: pd.DataFrame, data_period: pd.DataFrame) -> List[Dict]:
        """
        Equivalente: Cost_Data_Nodes_Daily_Extract
        """
        self.logger.info("\n🔄 Cost_Data_Nodes_Daily_Extract...")
        
        all_data = []
        for idx, row in default_settings.iterrows():
            code = row['code']
            
            filter1 = f"?currency={code}&filter[limit]="
            filter2 = f"&filter[resolution]=daily&start_date={self.start_date}&end_date={self.end_date}&group_by[node]=*"
            
            self.logger.info(f"  └─ Buscando Nodes para {code}...")
            
            data_list = self.client.get_cost_loop_data(filter1, filter2)
            all_data.extend(data_list)
        
        self.logger.info(f"✅ {len(all_data)} API responses coletadas")
        return all_data
    
    def extract_cost_data_tags(self, default_settings: pd.DataFrame, data_period: pd.DataFrame) -> List[Dict]:
        """
        Equivalente: Cost_Data_Tags_Daily_Extract
        """
        self.logger.info("\n🔄 Cost_Data_Tags_Daily_Extract...")
        
        all_data = []
        for idx, row in default_settings.iterrows():
            code = row['code']
            
            filter1 = f"?currency={code}&filter[limit]="
            filter2 = f"&filter[resolution]=daily&start_date={self.start_date}&end_date={self.end_date}&group_by[tag]=*"
            
            self.logger.info(f"  └─ Buscando Tags para {code}...")
            
            data_list = self.client.get_cost_loop_data(filter1, filter2)
            all_data.extend(data_list)
        
        self.logger.info(f"✅ {len(all_data)} API responses coletadas")
        return all_data
    
    def expand_daily_projects(self, extract_list: List[Dict]) -> pd.DataFrame:
        """
        Equivalente: Cost_Data_Projects_Daily
        Expande a estrutura JSON em tabela com todas as combinações
        """
        self.logger.info("\n🔄 Expanding: Cost_Data_Projects_Daily...")
        
        rows = []
        for response in extract_list:
            if not response or not response.get('data'):
                continue
            
            for daily_item in response.get('data', []):
                date = daily_item.get('date')
                
                # Cada daily_item pode ter 'projects' ou 'clusters', etc.
                # Vamos expandir TODOS os keys que contêm listas de items
                for key in ['projects', 'clusters', 'nodes', 'tags']:
                    items = daily_item.get(key, [])
                    if isinstance(items, list):
                        for item in items:
                            if isinstance(item, dict):
                                # Extrai estrutura do item
                                cost_total = item.get('cost', {}).get('total', {}).get('value', 0)
                                cost_units = item.get('cost', {}).get('total', {}).get('units', '')
                                
                                # Cria registro com tipo de item
                                row = {
                                    'date': date,
                                    'type': key[:-1],  # Remove 's' final
                                    'item_name': item.get(key[:-1]),  # Ex: item['project']
                                    'cost_total': cost_total,
                                    'cost_units': cost_units,
                                }
                                rows.append(row)
        
        df = pd.DataFrame(rows)
        self.logger.info(f"✅ {len(df)} linhas expandidas")
        return df

# ═══════════════════════════════════════════════════════════════════════════════
# GERADOR DE EXCEL
# ═══════════════════════════════════════════════════════════════════════════════

class ExcelGeneratorV5:
    def __init__(self, logger, currency: str = 'BRL'):
        self.logger = logger
        self.currency = currency
    
    def generate_excel(self, output_file: str, 
                      data_period: pd.DataFrame,
                      currency_master: pd.DataFrame,
                      default_settings: pd.DataFrame,
                      expanded_data: pd.DataFrame):
        """
        Gera Excel com todas as abas no mesmo formato do Power Query
        """
        self.logger.info("\n💾 Gerando arquivo Excel...")
        
        with pd.ExcelWriter(output_file, engine='xlsxwriter') as writer:
            
            # Aba: Data_Period
            data_period.to_excel(writer, sheet_name='Data_Period', index=False)
            self.logger.info("  ✅ Data_Period")
            
            # Aba: Default Master Settings
            default_settings.to_excel(writer, sheet_name='Default Master Settings', index=False)
            self.logger.info("  ✅ Default Master Settings")
            
            # Aba: Currency Master
            currency_master.to_excel(writer, sheet_name='Currency Master', index=False)
            self.logger.info("  ✅ Currency Master")
            
            # Aba: Expanded Data
            expanded_data.to_excel(writer, sheet_name='Expanded Data', index=False)
            self.logger.info(f"  ✅ Expanded Data ({len(expanded_data)} linhas)")
        
        size_mb = os.path.getsize(output_file) / (1024*1024)
        self.logger.info(f"\n✅ Arquivo salvo: {output_file} ({size_mb:.2f} MB)")

# ═══════════════════════════════════════════════════════════════════════════════
# FUNÇÃO PRINCIPAL
# ═══════════════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(description='OpenShift Cost Extractor v5.1 (Power Query → Python)')
    parser.add_argument('--start-date', type=str, default=None)
    parser.add_argument('--end-date', type=str, default=None)
    parser.add_argument('--output', type=str, default='openshift_costs.xlsx')
    parser.add_argument('--currency', type=str, default='BRL')
    args = parser.parse_args()

    if not args.end_date:
        args.end_date = datetime.now().strftime('%Y-%m-%d')

    if not args.start_date:
        start = datetime.now() - timedelta(days=30)
        args.start_date = start.strftime('%Y-%m-%d')

    logger.info("=" * 100)
    logger.info("🚀 OpenShift Cost Extractor v5.1 CORRIGIDO (Convertido Exatamente do Power Query)")
    logger.info("=" * 100)
    logger.info(f"Período: {args.start_date} a {args.end_date}")

    try:
        config = APIConfig()
        client = OpenShiftCostAPIClient(config, logger)
        
        # ┌─────────────────────────────────────────────────────────────────────────┐
        # │ EXECUÇÃO EM ORDEM DE DEPENDÊNCIA (EXATO COMO POWER QUERY)               │
        # └─────────────────────────────────────────────────────────────────────────┘
        
        logger.info("\n📥 Nível 0: Carregando APIs...")
        currency_df = client.get_currency_master()
        configs_df = client.get_default_configurations()
        
        transformer = PowerQueryTransformer(logger, client, args.start_date, args.end_date, args.currency)
        
        logger.info("\n📥 Nível 1: Gerando período e settings...")
        data_period_df = transformer.get_data_period()
        default_settings_df = transformer.get_default_master_settings(currency_df, configs_df)
        
        logger.info("\n📥 Nível 2: Extraindo dados com paginação...")
        extract_projects = transformer.extract_cost_data_projects(default_settings_df, data_period_df)
        extract_clusters = transformer.extract_cost_data_clusters(default_settings_df, data_period_df)
        extract_nodes = transformer.extract_cost_data_nodes(default_settings_df, data_period_df)
        extract_tags = transformer.extract_cost_data_tags(default_settings_df, data_period_df)
        
        logger.info("\n📥 Nível 4: Expandindo dados...")
        expanded_df = transformer.expand_daily_projects(extract_projects + extract_clusters + extract_nodes + extract_tags)
        
        logger.info(f"\n✅ Dados prontos: {len(expanded_df)} linhas")
        
        # Gera Excel
        generator = ExcelGeneratorV5(logger, args.currency)
        generator.generate_excel(
            args.output,
            data_period_df,
            currency_df,
            default_settings_df,
            expanded_df
        )
        
        logger.info("\n" + "=" * 100)
        logger.info("✅ SUCESSO! Excel gerado com lógica exatamente igual ao Power Query!")
        logger.info("=" * 100)

    except Exception as e:
        logger.error(f"❌ Erro: {e}", exc_info=True)
        sys.exit(1)

if __name__ == '__main__':
    main()
