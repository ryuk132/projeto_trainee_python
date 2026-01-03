#!/usr/bin/env python3.11

"""
OpenShift Cost Management - Extrator de Dados CORRIGIDO
Gera arquivo Excel com EXATAMENTE a mesma estrutura do arquivo original
COM A LÓGICA CORRIGIDA DE TRANSFORMAÇÃO DOS DADOS
"""

import os
import sys
import logging
import requests
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
import argparse

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class APIConfig:
    def __init__(self):
        self.client_id = os.getenv('OPENSHIFT_CLIENT_ID', '')
        self.client_secret = os.getenv('OPENSHIFT_CLIENT_SECRET', '')
        self.auth_url = 'https://sso.redhat.com/auth/realms/redhat-external/protocol/openid-connect/token'
        self.api_base_url = 'https://console.redhat.com/api/cost-management/v1'
        self.timeout = 30
        self.max_retries = 3
        self.backoff_factor = 0.5

class OpenShiftCostAPIClient:
    def __init__(self, config: APIConfig, logger):
        self.config = config
        self.logger = logger
        self.access_token = None
        self.token_expires_at = None
        self.session = self._create_session()

    def _create_session(self) -> requests.Session:
        session = requests.Session()
        retry_strategy = Retry(total=self.config.max_retries, status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["HEAD", "GET", "OPTIONS", "POST"], backoff_factor=self.config.backoff_factor)
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        return session

    def _get_token(self) -> str:
        try:
            self.logger.info("Obtendo novo access token...")
            auth_data = {'grant_type': 'client_credentials', 'client_id': self.config.client_id, 'client_secret': self.config.client_secret}
            response = self.session.post(self.config.auth_url, data=auth_data, timeout=self.config.timeout)
            response.raise_for_status()
            token_response = response.json()
            self.access_token = token_response['access_token']
            expires_in = token_response.get('expires_in', 900)
            self.token_expires_at = datetime.now() + timedelta(seconds=expires_in - 60)
            self.logger.info("Token obtido com sucesso")
            return self.access_token
        except Exception as e:
            self.logger.error(f"Erro ao obter token: {e}")
            raise

    def _ensure_token(self) -> str:
        if not self.access_token or (self.token_expires_at and datetime.now() >= self.token_expires_at):
            self._get_token()
        return self.access_token

    def _get_headers(self) -> Dict[str, str]:
        token = self._ensure_token()
        return {'Authorization': f'Bearer {token}', 'Content-Type': 'application/json', 'Accept': 'application/json'}

    def get_costs_by_groupby(self, start_date: str, end_date: str, currency: str = 'BRL') -> Dict[str, List[Dict[str, Any]]]:
        all_data = {}
        group_by_configs = [
            {'type': 'cluster', 'params': {'group_by[cluster]': '*'}},
            {'type': 'node', 'params': {'group_by[cluster]': '*', 'group_by[node]': '*'}},
            {'type': 'project', 'params': {'group_by[project]': '*'}},
            {'type': 'tag:produto', 'params': {'group_by[tag:produto]': '*'}},
        ]

        for group_by_config in group_by_configs:
            group_by_type = group_by_config['type']
            group_by_params = group_by_config['params']
            try:
                self.logger.info(f"Buscando dados agrupados por {group_by_type}...")
                all_items = []
                limit = 250
                offset = 0
                page = 1
                while True:
                    url = f"{self.config.api_base_url}/reports/openshift/costs/"
                    params = {'currency': currency, 'filter[limit]': limit, 'filter[offset]': offset,
                    'filter[resolution]': 'daily', 'start_date': start_date, 'end_date': end_date,
                    'order_by[cost]': 'desc'}
                    params.update(group_by_params)
                    response = self.session.get(url, params=params, headers=self._get_headers(), timeout=self.config.timeout)
                    response.raise_for_status()
                    data = response.json()
                    items = data.get('data', [])
                    meta = data.get('meta', {})
                    if not items:
                        break
                    all_items.extend(items)
                    count = meta.get('count', 0)
                    offset_next = offset + limit
                    self.logger.debug(f" Pagina {page}: obtidos {len(items)} registros (offset={offset}, total={len(all_items)}/{count})")
                    if count <= offset_next:
                        break
                    offset = offset_next
                    page += 1

                if 'tag' in group_by_type:
                    key = 'tag'
                    tag_key_name = group_by_type.split(':')[1]
                    if key not in all_data:
                        all_data[key] = []
                    all_data['_tag_key_name'] = tag_key_name
                    all_data[key].extend(all_items)
                else:
                    key = group_by_type
                    if key not in all_data:
                        all_data[key] = []
                    all_data[key].extend(all_items)

                self.logger.info(f"Obtidos {len(all_items)} registros para {group_by_type} ({page} paginas)")
            except Exception as e:
                self.logger.warning(f"Aviso: Falha ao obter dados para {group_by_type}: {e}")

        for key in ['cluster', 'node', 'project', 'tag']:
            if key not in all_data:
                all_data[key] = []

        return all_data

    def get_tags(self, limit: int = 1000) -> List[Dict[str, Any]]:
        try:
            self.logger.info("Buscando dados de tags...")
            url = f"{self.config.api_base_url}/tags/openshift"
            params = {'limit': limit}
            response = self.session.get(url, params=params, headers=self._get_headers(), timeout=self.config.timeout)
            response.raise_for_status()
            data = response.json()
            tags = data.get('data', [])
            self.logger.info(f"Tags obtidas: {len(tags)}")
            return tags
        except Exception as e:
            self.logger.error(f"Erro ao obter tags: {e}")
            return []

class ExcelFormatterFixed:
    """Classe corrigida que implementa a lógica exata da planilha Excel"""

    def __init__(self, logger, currency: str = 'BRL'):
        self.logger = logger
        self.currency = currency

    def _flatten_cost_data(self, cost_item: Dict, group_by_type: str, item_name: str, date: str) -> Dict:
        """Flattena dados de custos para a estrutura esperada"""
        row = {
            'code': self.currency,
            'Group By Code': group_by_type,
            'meta.distributed_overhead': True,
            'date': pd.to_datetime(date),
            'Name': item_name,
            'values.date': pd.to_datetime(date),
            'values.classification': cost_item.get('classification', ''),
            'values.source_uuid': ','.join(cost_item.get('source_uuid', [])) if isinstance(cost_item.get('source_uuid'), list) else '',
            'values.clusters': ','.join(cost_item.get('clusters', [])) if isinstance(cost_item.get('clusters'), list) else '',
        }

        # Infrastructure
        infra = cost_item.get('infrastructure', {})
        row['values.infrastructure.raw.value'] = infra.get('raw', {}).get('value', 0)
        row['values.infrastructure.raw.units'] = infra.get('raw', {}).get('units', 'BRL')
        row['values.infrastructure.markup.value'] = infra.get('markup', {}).get('value', 0)
        row['values.infrastructure.markup.units'] = infra.get('markup', {}).get('units', 'BRL')
        row['values.infrastructure.usage.value'] = infra.get('usage', {}).get('value', 0)
        row['values.infrastructure.usage.units'] = infra.get('usage', {}).get('units', 'BRL')
        row['values.infrastructure.total.value'] = infra.get('total', {}).get('value', 0)
        row['values.infrastructure.total.units'] = infra.get('total', {}).get('units', 'BRL')

        # Supplementary
        supp = cost_item.get('supplementary', {})
        row['values.supplementary.raw.value'] = supp.get('raw', {}).get('value', 0)
        row['values.supplementary.raw.units'] = supp.get('raw', {}).get('units', 'BRL')
        row['values.supplementary.markup.value'] = supp.get('markup', {}).get('value', 0)
        row['values.supplementary.markup.units'] = supp.get('markup', {}).get('units', 'BRL')
        row['values.supplementary.usage.value'] = supp.get('usage', {}).get('value', 0)
        row['values.supplementary.usage.units'] = supp.get('usage', {}).get('units', 'BRL')
        row['values.supplementary.total.value'] = supp.get('total', {}).get('value', 0)
        row['values.supplementary.total.units'] = supp.get('total', {}).get('units', 'BRL')

        # Cost
        cost = cost_item.get('cost', {})
        row['values.cost.raw.value'] = cost.get('raw', {}).get('value', 0)
        row['values.cost.raw.units'] = cost.get('raw', {}).get('units', 'BRL')
        row['values.cost.markup.value'] = cost.get('markup', {}).get('value', 0)
        row['values.cost.markup.units'] = cost.get('markup', {}).get('units', 'BRL')
        row['values.cost.usage.value'] = cost.get('usage', {}).get('value', 0)
        row['values.cost.usage.units'] = cost.get('usage', {}).get('units', 'BRL')
        row['values.cost.platform_distributed.value'] = cost.get('platform_distributed', {}).get('value', 0)
        row['values.cost.platform_distributed.units'] = cost.get('platform_distributed', {}).get('units', 'BRL')
        row['values.cost.worker_unallocated_distributed.value'] = cost.get('worker_unallocated_distributed', {}).get('value', 0)
        row['values.cost.worker_unallocated_distributed.units'] = cost.get('worker_unallocated_distributed', {}).get('units', 'BRL')
        row['values.cost.distributed.value'] = cost.get('distributed', {}).get('value', 0)
        row['values.cost.distributed.units'] = cost.get('distributed', {}).get('units', 'BRL')
        row['values.cost.total.value'] = cost.get('total', {}).get('value', 0)
        row['values.cost.total.units'] = cost.get('total', {}).get('units', 'BRL')

        row['values.delta_percent'] = cost_item.get('delta_percent', 0)
        row['key'] = cost_item.get('key', None)
        row['values.delta_value'] = cost_item.get('delta_value', 0)

        return row

    def _create_os_costs_daily(self, all_data: Dict) -> pd.DataFrame:
        """Cria a aba OS Costs Daily com dados flattenados"""
        rows = []

        # Project data
        for cost_item in all_data.get('project', []):
            date = cost_item.get('date', '')
            projects = cost_item.get('projects', [])
            for project in projects:
                project_name = project.get('project', '')
                values_list = project.get('values', [])
                for value_item in values_list:
                    row = self._flatten_cost_data(value_item, 'project', project_name, date)
                    row['Filter Month'] = pd.to_datetime(date).strftime('%Y-%m')
                    rows.append(row)

        # Cluster data
        for cost_item in all_data.get('cluster', []):
            date = cost_item.get('date', '')
            clusters = cost_item.get('clusters', [])
            for cluster in clusters:
                cluster_name = cluster.get('cluster', '')
                values_list = cluster.get('values', [])
                for value_item in values_list:
                    row = self._flatten_cost_data(value_item, 'cluster', cluster_name, date)
                    row['Filter Month'] = pd.to_datetime(date).strftime('%Y-%m')
                    rows.append(row)

        # Node data
        for cost_item in all_data.get('node', []):
            date = cost_item.get('date', '')
            clusters = cost_item.get('clusters', [])
            for cluster in clusters:
                cluster_name = cluster.get('cluster', '')
                nodes = cluster.get('nodes', [])
                for node in nodes:
                    node_name = node.get('node', 'No-node')
                    values_list = node.get('values', [])
                    for value_item in values_list:
                        row = self._flatten_cost_data(value_item, 'node', node_name, date)
                        row['Filter Month'] = pd.to_datetime(date).strftime('%Y-%m')
                        rows.append(row)

        # Tag data
        tag_key_name = all_data.get('_tag_key_name', 'produto')
        for cost_item in all_data.get('tag', []):
            date = cost_item.get('date', '')
            tags_list = cost_item.get(f'{tag_key_name}s', [])
            for tag in tags_list:
                tag_name = tag.get('tag', 'No-tag')
                values_list = tag.get('values', [])
                for value_item in values_list:
                    row = self._flatten_cost_data(value_item, 'tag', tag_name, date)
                    row['Filter Month'] = pd.to_datetime(date).strftime('%Y-%m')
                    rows.append(row)

        return pd.DataFrame(rows)

    def _create_os_cost_cluster_projects(self, all_data: Dict) -> pd.DataFrame:
        """Cria aba OS Cost Cluster Projects - Agrupa por cluster/projeto"""
        rows = []

        for cost_item in all_data.get('project', []):
            date = cost_item.get('date', '')
            projects = cost_item.get('projects', [])
            for project in projects:
                project_name = project.get('project', '')
                values_list = project.get('values', [])
                for value_item in values_list:
                    total_cost = value_item.get('cost', {}).get('total', {}).get('value', 0)
                    rows.append({
                        'code': self.currency,
                        'Group By Code': 'cluster',
                        'cluster': cost_item.get('cluster', ''),  # Ajuste conforme necessário
                        'date': pd.to_datetime(date),
                        'project': project_name,
                        'value': total_cost,
                        'units': self.currency,
                        'Filter Month': pd.to_datetime(date).strftime('%Y-%m'),
                    })

        return pd.DataFrame(rows)

    def _create_os_cost_project_tags(self, all_data: Dict) -> pd.DataFrame:
        """Cria aba OS Cost Project Tags - Mapeia projetos com tags"""
        rows = []

        tag_key_name = all_data.get('_tag_key_name', 'produto')

        for cost_item in all_data.get('tag', []):
            date = cost_item.get('date', '')
            tags_list = cost_item.get(f'{tag_key_name}s', [])
            for tag in tags_list:
                tag_name = tag.get('tag', '')
                values_list = tag.get('values', [])
                for value_item in values_list:
                    project_name = value_item.get('project', '')
                    rows.append({
                        'code': self.currency,
                        'date': pd.to_datetime(date),
                        'project': project_name,
                        'key': tag_key_name,
                        'values': tag_name,
                        'enabled': True,
                        'Filter Month': pd.to_datetime(date).strftime('%Y-%m'),
                    })

        return pd.DataFrame(rows)

    def format_to_excel(self, all_data: Dict, output_file: str, start_date: str, end_date: str, tags: List[Dict]):
        """Gera Excel com todas as abas conforme estrutura da planilha original"""
        self.logger.info(f"Formatando dados para Excel...")

        with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
            # Data_Period
            df_period = pd.DataFrame({
                'Start Date': ['Start Date', start_date, None, None, None],
                'End Date': ['End Date', end_date, None, None, None],
                'Col3': [None, None, None, None, None],
                'Col4': [None, None, None, None, None],
                'Col5': [None, None, None, None, None],
                'Col6': [None, None, None, None, None],
                'Col7': [None, None, None, None, None],
                'Guidelines': [None, 'Guidelines',
                'Enter start and end dates from the same month.',
                'If the need is to get data from multiple months, make copies of the file to ensure start and end date are from the same month.',
                'The date range should be no earlier to 4 months prior to the current month.'],
            })
            df_period.to_excel(writer, sheet_name='Data_Period', index=False, header=False)

            # Default Master Settings
            df_settings = pd.DataFrame({
                'code': ['BRL'],
                'name': ['Brazilian Real'],
                'symbol': ['R$'],
                'description': ['BRL (R$) - Brazilian Real'],
                'Default_Configurations.data.currency': ['BRL'],
                'Default_Configurations.data.cost_type': ['calculated_amortized_cost'],
            })
            df_settings.to_excel(writer, sheet_name='Default Master Settings', index=False)

            # Project Overhead Cost Types
            df_cost_types = pd.DataFrame({
                'Code': ['cost', 'distributed_cost'],
                'Description': ["Don't distribute overhead costs", 'Distribute through cost models'],
            })
            df_cost_types.to_excel(writer, sheet_name='Project Overhead Cost Types', index=False)

            # OpenShift Group Bys
            df_group_bys = pd.DataFrame({
                'Group By': ['Cluster', 'Node', 'Project', 'Tag'],
                'Group By Code': ['cluster', 'node', 'project', 'tag'],
            })
            df_group_bys.to_excel(writer, sheet_name='OpenShift Group Bys', index=False)

            # OS Tag Keys
            tag_data = []
            for tag in tags:
                tag_data.append({'count': 1, 'key': tag.get('key', 'produto'), 'enabled': True, 'Group By': 'tag'})
            if not tag_data:
                tag_data = [{'count': 1, 'key': 'produto', 'enabled': True, 'Group By': 'tag'}]
            df_tags = pd.DataFrame(tag_data)
            df_tags.to_excel(writer, sheet_name='OS Tag Keys', index=False)

            # OS Cost Cluster Projects
            df_cluster_projects = self._create_os_cost_cluster_projects(all_data)
            if not df_cluster_projects.empty:
                df_cluster_projects.to_excel(writer, sheet_name='OS Cost Cluster Projects', index=False)
            else:
                pd.DataFrame().to_excel(writer, sheet_name='OS Cost Cluster Projects', index=False)

            # OS Cost Project Tags
            df_project_tags = self._create_os_cost_project_tags(all_data)
            if not df_project_tags.empty:
                df_project_tags.to_excel(writer, sheet_name='OS Cost Project Tags', index=False)
            else:
                pd.DataFrame().to_excel(writer, sheet_name='OS Cost Project Tags', index=False)

            # OS Costs Daily
            df_daily = self._create_os_costs_daily(all_data)
            df_daily.to_excel(writer, sheet_name='OS Costs Daily', index=False)

        self.logger.info(f"Excel gerado com sucesso: {output_file}")

def main():
    parser = argparse.ArgumentParser(description='OpenShift Cost Management - Extrator de Dados')
    parser.add_argument('--start-date', type=str, help='Data inicial (YYYY-MM-DD)', default=None)
    parser.add_argument('--end-date', type=str, help='Data final (YYYY-MM-DD)', default=None)
    parser.add_argument('--output', type=str, help='Arquivo de saída', default='openshift_costs.xlsx')
    parser.add_argument('--currency', type=str, help='Moeda', default='BRL')
    args = parser.parse_args()

    if not args.end_date:
        args.end_date = datetime.now().strftime('%Y-%m-%d')

    if not args.start_date:
        start = datetime.now() - timedelta(days=30)
        args.start_date = start.strftime('%Y-%m-%d')

    logger.info(f"Iniciando extração de dados do OpenShift Cost Management")
    logger.info(f"Período: {args.start_date} a {args.end_date}")
    logger.info(f"Moeda: {args.currency}")

    try:
        config = APIConfig()
        client = OpenShiftCostAPIClient(config, logger)
        all_data = client.get_costs_by_groupby(args.start_date, args.end_date, args.currency)
        tags = client.get_tags()
        formatter = ExcelFormatterFixed(logger, args.currency)
        formatter.format_to_excel(all_data, args.output, args.start_date, args.end_date, tags)
        logger.info(f"Processo concluído com sucesso!")
    except Exception as e:
        logger.error(f"Erro durante a execução: {e}", exc_info=True)
        sys.exit(1)

if __name__ == '__main__':
    main()
