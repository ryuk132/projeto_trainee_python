#!/usr/bin/env python3.11
"""
OpenShift Cost Management - Extrator de Dados REFACTORADO
Versão limpa, modular e funcional
"""

import os
import sys
import logging
from typing import Dict, List, Any, Optional
from dataclasses import dataclass
from datetime import datetime, timedelta
import requests
import pandas as pd
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
import argparse


# Configuração de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s'
)
logger = logging.getLogger(__name__)


@dataclass
class APIConfig:
    """Configurações da API"""
    client_id: str = os.getenv('OPENSHIFT_CLIENT_ID', '')
    client_secret: str = os.getenv('OPENSHIFT_CLIENT_SECRET', '')
    auth_url: str = 'https://sso.redhat.com/auth/realms/redhat-external/protocol/openid-connect/token'
    api_base_url: str = 'https://console.redhat.com/api/cost-management/v1'
    timeout: int = 30
    max_retries: int = 3
    backoff_factor: float = 0.5


class OpenShiftCostAPIClient:
    """Cliente para API do OpenShift Cost Management"""
    
    def __init__(self, config: APIConfig):
        self.config = config
        self.session = self._create_session()
        self.access_token = None
        self.token_expires_at = None

    def _create_session(self) -> requests.Session:
        """Cria sessão HTTP com retry automático"""
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
        """Obtém token de autenticação"""
        logger.info("🔐 Obtendo token de autenticação...")
        auth_data = {
            'grant_type': 'client_credentials',
            'client_id': self.config.client_id,
            'client_secret': self.config.client_secret
        }
        response = self.session.post(
            self.config.auth_url, 
            data=auth_data, 
            timeout=self.config.timeout
        )
        response.raise_for_status()
        
        token_data = response.json()
        self.access_token = token_data['access_token']
        expires_in = token_data.get('expires_in', 900)
        self.token_expires_at = datetime.now() + timedelta(seconds=expires_in - 60)
        
        logger.info("✅ Token obtido")
        return self.access_token

    def _ensure_token(self) -> str:
        """Garante que o token está válido"""
        if not self.access_token or (self.token_expires_at and datetime.now() >= self.token_expires_at):
            return self._get_token()
        return self.access_token

    def _get_headers(self) -> Dict[str, str]:
        """Headers com token de autenticação"""
        return {
            'Authorization': f'Bearer {self._ensure_token()}',
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        }

    def _make_paginated_request(self, endpoint: str, base_params: Dict[str, str]) -> List[Dict]:
        """Faz requisições paginadas e retorna todos os itens"""
        all_items = []
        limit = 250
        offset = 0
        page = 1
        
        while True:
            params = base_params.copy()
            params.update({
                'filter[limit]': limit,
                'filter[offset]': offset,
                'filter[resolution]': 'daily'
            })
            
            response = self.session.get(
                endpoint,
                params=params,
                headers=self._get_headers(),
                timeout=self.config.timeout
            )
            response.raise_for_status()
            
            data = response.json()
            items = data.get('data', [])
            meta = data.get('meta', {})
            
            if not items:
                break
                
            all_items.extend(items)
            count = meta.get('count', 0)
            logger.debug(f"  Página {page}: {len(items)} itens (total: {len(all_items)}/{count})")
            
            offset += limit
            page += 1
            
            if len(all_items) >= count:
                break
        
        return all_items

    def get_clusters(self, start_date: str, end_date: str, currency: str = 'BRL') -> List[str]:
        """Obtém lista única de clusters"""
        logger.info("📍 Buscando clusters...")
        endpoint = f"{self.config.api_base_url}/reports/openshift/costs"
        params = {
            'start_date': start_date,
            'end_date': end_date,
            'currency': currency,
            'group_by[cluster]': 'cluster'
        }
        
        all_items = self._make_paginated_request(endpoint, params)
        clusters = {cluster.get('cluster') for item in all_items 
                   for cluster in item.get('clusters', []) 
                   if cluster.get('cluster')}
        
        logger.info(f"✅ {len(clusters)} clusters encontrados")
        return sorted(clusters)

    def get_projects_by_cluster(self, cluster_id: str, start_date: str, end_date: str, 
                              currency: str = 'BRL') -> List[Dict[str, Any]]:
        """Obtém projetos para um cluster específico"""
        logger.info(f"📊 Cluster: {cluster_id[:8]}...")
        endpoint = f"{self.config.api_base_url}/reports/openshift/costs"
        params = {
            'start_date': start_date,
            'end_date': end_date,
            'currency': currency,
            'filter[cluster]': cluster_id,
            'group_by[project]': 'project'
        }
        
        all_items = self._make_paginated_request(endpoint, params)
        rows = []
        
        for item in all_items:
            date = item.get('date')
            projects = item.get('projects', [])
            
            for project in projects:
                project_name = project.get('project', 'Unknown')
                values = project.get('values', [])
                
                for value in values:
                    cost = value.get('cost', {})
                    total_cost = cost.get('total', {}).get('value', 0)
                    
                    rows.append({
                        'cluster': cluster_id,
                        'date': date,
                        'project': project_name,
                        'value': total_cost
                    })
        
        return rows

    def get_tags(self, limit: int = 1000) -> List[Dict[str, Any]]:
        """Obtém lista de tags"""
        logger.info("🏷️  Buscando tags...")
        endpoint = f"{self.config.api_base_url}/tags/openshift"
        params = {'limit': limit}
        
        response = self.session.get(
            endpoint,
            params=params,
            headers=self._get_headers(),
            timeout=self.config.timeout
        )
        response.raise_for_status()
        
        data = response.json()
        tags = data.get('data', [])
        logger.info(f"✅ {len(tags)} tags obtidas")
        return tags


class ExcelFormatter:
    """Formatador de dados para Excel"""
    
    def __init__(self, currency: str = 'BRL'):
        self.currency = currency

    def format_cluster_projects(self, rows: List[Dict]) -> pd.DataFrame:
        """Formata dados para aba OS Cost Cluster Projects"""
        formatted_rows = []
        
        for row in rows:
            date_obj = pd.to_datetime(row['date'])
            formatted_rows.append({
                'code': self.currency,
                'Group By Code': 'cluster',
                'cluster': row['cluster'],
                'date': date_obj,
                'project': row['project'],
                'value': row['value'],
                'units': self.currency,
                'Filter Month': date_obj.strftime('%Y-%m')
            })
        
        return pd.DataFrame(formatted_rows)

    def create_excel(self, rows: List[Dict], output_file: str, start_date: str, 
                    end_date: str, tags: List[Dict]):
        """Cria arquivo Excel completo"""
        logger.info("📊 Criando arquivo Excel...")
        
        with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
            # DataPeriod
            df_period = pd.DataFrame({
                'Start Date': ['Start Date', start_date],
                'End Date': ['End Date', end_date],
                'Guidelines': [
                    None,
                    'Enter start and end dates from the same month.',
                    'If the need is to get data from multiple months, make copies of the file.',
                    'The date range should be no earlier to 4 months prior to the current month.'
                ]
            })
            df_period.to_excel(writer, sheet_name='DataPeriod', index=False, header=False)

            # Default Master Settings
            df_settings = pd.DataFrame([{
                'code': 'BRL',
                'name': 'Brazilian Real',
                'symbol': 'R$',
                'description': 'BRL - Brazilian Real',
                'DefaultConfigurations.data.currency': 'BRL',
                'DefaultConfigurations.data.costtype': 'calculatedamortizedcost'
            }])
            df_settings.to_excel(writer, sheet_name='Default Master Settings', index=False)

            # Project Overhead Cost Types
            df_cost_types = pd.DataFrame({
                'Code': ['cost', 'distributedcost'],
                'Description': ['Dont distribute overhead costs', 'Distribute through cost models']
            })
            df_cost_types.to_excel(writer, sheet_name='Project Overhead Cost Types', index=False)

            # OpenShift Group Bys
            df_group_bys = pd.DataFrame({
                'Group By': ['Cluster', 'Node', 'Project', 'Tag'],
                'Group By Code': ['cluster', 'node', 'project', 'tag']
            })
            df_group_bys.to_excel(writer, sheet_name='OpenShift Group Bys', index=False)

            # OS Tag Keys
            tag_data = [{'count': 1, 'key': tag.get('key', 'produto'), 'enabled': True, 'Group By': 'tag'} 
                       for tag in tags] or [{'count': 1, 'key': 'produto', 'enabled': True, 'Group By': 'tag'}]
            pd.DataFrame(tag_data).to_excel(writer, sheet_name='OS Tag Keys', index=False)

            # OS Cost Cluster Projects (PRINCIPAL)
            df_main = self.format_cluster_projects(rows)
            df_main.to_excel(writer, sheet_name='OS Cost Cluster Projects', index=False)

            # Abas vazias
            for sheet in ['OS Cost Project Tags', 'OS Costs Daily']:
                pd.DataFrame().to_excel(writer, sheet_name=sheet, index=False)

        logger.info(f"✅ Excel criado: {output_file} ({len(rows)} registros)")


def parse_args():
    """Parse dos argumentos da linha de comando"""
    parser = argparse.ArgumentParser(description='Extrator OpenShift Cost Management')
    parser.add_argument('--start-date', type=str, help='Data inicial (YYYY-MM-DD)', default=None)
    parser.add_argument('--end-date', type=str, help='Data final (YYYY-MM-DD)', default=None)
    parser.add_argument('--output', type=str, default='openshift_costs.xlsx')
    parser.add_argument('--currency', type=str, default='BRL')
    return parser.parse_args()


def main():
    """Função principal"""
    args = parse_args()
    
    # Datas default
    end_date = args.end_date or datetime.now().strftime('%Y-%m-%d')
    start_date = args.start_date or (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
    
    logger.info("=" * 60)
    logger.info("🚀 OpenShift Cost Management - Extrator de Dados")
    logger.info(f"Período: {start_date} → {end_date}")
    logger.info(f"Saída: {args.output}")
    logger.info("=" * 60)

    try:
        # Inicializa cliente e formatter
        config = APIConfig()
        client = OpenShiftCostAPIClient(config)
        formatter = ExcelFormatter(args.currency)

        # PASSO 1: Obter clusters
        clusters = client.get_clusters(start_date, end_date, args.currency)
        if not clusters:
            logger.error("❌ Nenhum cluster encontrado!")
            sys.exit(1)

        # PASSO 2: Para cada cluster, obter projetos
        all_rows = []
        for i, cluster_id in enumerate(clusters, 1):
            logger.info(f"[{i}/{len(clusters)}] Processando {cluster_id[:8]}...")
            rows = client.get_projects_by_cluster(cluster_id, start_date, end_date, args.currency)
            all_rows.extend(rows)

        # PASSO 3: Obter tags e criar Excel
        tags = client.get_tags()
        formatter.create_excel(all_rows, args.output, start_date, end_date, tags)

        logger.info("🎉 Processo concluído com sucesso!")
        
    except Exception as e:
        logger.error(f"❌ Erro: {e}", exc_info=True)
        sys.exit(1)


if __name__ == '__main__':
    main()
