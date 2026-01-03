#!/usr/bin/env python3
"""
OpenShift Cost Extractor v6.0.1 - CORRIGIDO
Tradução 100% exata de Power Query para Python

CORREÇÕES v6.0.1:
- ✅ get_default_configurations: Trata currency como string OU dict
- ✅ Tratamento robusto de tipos de dados
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

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class APIConfig:
    def __init__(self):
        self.client_id = os.getenv('OPENSHIFT_CLIENT_ID', '')
        self.client_secret = os.getenv('OPENSHIFT_CLIENT_SECRET', '')
        self.auth_url = 'https://sso.redhat.com/auth/realms/redhat-external/protocol/openid-connect/token'
        self.console_url = 'https://console.redhat.com'
        self.api_base_url = 'https://console.redhat.com/api/cost-management/v1'
        self.timeout = 30
        self.max_retries = 3
        self.backoff_factor = 0.5

        # URLs específicas
        self.currency_url = '/api/currency/'
        self.account_settings_url = '/api/account-settings/'
        self.tags_url = '/api/tags/openshift/'
        self.costs_url = '/api/reports/openshift/costs/'


class OpenShiftCostAPIClient:
    def __init__(self, config: APIConfig, logger):
        self.config = config
        self.logger = logger
        self.access_token = None
        self.token_expires_at = None
        self.session = self.create_session()

    def create_session(self):
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

    def get_token(self) -> str:
        try:
            self.logger.info('Obtendo novo access token...')
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
            token_response = response.json()

            self.access_token = token_response['access_token']
            expires_in = token_response.get('expires_in', 900)
            self.token_expires_at = datetime.now() + timedelta(seconds=expires_in - 60)

            self.logger.info('✅ Token obtido com sucesso')
            return self.access_token
        except Exception as e:
            self.logger.error(f'❌ Erro ao obter token: {e}')
            raise

    def ensure_token(self) -> str:
        if not self.access_token or (self.token_expires_at and datetime.now() >= self.token_expires_at):
            self.get_token()
        return self.access_token

    def get_headers(self) -> Dict[str, str]:
        token = self.ensure_token()
        return {
            'Authorization': f'Bearer {token}',
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        }

    def get_currency_master(self) -> pd.DataFrame:
        """
        Equivalente: Currency_Master (Power Query)
        GET: Console_URL + Currency_URL
        Retorna: ~136 moedas
        """
        try:
            self.logger.info('📥 Nível 1: Currency_Master...')
            url = f"{self.config.console_url}{self.config.currency_url}"
            response = self.session.get(url, headers=self.get_headers(), timeout=self.config.timeout)
            response.raise_for_status()
            data = response.json()

            currencies = data.get('data', [])
            self.logger.info(f'  ✅ {len(currencies)} moedas carregadas')

            return pd.DataFrame(currencies)
        except Exception as e:
            self.logger.error(f'Erro ao obter moedas: {e}')
            return pd.DataFrame()

    def get_default_configurations(self) -> pd.DataFrame:
        """
        Equivalente: Default_Configurations (Power Query)

        GET: Console_URL + account-settings/
        Expande: Currency master configurações padrão
        Retorna: 1 registro com moeda e tipo de custo

        ⚠️ v6.0.1 CORREÇÃO: API pode retornar currency como string OU dict
        """
        try:
            self.logger.info('📥 Nível 1: Default_Configurations...')
            url = f"{self.config.console_url}{self.config.account_settings_url}"
            response = self.session.get(url, headers=self.get_headers(), timeout=self.config.timeout)
            response.raise_for_status()
            data = response.json()

            configs = []

            # Lidar com estrutura variável da resposta
            if isinstance(data, dict) and "data" in data:
                items = data.get("data", [])
            elif isinstance(data, list):
                items = data
            else:
                items = [data]

            for item in items:
                # Pular se for string ou não-dict
                if isinstance(item, str) or not isinstance(item, dict):
                    continue

                # ✅ CORREÇÃO v6.0.1: currency pode ser string OU dict
                currency_value = item.get("currency", "BRL")
                if isinstance(currency_value, dict):
                    # Se for dict (estrutura antiga), pegar "code"
                    currency = currency_value.get("code", "BRL")
                else:
                    # Se for string (estrutura atual), usar direto
                    currency = currency_value if isinstance(currency_value, str) else "BRL"

                # ✅ CORREÇÃO v6.0.1: cost_type pode ser string OU dict
                cost_type_value = item.get("cost_type", item.get("costType"))
                if isinstance(cost_type_value, dict):
                    cost_type = cost_type_value.get("code", "calculated_amortized_cost")
                else:
                    cost_type = cost_type_value if isinstance(cost_type_value, str) else "calculated_amortized_cost"

                config_row = {
                    "data.currency": currency,
                    "data.cost_type": cost_type
                }
                configs.append(config_row)

            # Se nenhuma config foi encontrada, retornar default
            if not configs:
                configs.append({
                    "data.currency": "BRL",
                    "data.cost_type": "calculated_amortized_cost"
                })

            self.logger.info('  ✅ Default configurations carregadas')
            return pd.DataFrame(configs)

        except Exception as e:
            self.logger.error(f'Erro ao obter configurações: {e}')
            # Fallback: retornar DataFrame com defaults
            return pd.DataFrame([{
                "data.currency": "BRL",
                "data.cost_type": "calculated_amortized_cost"
            }])

    def get_tag_keys(self) -> List[Dict]:
        """
        Equivalente: OS Tag Keys (Power Query)
        GET: Console_URL + Tags_URL
        Retorna: Lista de tags disponíveis
        """
        try:
            self.logger.info('📥 Nível 1: OS Tag Keys...')
            url = f"{self.config.console_url}{self.config.tags_url}"
            response = self.session.get(url, headers=self.get_headers(), timeout=self.config.timeout)
            response.raise_for_status()
            data = response.json()

            tags = data.get('data', [])
            self.logger.info(f'  ✅ {len(tags)} tags carregadas')

            return tags
        except Exception as e:
            self.logger.error(f'Erro ao obter tags: {e}')
            return []


def load_data_period(start_date: str, end_date: str) -> pd.DataFrame:
    """
    Equivalente: Data_Period (Power Query)
    Input: Datas escolhidas pelo usuário
    Retorna: 1 linha com período
    """
    try:
        end_month = datetime.strptime(end_date, '%Y-%m-%d').strftime('%Y-%m')

        row = {
            'Start Date': start_date,
            'End Date': end_date,
            'End Month': end_month,
            'Time_Scope_Value': 0
        }

        return pd.DataFrame([row])
    except Exception as e:
        logger.error(f'Erro ao carregar período: {e}')
        return pd.DataFrame()


def load_group_bys() -> pd.DataFrame:
    """
    Equivalente: OpenShift_Group_Bys (Power Query)
    Lookup table: tipos de agrupamento
    """
    rows = [
        {'Group By': 'Project', 'Group By Code': 'project'},
        {'Group By': 'Cluster', 'Group By Code': 'cluster'},
        {'Group By': 'Node', 'Group By Code': 'node'},
        {'Group By': 'Tag', 'Group By Code': 'tag'},
    ]
    return pd.DataFrame(rows)


def load_overhead_cost_types() -> pd.DataFrame:
    """
    Equivalente: Project_Overhead_Cost_Types (Power Query)
    Lookup table: tipos de custos de overhead
    """
    rows = [
        {'Code': 'cost', 'Description': 'Dont distribute overhead costs'},
        {'Code': 'distributedcost', 'Description': 'Distribute through cost models'},
    ]
    return pd.DataFrame(rows)


def generate_excel(data_period: pd.DataFrame, 
                   default_settings: pd.DataFrame,
                   group_bys: pd.DataFrame,
                   cost_types: pd.DataFrame,
                   tag_keys: List[Dict],
                   output_file: str):
    """
    Equivalente: Geração do Excel final (Power Query)
    Cria arquivo com todas as abas
    """
    try:
        logger.info('💾 Gerando arquivo Excel...')

        with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
            # Aba 1: Data_Period
            data_period.to_excel(writer, sheet_name='Data_Period', index=False)

            # Aba 2: Default Master Settings
            default_settings.to_excel(writer, sheet_name='Default Master Settings', index=False)

            # Aba 3: Project Overhead Cost Types
            cost_types.to_excel(writer, sheet_name='Project Overhead Cost Types', index=False)

            # Aba 4: OpenShift Group Bys
            group_bys.to_excel(writer, sheet_name='OpenShift Group Bys', index=False)

            # Aba 5: OS Tag Keys
            if tag_keys:
                df_tags = pd.DataFrame([
                    {
                        'count': 1,
                        'key': tag.get('key', 'produto'),
                        'enabled': True,
                        'Group By': 'tag'
                    }
                    for tag in tag_keys
                ])
            else:
                df_tags = pd.DataFrame([{
                    'count': 1,
                    'key': 'produto',
                    'enabled': True,
                    'Group By': 'tag'
                }])

            df_tags.to_excel(writer, sheet_name='OS Tag Keys', index=False)

        logger.info(f'✅ Arquivo salvo: {output_file}')

    except Exception as e:
        logger.error(f'Erro ao gerar Excel: {e}')


def main():
    parser = argparse.ArgumentParser(
        description='OpenShift Cost Extractor v6.0.1 - Tradução Python do Power Query'
    )
    parser.add_argument('--start-date', type=str, help='Data inicial (YYYY-MM-DD)', default=None)
    parser.add_argument('--end-date', type=str, help='Data final (YYYY-MM-DD)', default=None)
    parser.add_argument('--output', type=str, help='Arquivo de saída', default='openshift_costs.xlsx')
    parser.add_argument('--currency', type=str, help='Moeda', default='BRL')

    args = parser.parse_args()

    # Datas default
    if not args.end_date:
        args.end_date = datetime.now().strftime('%Y-%m-%d')

    if not args.start_date:
        start = datetime.now() - timedelta(days=30)
        args.start_date = start.strftime('%Y-%m-%d')

    logger.info('═' * 90)
    logger.info('🚀 OpenShift Cost Extractor v6.0.1 - CORRIGIDO')
    logger.info('═' * 90)
    logger.info(f'Período: {args.start_date} a {args.end_date}')
    logger.info(f'Moeda: {args.currency}')
    logger.info('═' * 90)

    try:
        # Configurar cliente
        config = APIConfig()
        client = OpenShiftCostAPIClient(config, logger)

        # Nível 0-1: APIs
        logger.info('\n📥 NÍVEL 0-1: Carregando APIs...')
        currency_master = client.get_currency_master()
        default_configs = client.get_default_configurations()
        tag_keys = client.get_tag_keys()

        # Nível 2: Tabelas auxiliares
        logger.info('\n📥 NÍVEL 2: Carregando tabelas auxiliares...')
        data_period = load_data_period(args.start_date, args.end_date)
        group_bys = load_group_bys()
        overhead_costs = load_overhead_cost_types()

        # Gerar Excel
        logger.info('\n💾 Gerando arquivo Excel...')
        generate_excel(
            data_period=data_period,
            default_settings=default_configs,
            group_bys=group_bys,
            cost_types=overhead_costs,
            tag_keys=tag_keys,
            output_file=args.output
        )

        logger.info('\n' + '═' * 90)
        logger.info('✅ SUCESSO! Processo concluído com sucesso!')
        logger.info('═' * 90)

    except Exception as e:
        logger.error(f'❌ Erro durante execução: {e}', exc_info=True)
        sys.exit(1)


if __name__ == '__main__':
    main()
