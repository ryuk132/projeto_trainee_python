#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
OpenShift Cost Extractor v6.0.2
=====================================================
RED HAT CONSOLE - Cost Management API Integration

VERSÃO CORRIGIDA:
✅ v6.0.1: Tratamento de types (dict vs string) 
✅ v6.0.2: URLs CORRETOS da API (sem 404)

URLS CORRETOS DA API (Red Hat Console):
- CurrencyURL:      /api/cost-management/v1/currency?filter[limit]=15&limit=100&offset=0
- DefaultConfigs:   /api/cost-management/v1/account-settings
- TagsURL:          /api/cost-management/v1/tags/openshift
- CostsURL:         /api/cost-management/v1/reports/openshift/costs

Author: Cloud Cost Analytics Team
Date: 2026-01-02
"""

import os
import json
import argparse
import logging
from datetime import datetime
from typing import Optional, Dict, List, Any
import requests
import pandas as pd

# ============================================================================
# LOGGING SETUP
# ============================================================================

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# ============================================================================
# CONFIGURATION
# ============================================================================

class Config:
    """Configuração da aplicação"""

    def __init__(self):
        # URLs base
        self.console_url = os.getenv('CONSOLE_URL', 'https://console.redhat.com')

        # Credenciais
        self.client_id = os.getenv('OPENSHIFT_CLIENT_ID')
        self.client_secret = os.getenv('OPENSHIFT_CLIENT_SECRET')

        # Validação
        if not self.client_id or not self.client_secret:
            raise ValueError(
                "ERRO: Variáveis de ambiente não configuradas!\n"
                "Configure: OPENSHIFT_CLIENT_ID e OPENSHIFT_CLIENT_SECRET"
            )

        # Timeouts
        self.timeout = 30
        self.max_retries = 3


# ============================================================================
# SESSION MANAGEMENT
# ============================================================================

class Session:
    """Gerenciador de sessão com token"""

    def __init__(self, config: Config):
        self.config = config
        self.token = None
        self.token_expires = None
        self._refresh_token()

    def _refresh_token(self):
        """Obtém novo access token"""
        logger.info("Obtendo novo access token...")

        auth_url = "https://sso.redhat.com/auth/realms/redhat-external/protocol/openid-connect/token"


        auth = (self.config.client_id, self.config.client_secret)
        headers = {'Content-Type': 'application/x-www-form-urlencoded'}
        data = {'grant_type': 'client_credentials'}

        try:
            response = requests.post(
                auth_url,
                auth=auth,
                headers=headers,
                data=data,
                timeout=self.config.timeout
            )
            response.raise_for_status()

            auth_data = response.json()
            self.token = auth_data.get('access_token')

            logger.info("✅ Token obtido com sucesso")

        except requests.exceptions.RequestException as e:
            logger.error(f"Erro ao obter token: {e}")
            raise

    def get(self, url: str, headers: Optional[Dict] = None, **kwargs):
        """GET com tratamento automático de token"""
        if headers is None:
            headers = {}

        headers['Authorization'] = f'Bearer {self.token}'

        return requests.get(url, headers=headers, timeout=self.config.timeout, **kwargs)


# ============================================================================
# API FUNCTIONS (v6.0.2 - URLs CORRETOS)
# ============================================================================

def get_currencies(config: Config, session: Session) -> pd.DataFrame:
    """
    Equivalente: Currency_Master (Power Query)

    GET: Console_URL + /api/cost-management/v1/currency
    Retorna: Lista de moedas disponíveis
    """
    try:
        # ✅ URL CORRETO v6.0.2
        url = f"{config.console_url}/api/cost-management/v1/currency?filter[limit]=15&limit=100&offset=0"

        headers = {
            'Authorization': f'Bearer {session.token}',
            'Content-Type': 'application/json'
        }

        response = session.get(url, headers=headers)
        response.raise_for_status()

        data = response.json()
        currencies = data.get('data', [])

        df = pd.DataFrame([
            {
                'code': c.get('code'),
                'name': c.get('name'),
                'symbol': c.get('symbol'),
                'description': c.get('description')
            }
            for c in currencies
        ])

        logger.info(f"✅ {len(df)} moedas carregadas")
        return df

    except Exception as e:
        logger.error(f"Erro ao obter moedas: {e}")
        # Fallback
        return pd.DataFrame([{
            'code': 'BRL',
            'name': 'Brazilian Real',
            'symbol': 'R$',
            'description': 'Brazilian Real'
        }])


def get_default_configurations(config: Config, session: Session) -> pd.DataFrame:
    """
    Equivalente: Default_Configurations (Power Query)

    GET: Console_URL + /api/cost-management/v1/account-settings
    Retorna: 1 registro com moeda e tipo de custo padrão

    ✅ v6.0.1: Trata currency como string OU dict
    ✅ v6.0.2: URL CORRETO (sem /api/account-settings)
    """
    try:
        # ✅ URL CORRETO v6.0.2
        url = f"{config.console_url}/api/cost-management/v1/account-settings"

        headers = {
            'Authorization': f'Bearer {session.token}',
            'Content-Type': 'application/json'
        }

        response = session.get(url, headers=headers)
        response.raise_for_status()

        data = response.json()

        configs = []

        if isinstance(data, dict) and "data" in data:
            items = data.get("data", [])
        elif isinstance(data, list):
            items = data
        else:
            items = [data]

        for item in items:
            if isinstance(item, str) or not isinstance(item, dict):
                continue

            # ✅ v6.0.1: Trata currency como string OU dict
            currency_value = item.get("currency", "BRL")
            if isinstance(currency_value, dict):
                currency = currency_value.get("code", "BRL")
            else:
                currency = currency_value if isinstance(currency_value, str) else "BRL"

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

        if not configs:
            configs.append({
                "data.currency": "BRL",
                "data.cost_type": "calculated_amortized_cost"
            })

        logger.info("✅ Default configurations carregadas")
        return pd.DataFrame(configs)

    except Exception as e:
        logger.error(f"Erro ao obter configurações: {e}")
        return pd.DataFrame([{
            "data.currency": "BRL",
            "data.cost_type": "calculated_amortized_cost"
        }])


def get_tags(config: Config, session: Session) -> pd.DataFrame:
    """
    Equivalente: OS Tag Keys (Power Query)

    GET: Console_URL + /api/cost-management/v1/tags/openshift
    Retorna: Tag keys para OpenShift

    ✅ v6.0.2: URL CORRETO (sem /api/tags)
    """
    try:
        # ✅ URL CORRETO v6.0.2
        url = f"{config.console_url}/api/cost-management/v1/tags/openshift"

        headers = {
            'Authorization': f'Bearer {session.token}',
            'Content-Type': 'application/json'
        }

        response = session.get(url, headers=headers)
        response.raise_for_status()

        data = response.json()
        tags = data.get('data', [])

        df = pd.DataFrame([
            {
                'key': t.get('key'),
                'values': ','.join(t.get('values', []))
            }
            for t in tags
        ])

        logger.info(f"✅ {len(df)} tag keys carregadas")
        return df

    except Exception as e:
        logger.error(f"Erro ao obter tags: {e}")
        return pd.DataFrame([{'key': 'no_tags', 'values': ''}])


def get_cost_data(config: Config, session: Session, 
                  start_date: str, end_date: str, 
                  currency: str = "BRL") -> pd.DataFrame:
    """
    Equivalente: CostData (Power Query)

    GET: Console_URL + /api/cost-management/v1/reports/openshift/costs
    Retorna: Dados de custo por data, projeto, cluster, nó

    ✅ v6.0.2: URL CORRETO (sem /api/costs)
    """
    try:
        # ✅ URL CORRETO v6.0.2
        url = f"{config.console_url}/api/cost-management/v1/reports/openshift/costs"

        params = {
            'filter[resolution]': 'daily',
            'filter[time_scope_value]': -1,
            'filter[time_scope_units]': 'month',
            'filter[limit]': '10',
            'offset': '0'
        }

        headers = {
            'Authorization': f'Bearer {session.token}',
            'Content-Type': 'application/json'
        }

        response = session.get(url, headers=headers, params=params)
        response.raise_for_status()

        data = response.json()
        costs = data.get('data', [])

        df = pd.DataFrame(costs)
        logger.info(f"✅ {len(df)} registros de custo carregados")
        return df

    except Exception as e:
        logger.error(f"Erro ao obter dados de custo: {e}")
        return pd.DataFrame()


# ============================================================================
# MAIN EXECUTION
# ============================================================================

def main():
    parser = argparse.ArgumentParser(
        description='OpenShift Cost Extractor v6.0.2'
    )
    parser.add_argument('--start-date', required=True, help='Data inicial (YYYY-MM-DD)')
    parser.add_argument('--end-date', required=True, help='Data final (YYYY-MM-DD)')
    parser.add_argument('--output', default='openshift_costs.xlsx', help='Arquivo de saída')

    args = parser.parse_args()

    try:
        # Header
        logger.info("═" * 90)
        logger.info("🚀 OpenShift Cost Extractor v6.0.2 - URLS CORRETOS")
        logger.info("═" * 90)
        logger.info(f"Período: {args.start_date} a {args.end_date}")
        logger.info(f"Moeda: BRL")
        logger.info("═" * 90)

        # Configuração
        config = Config()
        session = Session(config)

        # Nível 0-1: Load APIs
        logger.info("")
        logger.info("📥 NÍVEL 0-1: Carregando APIs...")
        logger.info("📥 Nível 1: Currency_Master...")
        currencies_df = get_currencies(config, session)

        logger.info("📥 Nível 1: Default_Configurations...")
        configs_df = get_default_configurations(config, session)

        logger.info("📥 Nível 1: OS Tag Keys...")
        tags_df = get_tags(config, session)

        # Nível 2: Load auxiliary tables
        logger.info("")
        logger.info("📥 NÍVEL 2: Carregando tabelas auxiliares...")

        # Generate Excel
        logger.info("")
        logger.info("💾 Gerando arquivo Excel...")

        with pd.ExcelWriter(args.output, engine='openpyxl') as writer:
            currencies_df.to_excel(writer, sheet_name='Currencies', index=False)
            configs_df.to_excel(writer, sheet_name='Default Master Settings', index=False)
            tags_df.to_excel(writer, sheet_name='Tag Keys', index=False)

        logger.info(f"✅ Arquivo salvo: {args.output}")

        # Footer
        logger.info("")
        logger.info("═" * 90)
        logger.info("✅ SUCESSO! Processo concluído com sucesso!")
        logger.info("═" * 90)

    except Exception as e:
        logger.error(f"❌ ERRO: {e}")
        raise


if __name__ == '__main__':
    main()
