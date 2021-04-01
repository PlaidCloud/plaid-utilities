3#!/usr/bin/env python
# coding=utf-8
"""Loads the configuration file used by plaid apps in kubernetes."""
import os
import yaml
from typing import NamedTuple

CONFIG_PATH = os.env.get('PLAID_CONFIG_PATH', '/etc/plaidcloud/config.yaml')


class DatabaseConfig(NamedTuple):
    hostname: str


class EnvironmentConfig(NamedTuple):
    hostname: str = "plaidcloud.io"
    client_id: str
    designation: str = "dev"
    tempdir: str = "/tmp"
    verify_ssl: bool = False


class FeatureConfig(NamedTuple):
    async_copy: bool = True
    backward_compatible_state: bool = True
    decrypted_accounts: bool = True
    enable_cors: bool = False
    fast_clean_csv: bool = True
    flashback: bool = True
    google_login: bool = True
    table_update_recreate: bool = True
    use_numeric_cast: bool = True


class RMQConfig(NamedTuple):
    """Connection settings for a RabbitMQ instance."""
    hostname: str = "rabbit-rabbitmq-ha"
    port: int = 5679
    username: str
    password: str
    vhost: str


class RedisConfig(NamedTuple):
    """Settings for Redis client connections."""
    timeout: int = 1


class RedisURLConfig(NamedTuple):
    """URLs for Redis connections."""
    activity: str = "redis://redis-master/4"
    analyze_cache: str = "redis://redis-master/6"
    cron_jobs: str = "redis://redis-master/1"
    cron_running_jobs: str = "redis://redis-master/2"
    data_connection_cache: str = "redis://redis-master/11"
    document_cache: str = "redis://redis-master/7"
    hierarchy_cache: str = "redis://redis-master/10"
    identity_cache: str = "redis://redis-master/8"
    ipython_registry: str = "redis://redis-master/3"
    oauth_cache: str = "redis://redis-master/9"
    redis_scheduler: str = "redis://redis-master/13"
    scopes_cache: str = "redis://redis-master/12"
    session: str = "redis://redis-master/0"
    transform_container_registry: str = "redis://redis-master/5"


class ServiceConfig(NamedTuple):
    auth: str = "http://plaid-auth.plaid"
    client: str = "http://plaid-client.plaid"
    cron: str = "http://plaid-cron.plaid"
    data_explorer: str = "http://plaid-data-explorer.plaid"
    docs: str = "http://plaid-docs.plaid"
    flashback: str = "http://plaid-flashback.plaid/rpc"
    monitor: str = "http://plaid-monitor.plaid"
    plaidxl: str = "http://plaid-plaidxl.plaid"
    rpc: str = "http://plaid-rpc.plaid/json_rpc"
    superset: str = "http://plaid-superset.plaid"
    workflow: str = "http://plaid-workflow.plaid"


class PlaidConfig:
    """Parses a standard configuration file for consumption by python code."""
    def __init__(self):
        with open(CONFIG_PATH, 'r') as stream:
            # Leave exception unhandled. We don't want to start without a valid conf.
            self.cfg = yaml.safe_load(stream)

    @property
    def log_level(self):
        """Default log level for workflow-monitor."""
        return self.cfg.get('logLevel', 'INFO')

    @property
    def database(self) -> DatabaseConfig:
        db_config = self.cfg.get('database', {})
        return DatabaseConfig(**db_config)

    @property
    def environment(self) -> EnvironmentConfig:
        env_config = self.cfg.get('environment', {})
        return EnvironmentConfig(**env_config)

    @property
    def features(self) -> FeatureConfig:
        feature_config = self.cfg.get('features', {})
        return FeatureConfig(**feature_config)

    @property
    def kubernetes(self):
        """Configuration settings for kube-apiserver monitor."""
        k8s_config = self.cfg.get('kubernetes', {})
        return KubernetesConfig(**k8s_config)

    @property
    def rabbitmq(self) -> RMQConfig:
        """Configuration settings for RabbitMQ connection."""
        rmq_config = self.cfg.get('rabbitmq', {})
        return RMQConfig(**rmq_config)

    def redis_client(self) -> RedisConfig:
        """Settings for Redis client connections."""
        redis_config = self.cfg.get('redisClient', {})
        return RedisConfig(**redis_config)

    @property
    def redis_urls(self) -> RedisURLConfig:
        """URLs for Redis connections."""
        redis_config = self.cfg.get('redis', {})
        return RedisURLConfig(**redis_config)

    @property
    def service_urls(self) -> ServiceConfig:
        svc_config = self.cfg.get('services', {})
        return ServiceConfig(**svc_config)

    def __str__(self):
        return repr(self)

config = PlaidConfig()  # pylint: disable=invalid-name
