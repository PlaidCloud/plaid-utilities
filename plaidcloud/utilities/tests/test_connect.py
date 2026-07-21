#!/usr/bin/env python
# coding=utf-8
"""PlaidConnection used to swallow every argument — it accepted *args/**kwargs and then
called Connect.__init__(self) with none of them. These cover what it forwards."""

from unittest import mock

import pytest

from plaidcloud.utilities.connect import PlaidConnection


@pytest.fixture
def captured_connect_kwargs(monkeypatch):
    """Stand in for Connect.__init__, recording what PlaidConnection passed it and setting
    the attributes the rest of PlaidConnection.__init__ reads."""
    captured = {}

    def fake_connect_init(self, *args, **kwargs):
        captured.update(kwargs)
        self.is_local = False
        self.debug = False
        self.hostname = 'test.plaid.cloud'
        self._project_id = kwargs.get('project_id', '')
        self._workflow_id = ''

    monkeypatch.setattr('plaidcloud.utilities.connect.Connect.__init__', fake_connect_init)
    monkeypatch.setattr('plaidcloud.utilities.connect.Connection.__init__',
                        lambda self, rpc=None: None)
    monkeypatch.setattr('plaidcloud.utilities.connect.Logger', lambda rpc=None: mock.MagicMock())
    return captured


class TestPlaidConnectionForwarding:

    def test_direct_config_reaches_connect(self, captured_connect_kwargs, monkeypatch):
        monkeypatch.delenv('__PLAID_JUPYTER__', raising=False)
        PlaidConnection(
            rpc_uri='https://t.plaid.cloud/json-rpc/', auth_token='tok',
            workspace_uuid='ws', project_id='pid',
        )
        assert captured_connect_kwargs == {
            'rpc_uri': 'https://t.plaid.cloud/json-rpc/', 'auth_token': 'tok',
            'workspace_uuid': 'ws', 'project_id': 'pid',
        }

    def test_token_provider_reaches_connect(self, captured_connect_kwargs, monkeypatch):
        monkeypatch.delenv('__PLAID_JUPYTER__', raising=False)

        def provider():
            return 'per-session'

        PlaidConnection(rpc_uri='https://t.plaid.cloud/json-rpc/', token_provider=provider)
        assert captured_connect_kwargs['token_provider'] is provider

    def test_bare_connection_forwards_nothing(self, captured_connect_kwargs, monkeypatch):
        """The generated UDF boilerplate calls PlaidConnection() with no arguments; it must
        still take the environment/plaid.conf path."""
        monkeypatch.delenv('__PLAID_JUPYTER__', raising=False)
        PlaidConnection()
        assert captured_connect_kwargs == {}

    def test_project_id_alone_is_not_forwarded(self, captured_connect_kwargs, monkeypatch):
        """Without rpc_uri another source supplies the project, and Connect rejects a bare
        project_id — so it must not be passed through."""
        monkeypatch.delenv('__PLAID_JUPYTER__', raising=False)
        PlaidConnection(project_id='pid')
        assert captured_connect_kwargs == {}

    def test_xl_path_is_not_forwarded(self, captured_connect_kwargs, monkeypatch):
        """xl_path is consumed by PlaidConnection itself; forwarding it would TypeError."""
        monkeypatch.delenv('__PLAID_JUPYTER__', raising=False)
        PlaidConnection(rpc_uri='https://t.plaid.cloud/json-rpc/', auth_token='tok',
                        xl_path='book.xlsm')
        assert 'xl_path' not in captured_connect_kwargs

    def test_jupyter_path_still_seeds_environment(self, captured_connect_kwargs, monkeypatch):
        monkeypatch.setenv('__PLAID_JUPYTER__', 'True')
        monkeypatch.setenv('KEYCLOAK_ACCESS_TOKEN', 'kc-token')
        PlaidConnection(project_id='pid')
        import os
        assert os.environ['__PLAID_PROJECT_ID__'] == 'pid'
        assert os.environ['__PLAID_RPC_AUTH_TOKEN__'] == 'kc-token'
        assert captured_connect_kwargs == {}

    def test_jupyter_without_project_id_raises(self, captured_connect_kwargs, monkeypatch):
        monkeypatch.setenv('__PLAID_JUPYTER__', 'True')
        with pytest.raises(Exception, match='Set the Project ID'):
            PlaidConnection()
