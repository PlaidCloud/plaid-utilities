"""What the remote dimension client puts on the wire, against a recording stand-in for the RPC surface."""
from types import SimpleNamespace

import pytest

from plaidcloud.utilities.remote.dimension import MAIN, Dimension, Dimensions

DUID = 'a3f1c2d4-5e6f-4a7b-8c9d-0e1f2a3b4c5d'


class _Rpc:
    """Records every call as (method, kwargs); answers each with what the server would."""

    def __init__(self):
        self.calls = []

    def __getattr__(self, method):
        def call(**kwargs):
            self.calls.append((method, kwargs))
            return {'lookup_by_name': DUID, 'dimension': {'name': 'periods'}}.get(method, ['Year'])
        return call


def _conn():
    rpc = _Rpc()
    return SimpleNamespace(project_id='proj', rpc=SimpleNamespace(analyze=SimpleNamespace(dimension=rpc))), rpc


def test_rename_sends_the_id_and_the_new_name():
    conn, rpc = _conn()

    Dimensions(conn).rename_dimension(DUID, 'periods_v2')

    assert rpc.calls == [('rename_dimension', {'project_id': 'proj', 'duid': DUID, 'name': 'periods_v2'})]


@pytest.mark.parametrize('direction', ['right', 'left', 'up', 'down'])
def test_a_shift_sends_one_child_as_a_list_and_returns_its_new_parent(direction):
    conn, rpc = _conn()
    dim = Dimension(conn, 'periods')

    new_parent = getattr(dim, f'shift_node_{direction}')('Q1', 'Jan')

    assert new_parent == 'Year'
    assert rpc.calls[-1] == (
        f'shift_node_{direction}', {'project_id': 'proj', 'name': 'periods', 'parent': 'Q1', 'children': ['Jan'], 'hierarchy': MAIN},
    )


def test_a_call_with_no_wrapper_passes_through_and_returns_the_answer():
    conn, rpc = _conn()
    dim = Dimension(conn, 'periods')

    assert dim.some_new_rpc(node='Q1') == ['Year']
    assert rpc.calls[-1] == ('some_new_rpc', {'project_id': 'proj', 'name': 'periods', 'node': 'Q1'})


def test_a_private_attribute_is_not_an_rpc():
    conn, _ = _conn()
    dim = Dimension(conn, 'periods')

    with pytest.raises(AttributeError):
        dim._not_a_method


def test_reload_is_gone():
    conn, _ = _conn()

    assert 'reload' not in vars(Dimension)
    assert not hasattr(Dimensions(conn), 'reload')
