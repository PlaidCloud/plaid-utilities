# coding=utf-8
"""Tests for plaidcloud.utilities.bom."""

import unittest

import pandas as pd

from plaidcloud.utilities.bom import BOM, Node


class TestNode(unittest.TestCase):

    def test_node_without_parent_has_none_parent(self):
        n = Node(None, 'solo')
        self.assertEqual(n.id, 'solo')
        self.assertEqual(n.get_parents(), [None])
        self.assertEqual(n.get_children(), {})

    def test_add_child_registers_in_parent(self):
        root = Node(None, 'root')
        child = Node(root, 'c', makeup_volume=2)
        self.assertIn(child, root.get_children())
        self.assertEqual(root.get_children()[child], 2)

    def test_remove_child(self):
        root = Node(None, 'root')
        a = Node(root, 'a')
        Node(root, 'b')
        self.assertEqual(len(root.get_children()), 2)
        root.remove_child('a')
        self.assertEqual(len(root.get_children()), 1)
        self.assertNotIn(a, root.get_children())

    def test_is_child_of_and_is_parent_of(self):
        root = Node(None, 'root')
        child = Node(root, 'c')
        self.assertTrue(child.is_child_of('root'))
        self.assertFalse(child.is_child_of('missing'))
        self.assertTrue(root.is_parent_of('c'))
        self.assertFalse(root.is_parent_of('missing'))

    def test_set_cost_and_get_cost_leaf(self):
        n = Node(None, 'leaf')
        n.set_cost(10)
        self.assertEqual(n.get_cost(), 10)

    def test_calculate_cost_rolls_up_children(self):
        root = Node(None, 'root')
        left = Node(root, 'left', makeup_volume=2)
        right = Node(root, 'right', makeup_volume=3)
        left.set_cost(5)
        right.set_cost(4)
        # (5 * 2) + (4 * 3) = 22
        self.assertEqual(root.get_cost(), 22)

    def test_override_cost_wins_over_children(self):
        root = Node(None, 'root')
        c = Node(root, 'c', makeup_volume=1)
        c.set_cost(100)
        root.set_override_cost(0)
        self.assertEqual(root.get_cost(), 0)

    def test_zero_makeup_volume_skips_child(self):
        root = Node(None, 'root')
        c = Node(root, 'c', makeup_volume=0)
        c.set_cost(999)
        self.assertEqual(root.get_cost(), 0)

    def test_reset_cost_forces_recalculation(self):
        root = Node(None, 'root')
        c = Node(root, 'c', makeup_volume=2)
        c.set_cost(5)
        self.assertEqual(root.get_cost(), 10)
        c.set_cost(7)
        # Still cached
        self.assertEqual(root.get_cost(), 10)
        root.reset_cost()
        self.assertEqual(root.get_cost(), 14)

    def test_repr_includes_id(self):
        n = Node(None, 'widget')
        n.set_cost(1)
        self.assertIn('widget', repr(n))

    def test_get_siblings_returns_parent_children(self):
        root = Node(None, 'root')
        a = Node(root, 'a')
        Node(root, 'b')
        siblings = a.get_siblings('root')
        self.assertEqual(len(siblings), 2)

    def test_get_siblings_missing_parent_returns_none(self):
        root = Node(None, 'root')
        a = Node(root, 'a')
        self.assertIsNone(a.get_siblings('nonexistent'))


class TestBOM(unittest.TestCase):

    def setUp(self):
        self.bom = BOM()
        # root -> a, root -> b; a -> a1, a -> a2
        self.bom.add_node('root', 'a', 1)
        self.bom.add_node('root', 'b', 2)
        self.bom.add_node('a', 'a1', 3)
        self.bom.add_node('a', 'a2', 1)

    def test_initial_state_has_root_only(self):
        fresh = BOM()
        self.assertEqual(fresh.get_node_count(), 1)
        self.assertEqual(fresh.get_node('root').id, 'root')

    def test_add_node_count(self):
        # root + a + b + a1 + a2
        self.assertEqual(self.bom.get_node_count(), 5)

    def test_add_node_with_missing_parent_autocreates(self):
        bom = BOM()
        bom.add_node('missing_parent', 'kid', 1)
        self.assertEqual(
            bom.get_parent_ids('kid'),
            ['missing_parent'],
        )
        # The auto-created parent should be under root
        self.assertIn(
            'root',
            bom.get_parent_ids('missing_parent'),
        )

    def test_add_existing_child_registers_under_new_parent(self):
        # "a1" already exists below "a"; re-add under "b". The existing Node
        # instance gets registered as a child of b (though its parents list is
        # only populated on initial creation, so we assert the children edge).
        self.bom.add_node('b', 'a1', 5)
        self.assertIn('a1', self.bom.get_children_ids('b'))

    def test_delete_node_removes_from_parents(self):
        self.bom.delete_node('a1')
        self.assertEqual(
            set(self.bom.get_children_ids('a')),
            {'a2'},
        )
        with self.assertRaises(Exception):
            self.bom.get_node('a1')

    def test_delete_missing_node_is_noop(self):
        # Should not raise
        self.bom.delete_node('does_not_exist')

    def test_get_node_missing_raises(self):
        with self.assertRaises(Exception):
            self.bom.get_node('nope')

    def test_get_parent_ids_missing_returns_none(self):
        self.assertIsNone(self.bom.get_parent_ids('does_not_exist'))

    def test_is_child_of_and_is_parent_of(self):
        self.assertTrue(self.bom.is_child_of('a', 'root'))
        self.assertTrue(self.bom.is_parent_of('a', 'a1'))
        self.assertFalse(self.bom.is_child_of('a', 'b'))

    def test_get_siblings_ids(self):
        siblings = set(self.bom.get_sibling_ids('a1', 'a'))
        self.assertEqual(siblings, {'a1', 'a2'})

    def test_set_cost_and_get_all_costs_frame(self):
        self.bom.set_cost('a1', 10)
        self.bom.set_cost('a2', 5)
        self.bom.set_cost('b', 7)
        frame = self.bom.get_all_costs()
        self.assertIsInstance(frame, pd.DataFrame)
        self.assertEqual(set(frame.columns), {'node', 'cost'})
        # a = (10 * 3) + (5 * 1) = 35
        costs = dict(zip(frame['node'], frame['cost']))
        self.assertEqual(costs['a'], 35)
        # root = (35 * 1) + (7 * 2) = 49
        self.assertEqual(costs['root'], 49)

    def test_set_override_cost(self):
        self.bom.set_cost('a1', 10)
        self.bom.set_cost('a2', 5)
        self.bom.set_override_cost('a', 1000)
        self.assertEqual(self.bom.get_node('a').get_cost(), 1000)

    def test_reset_costs_clears_all_calculated(self):
        self.bom.set_cost('a1', 10)
        self.bom.set_cost('a2', 5)
        self.bom.set_cost('b', 7)
        # Warm up the cache - a = 10*3 + 5*1 = 35
        self.assertEqual(self.bom.get_node('a').get_cost(), 35)

        # Manually invalidate all caches, then re-set leaf values.
        self.bom.reset_costs()
        self.bom.set_cost('a1', 1)
        self.bom.set_cost('a2', 1)
        self.bom.set_cost('b', 1)
        # a = 1*3 + 1*1 = 4
        self.assertEqual(self.bom.get_node('a').get_cost(), 4)

    def test_get_bom_frame(self):
        df = self.bom.get_bom()
        self.assertIsInstance(df, pd.DataFrame)
        self.assertEqual(
            list(df.columns),
            ['parent', 'child', 'makeup_volume'],
        )
        edges = set(zip(df['parent'], df['child']))
        self.assertIn(('root', 'a'), edges)
        self.assertIn(('a', 'a1'), edges)

    def test_get_frame_includes_left_right_leaf(self):
        df = self.bom.get_frame(table=None)
        self.assertEqual(
            set(df.columns),
            {'parent', 'child', 'makeup_volume',
             'effective_makeup_volume', 'leaf', 'left', 'right', 'indent'},
        )
        # Leaves should be a1, a2, b; non-leaves: a
        leaves = set(df[df['leaf'] == True]['child'])  # noqa: E712
        self.assertIn('a1', leaves)
        self.assertIn('a2', leaves)
        self.assertIn('b', leaves)
        non_leaves = set(df[df['leaf'] == False]['child'])  # noqa: E712
        self.assertIn('a', non_leaves)

    def test_get_frame_effective_makeup_volume_multiplies(self):
        df = self.bom.get_frame(table=None)
        row = df[df['child'] == 'a1'].iloc[0]
        # a1 is under a (mv=1) which is under root (mv=1), and a1 mv=3,
        # so effective = 1 * 3 = 3
        self.assertEqual(row['effective_makeup_volume'], 3)

    def test_get_pretty_frame_columns(self):
        self.bom.set_cost('a1', 1)
        self.bom.set_cost('a2', 1)
        self.bom.set_cost('b', 1)
        df = self.bom.get_pretty_frame(table=None)
        self.assertEqual(
            set(df.columns),
            {'friendly', 'parent', 'child', 'makeup_volume',
             'effective_makeup_volume'},
        )
        self.assertTrue(len(df) > 0)

    def test_load_dataframe_round_trip(self):
        source = pd.DataFrame({
            'parent': ['root', 'root', 'a'],
            'child': ['a', 'b', 'a1'],
            'makeup_volume': [1, 2, 3],
        })
        bom = BOM()
        bom.load_dataframe(source)
        self.assertEqual(bom.get_node_count(), 4)  # root + a + b + a1
        self.assertEqual(
            set(bom.get_children_ids('root')),
            {'a', 'b'},
        )

    def test_load_dataframe_missing_parent_raises(self):
        bad = pd.DataFrame({'child': ['a'], 'makeup_volume': [1]})
        with self.assertRaisesRegex(Exception, 'parent'):
            BOM().load_dataframe(bad)

    def test_load_dataframe_missing_child_raises(self):
        bad = pd.DataFrame({'parent': ['root'], 'makeup_volume': [1]})
        with self.assertRaisesRegex(Exception, 'child'):
            BOM().load_dataframe(bad)

    def test_load_dataframe_missing_makeup_volume_raises(self):
        bad = pd.DataFrame({'parent': ['root'], 'child': ['a']})
        with self.assertRaisesRegex(Exception, 'makeup_volume'):
            BOM().load_dataframe(bad)

    def test_constructor_with_load_path_delegates_to_load(self):
        # `load` is unimplemented, so constructing with load_path should raise
        # NotImplementedError once load is invoked.
        with self.assertRaises(NotImplementedError):
            BOM(load_path='/any')

    def test_load_dataframe_none_is_noop(self):
        bom = BOM()
        bom.load_dataframe(None)
        # Just root still
        self.assertEqual(bom.get_node_count(), 1)

    def test_clear_resets_hierarchy(self):
        self.bom.clear()
        self.assertEqual(self.bom.get_node_count(), 1)

    def test_save_and_load_not_implemented(self):
        # save delegates to save_hierarchy which is abstract
        with self.assertRaises(NotImplementedError):
            self.bom.save('/tmp/ignored')
        with self.assertRaises(NotImplementedError):
            self.bom.load('/tmp/ignored')


if __name__ == '__main__':
    unittest.main()
