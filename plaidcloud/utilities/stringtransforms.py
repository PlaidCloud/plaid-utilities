#!/usr/bin/env python
# coding=utf-8
"""`{variable}` substitution, which lives in plaidcloud-rpc so that a service
substituting variables need not install pandas; re-exported for every existing
`plaidcloud.utilities.stringtransforms` import."""

from plaidcloud.rpc.stringtransforms import VariableSubstitutionError, apply_variables, replaceTags

__all__ = ['VariableSubstitutionError', 'apply_variables', 'replaceTags']
