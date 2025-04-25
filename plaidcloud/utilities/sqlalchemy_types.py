from sqlalchemy.ext.compiler import  compiles
from sqlalchemy.types import NUMERIC

@compiles(NUMERIC, 'snowflake')
def compile_numeric_snowflake(type_, compiler, **kw):
    return "NUMERIC(38, 10)"
