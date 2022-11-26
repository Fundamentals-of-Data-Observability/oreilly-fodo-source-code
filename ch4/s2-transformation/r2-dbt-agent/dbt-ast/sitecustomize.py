# Injecting code in DBT to generate data observations naturally

import sys
from ast import *
from importlib.abc import MetaPathFinder, Loader
from importlib.machinery import ModuleSpec
from pathlib import Path

class DBTKensuAgentPathFinder(MetaPathFinder):

  def find_spec(self, fullname, path, target=None):
    if path is None:
      return None
    if fullname not in ["dbt.task.run", "dbt.adapters.sql.connections", "dbt.context.providers"]:
      return None
    p = path
    if isinstance(path, list):
      p = path[0]
    elif isinstance(path, str):
      p = path
    elif hasattr(path, "_path"):
      # _NamespacePath : https://github.com/python/cpython/blob/13d44891426e0faf165f974f2e46907ab5b645a9/Lib/importlib/_bootstrap_external.py#L1329
      p = path._path[0]
    # so we provide the current loader => will use exec_module
    return ModuleSpec(fullname, DBTKensuAgentLoader(fullname, p))


class DBTKensuAgentLoader(Loader):
  PSEUDO_FILENAME = '<kensu>'
  def __init__(self, fullname, path) -> None:
    super().__init__()
    self.fullname = fullname
    self.module_name = fullname.split(".")[-1]
    self.path = path
    source = Path(self.path, self.module_name + ".py").read_text()
    tree = parse(source, self.PSEUDO_FILENAME)
    new_tree = DBTKensuTransformer().visit(tree)
    fix_missing_locations(new_tree)
    self.code = compile(new_tree, self.PSEUDO_FILENAME, 'exec')

  def exec_module(self, module):
    module.__file__ = self.PSEUDO_FILENAME
    exec(self.code, module.__dict__)


class DBTKensuTransformer(NodeTransformer):
  CURRENT_ModelRunner = False
  CURRENT_SQLConnectionManager = False
  CURRENT_ProviderContext = False
  def visit_Module(self, node):
    new_node = self.generic_visit(node)
    return new_node

  def visit_ClassDef(self, node):
    if node.name == "ModelRunner":
      DBTKensuTransformer.CURRENT_ModelRunner = True
    else:
      DBTKensuTransformer.CURRENT_ModelRunner = False

    if node.name == "SQLConnectionManager":
      DBTKensuTransformer.CURRENT_SQLConnectionManager = True
    else:
      DBTKensuTransformer.CURRENT_SQLConnectionManager = False

    if node.name == "ProviderContext":
      DBTKensuTransformer.CURRENT_ProviderContext = True
    else:
      DBTKensuTransformer.CURRENT_ProviderContext = False

    new_node = self.generic_visit(node)
    return new_node


  def visit_FunctionDef(self, node):
    new_node = self.generic_visit(node)
    if DBTKensuTransformer.CURRENT_ModelRunner:
      if node.name == "execute":
        # just init here; the actual datasources & lineages will be reported by dbt plugins themselves (bigquery, postgres)
        # add this code:
        # from dbt.task.kensu_reporting import dbt_init_kensu, kensu_report_rules
        # kensu_collector = dbt_init_kensu(context, model)
        # Index = 1 because after `context` is created
        new_node.body.insert(1,
          ImportFrom(
            module='kensu_reporting',
            names=[
                alias(name='dbt_init_kensu', asname=None)
            ],
            level=0,
          )
        )
        new_node.body.insert(2,
          Assign(
              targets=[Name(id='kensu_collector', ctx=Store())],
              value=Call(
                  func=Name(id='dbt_init_kensu', ctx=Load()),
                  args=[
                      Name(id='context', ctx=Load()),
                      Name(id='model', ctx=Load()),
                  ],
                  keywords=[],
              ),
              type_comment=None,
          )
        )
    elif DBTKensuTransformer.CURRENT_SQLConnectionManager:
      if node.name == "add_query":
        # the last statement is `with self.exception_handler(sql):`
        with_last_statement = new_node.body[-1]
        # insert kensu before the return in the `with` statement
        with_last_statement.body.insert(-1,
          ImportFrom(
            module='kensu_reporting',
            names=[
                alias(name='maybe_report_sql', asname=None),
            ],
            level=0,
          )
        )
        with_last_statement.body.insert(-1,
          Expr(
            value=Call(
              func=Name(id='maybe_report_sql', ctx=Load()),
              args=[
                Name(id='self', ctx=Load()),
                Name(id='cursor', ctx=Load()),
                Name(id='sql', ctx=Load()),
                Name(id='bindings', ctx=Load()),
              ],
              keywords=[],
            ),
          ),
        )
    elif DBTKensuTransformer.CURRENT_ProviderContext:
      if node.name == "load_agate_table":
        # intercept return table
        new_node.body.insert(-1, 
          ImportFrom(
            module='kensu_reporting',
            names=[alias(name='intercept_seed_table', asname=None)],
            level=0,
          )
        )
        new_node.body.insert(-1, 
          Expr(
            value=Call(
              func=Name(id='intercept_seed_table', ctx=Load()),
              args=[
                Name(id='table', ctx=Load())
              ],
              keywords=[],
            ),
          )
        )

    return new_node

# ensuring the Path Finder is loaded first to intercept all others
sys.meta_path.insert(0, DBTKensuAgentPathFinder())