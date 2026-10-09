"""
Test that all DAGs can be imported without errors.

This test recursively imports all .py files from dags/ directory
and fails if any DAG has import errors or syntax issues.
"""

import importlib.util
import sys
from pathlib import Path

import pytest


def get_dag_files() -> list[Path]:
    """Get all Python files from dags/ directory."""
    dags_dir = Path(__file__).parent.parent / "dags"
    return list(dags_dir.rglob("*.py"))


def import_module_from_path(path: Path) -> None:
    """Import a Python module from file path."""
    module_name = path.stem
    spec = importlib.util.spec_from_file_location(module_name, path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Cannot load spec for {path}")
    
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)


class TestDagImports:
    """Test suite for DAG imports."""

    def test_all_dag_files_exist(self) -> None:
        """Verify that DAG files exist."""
        dag_files = get_dag_files()
        assert len(dag_files) > 0, "No DAG files found in dags/ directory"

    @pytest.mark.parametrize("dag_file", get_dag_files(), ids=lambda p: str(p.name))
    def test_dag_imports(self, dag_file: Path) -> None:
        """Test that each DAG file can be imported without errors."""
        # Skip __init__.py files
        if dag_file.name == "__init__.py":
            pytest.skip("Skipping __init__.py")
        
        # Add dags directory to path for imports to work
        dags_dir = Path(__file__).parent.parent / "dags"
        if str(dags_dir) not in sys.path:
            sys.path.insert(0, str(dags_dir))
        
        try:
            import_module_from_path(dag_file)
        except Exception as e:
            pytest.fail(f"Failed to import {dag_file}: {e}")


@pytest.fixture(scope="module")
def dagbag():
    from airflow.models import DagBag

    dags_dir = Path(__file__).parent.parent / "dags"
    return DagBag(dag_folder=str(dags_dir), include_examples=False)


def test_dagbag_has_no_import_errors(dagbag) -> None:
    """Airflow сам парсит папку dags/ так же, как scheduler на сервере."""
    assert not dagbag.import_errors, dagbag.import_errors
    assert len(dagbag.dags) > 0


def test_master_children_exist(dagbag) -> None:
    """Каждый DAG, который запускает мастер, должен существовать."""
    assert "okx_master_raw_to_core_daily" in dagbag.dags

    from okx.pipelines.okx_master_raw_to_core_daily import CHILD_DAGS_IN_ORDER

    missing = [dag_id for dag_id in CHILD_DAGS_IN_ORDER if dag_id not in dagbag.dags]
    assert not missing, f"Мастер ссылается на несуществующие DAG: {missing}"


