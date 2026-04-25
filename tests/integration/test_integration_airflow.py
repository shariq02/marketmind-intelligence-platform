# MarketMind Intelligence Platform V1
# Airflow DAG Integration Tests - CORRECTED
# Date: April 24, 2026

import pytest
from pathlib import Path
from airflow.models import DagBag


@pytest.mark.integration
@pytest.mark.airflow
class TestDAGIntegrity:
    """Test DAG file integrity and loading"""
    
    @pytest.fixture
    def dag_bag(self):
        """Load DAG bag"""
        return DagBag(
            dag_folder='airflow/dags',
            include_examples=False
        )
    
    def test_dag_bag_import(self, dag_bag):
        """Test all DAG files can be imported without errors"""
        assert len(dag_bag.import_errors) == 0, \
            f"DAG import errors: {dag_bag.import_errors}"
        
        assert len(dag_bag.dags) > 0, "No DAGs found"
        
        print(f"Successfully loaded {len(dag_bag.dags)} DAGs")
    
    def test_no_dag_loading_errors(self, dag_bag):
        """Test DAGs load without errors"""
        for dag_id, dag in dag_bag.dags.items():
            assert dag is not None, f"DAG {dag_id} is None"
            assert len(dag.tasks) > 0, f"DAG {dag_id} has no tasks"


@pytest.mark.integration
@pytest.mark.airflow
class TestDailyMarketDataDAG:
    """Test daily_market_data DAG"""
    
    @pytest.fixture
    def dag_bag(self):
        return DagBag(dag_folder='airflow/dags', include_examples=False)
    
    def test_dag_exists(self, dag_bag):
        """Test daily_market_data DAG exists"""
        assert 'daily_market_data' in dag_bag.dags, \
            f"daily_market_data DAG not found. Available: {list(dag_bag.dags.keys())}"
    
    def test_dag_has_required_tasks(self, dag_bag):
        """Test DAG has required tasks"""
        dag = dag_bag.dags['daily_market_data']
        
        required_tasks = [
            'start',
            'check_market_open',
            'fetch_data',
            'bronze_writer',
            'silver_transformer',
            'gold_loader',
            'record_audit',
            'end'
        ]
        
        task_ids = [task.task_id for task in dag.tasks]
        
        for required_task in required_tasks:
            assert required_task in task_ids, \
                f"Required task '{required_task}' not found in DAG"
    
    def test_dag_schedule_interval(self, dag_bag):
        """Test DAG schedule interval"""
        dag = dag_bag.dags['daily_market_data']
        
        assert dag.schedule_interval is not None, \
            "DAG schedule_interval should be configured"
        
        # Should run weekdays at 6 PM
        assert dag.schedule_interval == '0 18 * * 1-5'


@pytest.mark.integration
@pytest.mark.airflow
class TestWeeklyCorporateActionsDAG:
    """Test weekly_corporate_actions DAG"""
    
    @pytest.fixture
    def dag_bag(self):
        return DagBag(dag_folder='airflow/dags', include_examples=False)
    
    def test_dag_exists(self, dag_bag):
        """Test weekly_corporate_actions DAG exists"""
        assert 'weekly_corporate_actions' in dag_bag.dags, \
            f"weekly_corporate_actions DAG not found. Available: {list(dag_bag.dags.keys())}"
    
    def test_dag_schedule_interval(self, dag_bag):
        """Test DAG runs weekly on Sunday"""
        dag = dag_bag.dags['weekly_corporate_actions']
        
        # Should run Sundays at 2 AM
        assert dag.schedule_interval == '0 2 * * 0'


@pytest.mark.integration
@pytest.mark.airflow
class TestMonthlyMacroIndicatorsDAG:
    """Test monthly_macro_indicators DAG"""
    
    @pytest.fixture
    def dag_bag(self):
        return DagBag(dag_folder='airflow/dags', include_examples=False)
    
    def test_dag_exists(self, dag_bag):
        """Test monthly_macro_indicators DAG exists"""
        assert 'monthly_macro_indicators' in dag_bag.dags, \
            f"monthly_macro_indicators DAG not found. Available: {list(dag_bag.dags.keys())}"
    
    def test_dag_schedule_interval(self, dag_bag):
        """Test DAG runs monthly"""
        dag = dag_bag.dags['monthly_macro_indicators']
        
        # Should run 1st of each month at 3 AM
        assert dag.schedule_interval == '0 3 1 * *'


@pytest.mark.integration
@pytest.mark.airflow
class TestQuarterlySecFilingsDAG:
    """Test quarterly_sec_filings DAG"""
    
    @pytest.fixture
    def dag_bag(self):
        return DagBag(dag_folder='airflow/dags', include_examples=False)
    
    def test_dag_exists(self, dag_bag):
        """Test quarterly_sec_filings DAG exists"""
        assert 'quarterly_sec_filings' in dag_bag.dags, \
            f"quarterly_sec_filings DAG not found. Available: {list(dag_bag.dags.keys())}"
    
    def test_dag_manual_trigger_only(self, dag_bag):
        """Test DAG is manual trigger only"""
        dag = dag_bag.dags['quarterly_sec_filings']
        
        # Manual trigger = schedule_interval is None
        assert dag.schedule_interval is None


@pytest.mark.integration
@pytest.mark.airflow
class TestAdhocFullBackfillDAG:
    """Test adhoc_full_backfill DAG"""
    
    @pytest.fixture
    def dag_bag(self):
        return DagBag(dag_folder='airflow/dags', include_examples=False)
    
    def test_dag_exists(self, dag_bag):
        """Test adhoc_full_backfill DAG exists"""
        assert 'adhoc_full_backfill' in dag_bag.dags, \
            f"adhoc_full_backfill DAG not found. Available: {list(dag_bag.dags.keys())}"
    
    def test_dag_manual_trigger_only(self, dag_bag):
        """Test DAG is manual trigger only"""
        dag = dag_bag.dags['adhoc_full_backfill']
        
        assert dag.schedule_interval is None


@pytest.mark.integration
@pytest.mark.airflow
class TestDAGDefaultArgs:
    """Test all DAGs have proper default args"""
    
    @pytest.fixture
    def dag_bag(self):
        return DagBag(dag_folder='airflow/dags', include_examples=False)
    
    def test_all_dags_have_owner(self, dag_bag):
        """Test all DAGs have owner configured"""
        for dag_id, dag in dag_bag.dags.items():
            assert 'owner' in dag.default_args, \
                f"DAG {dag_id} missing 'owner' in default_args"
    
    def test_all_dags_have_retries(self, dag_bag):
        """Test all DAGs have retries configured"""
        for dag_id, dag in dag_bag.dags.items():
            assert 'retries' in dag.default_args, \
                f"DAG {dag_id} missing 'retries' in default_args"


@pytest.mark.integration
@pytest.mark.airflow
class TestDAGConcurrency:
    """Test DAG concurrency settings"""
    
    @pytest.fixture
    def dag_bag(self):
        return DagBag(dag_folder='airflow/dags', include_examples=False)
    
    def test_dags_have_max_active_runs(self, dag_bag):
        """Test DAGs have max_active_runs configured"""
        for dag_id, dag in dag_bag.dags.items():
            assert dag.max_active_runs is not None, \
                f"DAG {dag_id} should have max_active_runs configured"
            assert dag.max_active_runs > 0


@pytest.mark.integration
@pytest.mark.airflow
class TestDAGTags:
    """Test DAG tags"""
    
    @pytest.fixture
    def dag_bag(self):
        return DagBag(dag_folder='airflow/dags', include_examples=False)
    
    def test_all_dags_have_tags(self, dag_bag):
        """Test all DAGs have tags configured"""
        for dag_id, dag in dag_bag.dags.items():
            assert dag.tags is not None, \
                f"DAG {dag_id} should have tags configured"
            assert len(dag.tags) > 0, \
                f"DAG {dag_id} should have at least one tag"
