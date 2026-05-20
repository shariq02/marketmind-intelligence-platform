# MarketMind Intelligence Platform V1
# Unit Tests for PipelineAudit Schema
# Date: April 24, 2026

import pytest
import uuid
from pydantic import ValidationError
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from code.bronze.schemas.pipeline_audit import (
    PipelineAudit, ConnectorName, IngestionStatus, ExecutionMode
)


@pytest.mark.unit
@pytest.mark.schema
class TestPipelineAuditSchema:
    """Test suite for PipelineAudit Pydantic model"""
    
    def test_valid_audit(self):
        """Test creating valid audit record"""
        audit = PipelineAudit(
            audit_id=str(uuid.uuid4()),
            connector_name=ConnectorName.POLYGON_BARS,
            entity_type="ticker",
            entity_id="AAPL",
            start_timestamp=1713564000000,
            end_timestamp=1713564120000,
            duration_ms=120000,
            status=IngestionStatus.SUCCESS,
            records_retrieved=100,
            records_written=100,
            bytes_written=10000,
            api_calls_made=1,
            execution_mode=ExecutionMode.INCREMENTAL
        )
        
        assert audit.entity_id == "AAPL"
        assert audit.status == IngestionStatus.SUCCESS
    
    def test_uuid_validation(self):
        """Test audit_id must be valid UUID"""
        with pytest.raises(ValidationError) as exc_info:
            PipelineAudit(
                audit_id="not-a-uuid",
                connector_name=ConnectorName.POLYGON_BARS,
                entity_type="ticker",
                entity_id="AAPL",
                start_timestamp=1713564000000,
                end_timestamp=1713564120000,
                duration_ms=120000,
                status=IngestionStatus.SUCCESS,
                records_retrieved=100,
                records_written=100,
                bytes_written=10000,
                api_calls_made=1,
                execution_mode=ExecutionMode.INCREMENTAL
            )
        
        assert "must be a valid UUID" in str(exc_info.value)
    
    def test_end_before_start_fails(self):
        """Test end_timestamp must be >= start_timestamp"""
        with pytest.raises(ValidationError) as exc_info:
            PipelineAudit(
                audit_id=str(uuid.uuid4()),
                connector_name=ConnectorName.POLYGON_BARS,
                entity_type="ticker",
                entity_id="AAPL",
                start_timestamp=1713564120000,
                end_timestamp=1713564000000,
                duration_ms=120000,
                status=IngestionStatus.SUCCESS,
                records_retrieved=100,
                records_written=100,
                bytes_written=10000,
                api_calls_made=1,
                execution_mode=ExecutionMode.INCREMENTAL
            )
        
        assert "must be >=" in str(exc_info.value)
    
    def test_duration_mismatch_fails(self):
        """Test duration_ms must match timestamp difference"""
        with pytest.raises(ValidationError) as exc_info:
            PipelineAudit(
                audit_id=str(uuid.uuid4()),
                connector_name=ConnectorName.POLYGON_BARS,
                entity_type="ticker",
                entity_id="AAPL",
                start_timestamp=1713564000000,
                end_timestamp=1713564120000,
                duration_ms=999999,
                status=IngestionStatus.SUCCESS,
                records_retrieved=100,
                records_written=100,
                bytes_written=10000,
                api_calls_made=1,
                execution_mode=ExecutionMode.INCREMENTAL
            )
        
        assert "does not match" in str(exc_info.value)
    
    def test_failure_requires_error_message(self):
        """Test FAILURE status requires error_message"""
        with pytest.raises(ValidationError) as exc_info:
            PipelineAudit(
                audit_id=str(uuid.uuid4()),
                connector_name=ConnectorName.POLYGON_BARS,
                entity_type="ticker",
                entity_id="AAPL",
                start_timestamp=1713564000000,
                end_timestamp=1713564120000,
                duration_ms=120000,
                status=IngestionStatus.FAILURE,
                records_retrieved=0,
                records_written=0,
                bytes_written=0,
                api_calls_made=1,
                execution_mode=ExecutionMode.INCREMENTAL
            )
        
        assert "error_message required" in str(exc_info.value)
    
    def test_create_new(self):
        """Test creating new audit record"""
        audit = PipelineAudit.create_new(
            connector_name=ConnectorName.POLYGON_BARS,
            entity_type="ticker",
            entity_id="AAPL",
            execution_mode=ExecutionMode.INCREMENTAL
        )
        
        assert audit.entity_id == "AAPL"
        assert audit.status == IngestionStatus.SUCCESS
    
    def test_complete_success(self):
        """Test completing audit with success"""
        audit = PipelineAudit.create_new(
            connector_name=ConnectorName.POLYGON_BARS,
            entity_type="ticker",
            entity_id="AAPL",
            execution_mode=ExecutionMode.INCREMENTAL
        )
        
        audit.complete_success(
            records_retrieved=100,
            records_written=100,
            bytes_written=10000,
            api_calls_made=1
        )
        
        assert audit.status == IngestionStatus.SUCCESS
        assert audit.records_written == 100
    
    def test_complete_failure(self):
        """Test completing audit with failure"""
        audit = PipelineAudit.create_new(
            connector_name=ConnectorName.POLYGON_BARS,
            entity_type="ticker",
            entity_id="AAPL",
            execution_mode=ExecutionMode.INCREMENTAL
        )
        
        audit.complete_failure(
            error_message="API error",
            error_code="API_500"
        )
        
        assert audit.status == IngestionStatus.FAILURE
        assert audit.error_message == "API error"
    
    def test_to_dict(self):
        """Test conversion to dictionary"""
        audit = PipelineAudit.create_new(
            connector_name=ConnectorName.POLYGON_BARS,
            entity_type="ticker",
            entity_id="AAPL",
            execution_mode=ExecutionMode.INCREMENTAL
        )
        
        audit_dict = audit.to_dict()
        
        assert isinstance(audit_dict, dict)
        assert audit_dict['entity_id'] == 'AAPL'
