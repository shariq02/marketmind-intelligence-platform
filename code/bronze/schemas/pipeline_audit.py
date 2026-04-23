# ====================================================================
# Pydantic Model for Pipeline Audit Trail
# MarketMind Intelligence Platform V1
# Author: Sharique Mohammad
# Date: April 20, 2026
# ====================================================================
# FILE: code/bronze/schemas/pipeline_audit.py
# Purpose: Python validation model for pipeline audit trail (system-generated)
# ====================================================================
"""
Pipeline Audit Pydantic Model

Validates pipeline audit records before writing to Kafka topic
pipeline.audit.v1.

Data Source: System-generated (not from external API)
Kafka Topic: pipeline.audit.v1
Avro Schema: pipeline_audit_v1.avsc

Field Validation:
- audit_id: UUID format
- connector_name: Must be valid connector enum
- execution_mode: Must be valid mode (COLD_BOOTSTRAP, INCREMENTAL, etc.)
- status: Must be SUCCESS, FAILURE, PARTIAL_SUCCESS, or SKIPPED
- timestamps: Start must be before end

Usage:
    from code.bronze.schemas.pipeline_audit import (
        PipelineAudit, ConnectorName, IngestionStatus, ExecutionMode
    )
    
    audit = PipelineAudit(
        audit_id="550e8400-e29b-41d4-a716-446655440000",
        connector_name=ConnectorName.POLYGON_BARS,
        entity_type="ticker",
        entity_id="AAPL",
        start_timestamp=1713564000000,
        end_timestamp=1713564120000,
        duration_ms=120000,
        status=IngestionStatus.SUCCESS,
        records_retrieved=178,
        records_written=178,
        bytes_written=45000,
        api_calls_made=1,
        execution_mode=ExecutionMode.INCREMENTAL,
        checkpoint_updated=True
    )
"""

from pydantic import BaseModel, Field, field_validator, model_validator
from typing import Optional
from datetime import datetime
from enum import Enum
import uuid


class ConnectorName(str, Enum):
    """Type of data connector."""
    POLYGON_BARS = "POLYGON_BARS"
    POLYGON_CORPORATE_ACTIONS = "POLYGON_CORPORATE_ACTIONS"
    AKSHARE_MACRO = "AKSHARE_MACRO"
    EDGARTOOLS_FILINGS = "EDGARTOOLS_FILINGS"


class IngestionStatus(str, Enum):
    """Outcome of ingestion attempt."""
    SUCCESS = "SUCCESS"
    PARTIAL_SUCCESS = "PARTIAL_SUCCESS"
    FAILURE = "FAILURE"
    SKIPPED = "SKIPPED"


class ExecutionMode(str, Enum):
    """Mode of pipeline execution."""
    COLD_BOOTSTRAP = "COLD_BOOTSTRAP"
    INCREMENTAL = "INCREMENTAL"
    BACKFILL = "BACKFILL"
    REPLAY = "REPLAY"


class PipelineAudit(BaseModel):
    """
    Pipeline audit trail model.
    
    Tracks what data was ingested, when, and by which connector.
    System-generated during data ingestion pipeline execution.
    """
    
    audit_id: str = Field(
        ...,
        description="Unique identifier for this audit record (UUID)"
    )
    
    connector_name: ConnectorName = Field(
        ...,
        description="Name of the connector that performed ingestion"
    )
    
    entity_type: str = Field(
        ...,
        description="Type of entity ingested (ticker, indicator, filing)",
        min_length=1,
        max_length=50
    )
    
    entity_id: str = Field(
        ...,
        description="Identifier of the entity (e.g., AAPL, US_GDP_QOQ)",
        min_length=1,
        max_length=100
    )
    
    start_timestamp: int = Field(
        ...,
        description="Unix timestamp when ingestion started",
        gt=0
    )
    
    end_timestamp: int = Field(
        ...,
        description="Unix timestamp when ingestion completed",
        gt=0
    )
    
    duration_ms: int = Field(
        ...,
        description="Duration of ingestion in milliseconds",
        ge=0
    )
    
    status: IngestionStatus = Field(
        ...,
        description="Outcome of the ingestion attempt"
    )
    
    records_retrieved: int = Field(
        ...,
        description="Number of records retrieved from source API",
        ge=0
    )
    
    records_written: int = Field(
        ...,
        description="Number of records successfully written to Kafka",
        ge=0
    )
    
    bytes_written: int = Field(
        ...,
        description="Total bytes written to Kafka topic",
        ge=0
    )
    
    date_range_start: Optional[str] = Field(
        None,
        description="Start date of data requested (YYYY-MM-DD)"
    )
    
    date_range_end: Optional[str] = Field(
        None,
        description="End date of data requested (YYYY-MM-DD)"
    )
    
    error_message: Optional[str] = Field(
        None,
        description="Error message if status is FAILURE or PARTIAL_SUCCESS",
        max_length=1000
    )
    
    error_code: Optional[str] = Field(
        None,
        description="Error code if status is FAILURE or PARTIAL_SUCCESS",
        max_length=50
    )
    
    api_calls_made: int = Field(
        ...,
        description="Number of API calls made to source",
        ge=0
    )
    
    rate_limited: bool = Field(
        False,
        description="Whether rate limiting was encountered"
    )
    
    checkpoint_updated: bool = Field(
        False,
        description="Whether checkpoint was updated in PostgreSQL"
    )
    
    execution_mode: ExecutionMode = Field(
        ...,
        description="Mode of execution"
    )
    
    airflow_dag_id: Optional[str] = Field(
        None,
        description="Airflow DAG ID that triggered ingestion",
        max_length=100
    )
    
    airflow_task_id: Optional[str] = Field(
        None,
        description="Airflow Task ID",
        max_length=100
    )
    
    airflow_run_id: Optional[str] = Field(
        None,
        description="Airflow Run ID (execution date)",
        max_length=100
    )
    
    @field_validator('audit_id')
    @classmethod
    def validate_uuid(cls, v: str) -> str:
        """Validate audit_id is a valid UUID."""
        try:
            uuid.UUID(v)
            return v
        except ValueError:
            raise ValueError(f'audit_id must be a valid UUID, got: {v}')
    
    @field_validator('date_range_start', 'date_range_end')
    @classmethod
    def validate_date_format(cls, v: Optional[str]) -> Optional[str]:
        """Validate date is in YYYY-MM-DD format."""
        if v is None:
            return v
        
        try:
            datetime.strptime(v, '%Y-%m-%d')
            return v
        except ValueError:
            raise ValueError(f'Date must be in YYYY-MM-DD format, got: {v}')
    
    @model_validator(mode='after')
    def validate_timestamps(self) -> 'PipelineAudit':
        """Validate timestamp relationships."""
        if self.end_timestamp < self.start_timestamp:
            raise ValueError('end_timestamp must be >= start_timestamp')
        
        # Duration should match timestamp difference
        expected_duration = self.end_timestamp - self.start_timestamp
        if abs(self.duration_ms - expected_duration) > 1000:  # Allow 1s tolerance
            raise ValueError(
                f'duration_ms ({self.duration_ms}) does not match '
                f'timestamp difference ({expected_duration})'
            )
        
        return self
    
    @model_validator(mode='after')
    def validate_error_fields(self) -> 'PipelineAudit':
        """Validate error fields based on status."""
        if self.status in [IngestionStatus.FAILURE, IngestionStatus.PARTIAL_SUCCESS]:
            if not self.error_message:
                raise ValueError(
                    f'error_message required for status {self.status.value}'
                )
        
        return self
    
    def to_dict(self) -> dict:
        """Convert to dictionary for Kafka serialization."""
        return self.model_dump()
    
    @classmethod
    def create_new(
        cls,
        connector_name: ConnectorName,
        entity_type: str,
        entity_id: str,
        execution_mode: ExecutionMode,
        airflow_dag_id: Optional[str] = None,
        airflow_task_id: Optional[str] = None,
        airflow_run_id: Optional[str] = None
    ) -> 'PipelineAudit':
        """
        Create a new audit record at start of ingestion.
        Call complete_success() or complete_failure() when done.
        
        Args:
            connector_name: Connector performing ingestion
            entity_type: Type of entity (ticker, indicator, filing)
            entity_id: Entity identifier
            execution_mode: Execution mode
            airflow_dag_id: Optional Airflow DAG ID
            airflow_task_id: Optional Airflow Task ID
            airflow_run_id: Optional Airflow Run ID
        
        Returns:
            PipelineAudit instance with start_timestamp set
        """
        start_ts = int(datetime.now().timestamp() * 1000)
        
        return cls(
            audit_id=str(uuid.uuid4()),
            connector_name=connector_name,
            entity_type=entity_type,
            entity_id=entity_id,
            start_timestamp=start_ts,
            end_timestamp=start_ts,  # Placeholder, updated on completion
            duration_ms=0,  # Placeholder
            status=IngestionStatus.SUCCESS,  # Placeholder
            records_retrieved=0,
            records_written=0,
            bytes_written=0,
            api_calls_made=0,
            rate_limited=False,
            checkpoint_updated=False,
            execution_mode=execution_mode,
            airflow_dag_id=airflow_dag_id,
            airflow_task_id=airflow_task_id,
            airflow_run_id=airflow_run_id
        )
    
    def complete_success(
        self,
        records_retrieved: int,
        records_written: int,
        bytes_written: int,
        api_calls_made: int,
        rate_limited: bool = False,
        checkpoint_updated: bool = True,
        date_range_start: Optional[str] = None,
        date_range_end: Optional[str] = None
    ) -> 'PipelineAudit':
        """Update audit record on successful completion."""
        end_ts = int(datetime.now().timestamp() * 1000)
        
        self.end_timestamp = end_ts
        self.duration_ms = end_ts - self.start_timestamp
        self.status = IngestionStatus.SUCCESS
        self.records_retrieved = records_retrieved
        self.records_written = records_written
        self.bytes_written = bytes_written
        self.api_calls_made = api_calls_made
        self.rate_limited = rate_limited
        self.checkpoint_updated = checkpoint_updated
        self.date_range_start = date_range_start
        self.date_range_end = date_range_end
        
        return self
    
    def complete_failure(
        self,
        error_message: str,
        error_code: str,
        records_retrieved: int = 0,
        api_calls_made: int = 0
    ) -> 'PipelineAudit':
        """Update audit record on failure."""
        end_ts = int(datetime.now().timestamp() * 1000)
        
        self.end_timestamp = end_ts
        self.duration_ms = end_ts - self.start_timestamp
        self.status = IngestionStatus.FAILURE
        self.records_retrieved = records_retrieved
        self.records_written = 0
        self.bytes_written = 0
        self.api_calls_made = api_calls_made
        self.error_message = error_message
        self.error_code = error_code
        
        return self


# Example usage
if __name__ == '__main__':
    # Create new audit at start
    audit = PipelineAudit.create_new(
        connector_name=ConnectorName.POLYGON_BARS,
        entity_type="ticker",
        entity_id="AAPL",
        execution_mode=ExecutionMode.INCREMENTAL,
        airflow_dag_id="market_bars_daily"
    )
    print(f"Started audit: {audit.audit_id}")
    
    # Complete with success
    audit.complete_success(
        records_retrieved=178,
        records_written=178,
        bytes_written=45000,
        api_calls_made=1,
        date_range_start="2026-04-20",
        date_range_end="2026-04-20"
    )
    print(f"Completed: {audit.status.value}, duration={audit.duration_ms}ms")
