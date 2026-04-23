# ====================================================================
# Pydantic Model for Quality Check Alerts
# MarketMind Intelligence Platform V1
# Author: Sharique Mohammad
# Date: April 20, 2026
# ====================================================================
# FILE: code/bronze/schemas/quality_alert.py
# Purpose: Python validation model for quality check alerts (system-generated)
# ====================================================================
"""
Quality Alert Pydantic Model

Validates quality check failure alerts before writing to Kafka topic
quality.alerts.v1.

Data Source: System-generated (not from external API)
Kafka Topic: quality.alerts.v1
Avro Schema: quality_alerts_v1.avsc

Field Validation:
- alert_id: UUID format
- layer: Must be BRONZE, SILVER, or GOLD
- check_type: Must be valid quality check type
- severity: Must be CRITICAL, HIGH, MEDIUM, or LOW
- failure_rate: 0-100 percentage

Usage:
    from code.bronze.schemas.quality_alert import (
        QualityAlert, Layer, CheckType, Severity, CheckResult
    )
    
    alert = QualityAlert(
        alert_id="550e8400-e29b-41d4-a716-446655440000",
        check_timestamp=1713564000000,
        layer=Layer.BRONZE,
        table_name="market_bars",
        check_type=CheckType.COMPLETENESS,
        severity=Severity.HIGH,
        check_result=CheckResult.FAIL,
        failure_description="Missing data for 15 tickers on 2026-01-15",
        row_count_checked=500,
        failure_count=15,
        failure_rate=3.0,
        threshold_value=95.0,
        actual_value=97.0,
        pipeline_blocked=True
    )
"""

from pydantic import BaseModel, Field, field_validator, model_validator
from typing import Optional
from datetime import datetime
from enum import Enum
import uuid


class Layer(str, Enum):
    """Data layer where quality check was performed."""
    BRONZE = "BRONZE"
    SILVER = "SILVER"
    GOLD = "GOLD"


class CheckType(str, Enum):
    """Type of quality check."""
    COMPLETENESS = "COMPLETENESS"
    FRESHNESS = "FRESHNESS"
    UNIQUENESS = "UNIQUENESS"
    VALIDITY = "VALIDITY"
    CONSISTENCY = "CONSISTENCY"
    SCHEMA_COMPLIANCE = "SCHEMA_COMPLIANCE"
    RANGE_CHECK = "RANGE_CHECK"


class Severity(str, Enum):
    """Severity level of quality issue."""
    CRITICAL = "CRITICAL"
    HIGH = "HIGH"
    MEDIUM = "MEDIUM"
    LOW = "LOW"


class CheckResult(str, Enum):
    """Result of quality check."""
    FAIL = "FAIL"
    WARN = "WARN"


class QualityAlert(BaseModel):
    """
    Quality check alert model.
    
    Represents quality check failures in Bronze, Silver, or Gold layers.
    System-generated when data quality checks fail.
    """
    
    alert_id: str = Field(
        ...,
        description="Unique identifier for this alert (UUID)"
    )
    
    check_timestamp: int = Field(
        ...,
        description="Unix timestamp when quality check was performed",
        gt=0
    )
    
    layer: Layer = Field(
        ...,
        description="Data layer where check was performed"
    )
    
    table_name: str = Field(
        ...,
        description="Name of table/dataset that failed quality check",
        min_length=1,
        max_length=100
    )
    
    check_type: CheckType = Field(
        ...,
        description="Type of quality check that failed"
    )
    
    severity: Severity = Field(
        ...,
        description="Severity level of the quality issue"
    )
    
    check_result: CheckResult = Field(
        ...,
        description="Result: FAIL (blocks pipeline) or WARN (logs but continues)"
    )
    
    failure_description: str = Field(
        ...,
        description="Human-readable description of what failed",
        min_length=1,
        max_length=500
    )
    
    failure_details: Optional[str] = Field(
        None,
        description="Detailed technical information (JSON format)",
        max_length=5000
    )
    
    row_count_checked: int = Field(
        ...,
        description="Total number of rows checked",
        ge=0
    )
    
    failure_count: int = Field(
        ...,
        description="Number of rows/records that failed",
        ge=0
    )
    
    failure_rate: float = Field(
        ...,
        description="Failure rate as percentage (0-100)",
        ge=0,
        le=100
    )
    
    threshold_value: Optional[float] = Field(
        None,
        description="Threshold value that was violated"
    )
    
    actual_value: Optional[float] = Field(
        None,
        description="Actual measured value that triggered alert"
    )
    
    partition_path: Optional[str] = Field(
        None,
        description="Partition path of data that failed",
        max_length=200
    )
    
    date_range_start: Optional[str] = Field(
        None,
        description="Start date of data checked (YYYY-MM-DD)"
    )
    
    date_range_end: Optional[str] = Field(
        None,
        description="End date of data checked (YYYY-MM-DD)"
    )
    
    pipeline_blocked: bool = Field(
        False,
        description="Whether this failure blocked downstream pipeline"
    )
    
    resolved: bool = Field(
        False,
        description="Whether this issue has been resolved"
    )
    
    resolution_timestamp: Optional[int] = Field(
        None,
        description="Unix timestamp when issue was resolved",
        gt=0
    )
    
    airflow_dag_id: Optional[str] = Field(
        None,
        description="Airflow DAG ID that performed quality check",
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
    
    @field_validator('alert_id')
    @classmethod
    def validate_uuid(cls, v: str) -> str:
        """Validate alert_id is a valid UUID."""
        try:
            uuid.UUID(v)
            return v
        except ValueError:
            raise ValueError(f'alert_id must be a valid UUID, got: {v}')
    
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
    def validate_failure_counts(self) -> 'QualityAlert':
        """Validate failure count relationships."""
        if self.failure_count > self.row_count_checked:
            raise ValueError(
                f'failure_count ({self.failure_count}) cannot exceed '
                f'row_count_checked ({self.row_count_checked})'
            )
        
        # Validate failure_rate calculation
        if self.row_count_checked > 0:
            expected_rate = (self.failure_count / self.row_count_checked) * 100
            if abs(self.failure_rate - expected_rate) > 0.1:
                raise ValueError(
                    f'failure_rate ({self.failure_rate}) does not match '
                    f'calculated rate ({expected_rate:.2f})'
                )
        
        return self
    
    @model_validator(mode='after')
    def validate_resolution(self) -> 'QualityAlert':
        """Validate resolution timestamp is present if resolved."""
        if self.resolved and not self.resolution_timestamp:
            raise ValueError('resolution_timestamp required when resolved=True')
        
        if not self.resolved and self.resolution_timestamp:
            raise ValueError('resolution_timestamp should be null when resolved=False')
        
        return self
    
    def to_dict(self) -> dict:
        """Convert to dictionary for Kafka serialization."""
        return self.model_dump()
    
    @classmethod
    def create_new(
        cls,
        layer: Layer,
        table_name: str,
        check_type: CheckType,
        severity: Severity,
        check_result: CheckResult,
        failure_description: str,
        row_count_checked: int,
        failure_count: int,
        threshold_value: Optional[float] = None,
        actual_value: Optional[float] = None,
        failure_details: Optional[str] = None,
        date_range_start: Optional[str] = None,
        date_range_end: Optional[str] = None,
        pipeline_blocked: bool = False,
        airflow_dag_id: Optional[str] = None,
        airflow_task_id: Optional[str] = None,
        airflow_run_id: Optional[str] = None
    ) -> 'QualityAlert':
        """
        Create a new quality alert.
        
        Args:
            layer: Data layer (BRONZE, SILVER, GOLD)
            table_name: Name of table that failed
            check_type: Type of quality check
            severity: Severity level
            check_result: FAIL or WARN
            failure_description: Human-readable description
            row_count_checked: Total rows checked
            failure_count: Number of failures
            threshold_value: Optional threshold
            actual_value: Optional actual value
            failure_details: Optional detailed JSON
            date_range_start: Optional start date
            date_range_end: Optional end date
            pipeline_blocked: Whether pipeline was blocked
            airflow_dag_id: Optional Airflow DAG ID
            airflow_task_id: Optional Airflow Task ID
            airflow_run_id: Optional Airflow Run ID
        
        Returns:
            QualityAlert instance
        """
        # Calculate failure rate
        if row_count_checked > 0:
            failure_rate = (failure_count / row_count_checked) * 100
        else:
            failure_rate = 0.0
        
        return cls(
            alert_id=str(uuid.uuid4()),
            check_timestamp=int(datetime.now().timestamp() * 1000),
            layer=layer,
            table_name=table_name,
            check_type=check_type,
            severity=severity,
            check_result=check_result,
            failure_description=failure_description,
            failure_details=failure_details,
            row_count_checked=row_count_checked,
            failure_count=failure_count,
            failure_rate=failure_rate,
            threshold_value=threshold_value,
            actual_value=actual_value,
            date_range_start=date_range_start,
            date_range_end=date_range_end,
            pipeline_blocked=pipeline_blocked,
            resolved=False,
            airflow_dag_id=airflow_dag_id,
            airflow_task_id=airflow_task_id,
            airflow_run_id=airflow_run_id
        )
    
    def mark_resolved(self) -> 'QualityAlert':
        """Mark this alert as resolved."""
        self.resolved = True
        self.resolution_timestamp = int(datetime.now().timestamp() * 1000)
        return self


# Example usage
if __name__ == '__main__':
    # Create completeness failure alert
    alert = QualityAlert.create_new(
        layer=Layer.BRONZE,
        table_name="market_bars",
        check_type=CheckType.COMPLETENESS,
        severity=Severity.HIGH,
        check_result=CheckResult.FAIL,
        failure_description="Missing data for 15 tickers on 2026-01-15",
        row_count_checked=500,
        failure_count=15,
        threshold_value=95.0,
        actual_value=97.0,
        date_range_start="2026-01-15",
        date_range_end="2026-01-15",
        pipeline_blocked=True,
        airflow_dag_id="market_bars_quality"
    )
    print(f"Created alert: {alert.alert_id}")
    print(f"Failure rate: {alert.failure_rate}%")
    
    # Resolve alert
    alert.mark_resolved()
    print(f"Alert resolved at: {alert.resolution_timestamp}")
