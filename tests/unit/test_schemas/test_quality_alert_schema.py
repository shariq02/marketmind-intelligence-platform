# MarketMind Intelligence Platform V1
# Unit Tests for QualityAlert Schema
# Date: April 24, 2026

import pytest
import uuid
from pydantic import ValidationError
from code.bronze.schemas.quality_alert import (
    QualityAlert, Layer, CheckType, Severity, CheckResult
)


@pytest.mark.unit
@pytest.mark.schema
class TestQualityAlertSchema:
    """Test suite for QualityAlert Pydantic model"""
    
    def test_valid_alert(self):
        """Test creating valid quality alert"""
        alert = QualityAlert(
            alert_id=str(uuid.uuid4()),
            check_timestamp=1713564000000,
            layer=Layer.BRONZE,
            table_name="market_bars",
            check_type=CheckType.COMPLETENESS,
            severity=Severity.HIGH,
            check_result=CheckResult.FAIL,
            failure_description="Missing data",
            row_count_checked=1000,
            failure_count=100,
            failure_rate=10.0,
            pipeline_blocked=True
        )
        
        assert alert.table_name == "market_bars"
        assert alert.failure_rate == 10.0
    
    def test_uuid_validation(self):
        """Test alert_id must be valid UUID"""
        with pytest.raises(ValidationError) as exc_info:
            QualityAlert(
                alert_id="not-a-uuid",
                check_timestamp=1713564000000,
                layer=Layer.BRONZE,
                table_name="market_bars",
                check_type=CheckType.COMPLETENESS,
                severity=Severity.HIGH,
                check_result=CheckResult.FAIL,
                failure_description="Missing data",
                row_count_checked=1000,
                failure_count=100,
                failure_rate=10.0
            )
        
        assert "must be a valid UUID" in str(exc_info.value)
    
    def test_failure_count_exceeds_checked_fails(self):
        """Test failure_count cannot exceed row_count_checked"""
        with pytest.raises(ValidationError) as exc_info:
            QualityAlert(
                alert_id=str(uuid.uuid4()),
                check_timestamp=1713564000000,
                layer=Layer.BRONZE,
                table_name="market_bars",
                check_type=CheckType.COMPLETENESS,
                severity=Severity.HIGH,
                check_result=CheckResult.FAIL,
                failure_description="Missing data",
                row_count_checked=100,
                failure_count=200,
                failure_rate=200.0
            )
        
        assert "less than or equal to 100" in str(exc_info.value)
    
    def test_failure_rate_mismatch_fails(self):
        """Test failure_rate must match calculation"""
        with pytest.raises(ValidationError) as exc_info:
            QualityAlert(
                alert_id=str(uuid.uuid4()),
                check_timestamp=1713564000000,
                layer=Layer.BRONZE,
                table_name="market_bars",
                check_type=CheckType.COMPLETENESS,
                severity=Severity.HIGH,
                check_result=CheckResult.FAIL,
                failure_description="Missing data",
                row_count_checked=1000,
                failure_count=100,
                failure_rate=50.0
            )
        
        assert "does not match" in str(exc_info.value)
    
    def test_resolved_requires_timestamp(self):
        """Test resolved=True requires resolution_timestamp"""
        with pytest.raises(ValidationError) as exc_info:
            QualityAlert(
                alert_id=str(uuid.uuid4()),
                check_timestamp=1713564000000,
                layer=Layer.BRONZE,
                table_name="market_bars",
                check_type=CheckType.COMPLETENESS,
                severity=Severity.HIGH,
                check_result=CheckResult.FAIL,
                failure_description="Missing data",
                row_count_checked=1000,
                failure_count=100,
                failure_rate=10.0,
                resolved=True
            )
        
        assert "resolution_timestamp required" in str(exc_info.value)
    
    def test_create_new(self):
        """Test creating new alert"""
        alert = QualityAlert.create_new(
            layer=Layer.BRONZE,
            table_name="market_bars",
            check_type=CheckType.COMPLETENESS,
            severity=Severity.HIGH,
            check_result=CheckResult.FAIL,
            failure_description="Missing data",
            row_count_checked=1000,
            failure_count=100
        )
        
        assert alert.failure_rate == 10.0
        assert not alert.resolved
    
    def test_mark_resolved(self):
        """Test marking alert as resolved"""
        alert = QualityAlert.create_new(
            layer=Layer.BRONZE,
            table_name="market_bars",
            check_type=CheckType.COMPLETENESS,
            severity=Severity.HIGH,
            check_result=CheckResult.FAIL,
            failure_description="Missing data",
            row_count_checked=1000,
            failure_count=100
        )
        
        alert.mark_resolved()
        
        assert alert.resolved
        assert alert.resolution_timestamp is not None
    
    def test_to_dict(self):
        """Test conversion to dictionary"""
        alert = QualityAlert.create_new(
            layer=Layer.BRONZE,
            table_name="market_bars",
            check_type=CheckType.COMPLETENESS,
            severity=Severity.HIGH,
            check_result=CheckResult.FAIL,
            failure_description="Missing data",
            row_count_checked=1000,
            failure_count=100
        )
        
        alert_dict = alert.to_dict()
        
        assert isinstance(alert_dict, dict)
        assert alert_dict['table_name'] == 'market_bars'
