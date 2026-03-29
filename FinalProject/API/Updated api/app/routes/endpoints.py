# Task C5

from fastapi import APIRouter, Depends, UploadFile, File, Form
from app.db.session import get_orkuflaedi_session
from sqlalchemy.orm import Session
from app.services.service import (
    get_updated_monthly_energy_flow_data,
    get_updated_monthly_customer_usage_data,
    insert_test_measurement_data
)
from app.utils.validate_date_range import validate_date_range_helper
from datetime import datetime

router = APIRouter()
db_name = "OrkuFlaediIsland"


'''
Endpoint 1: get_updated_monthly_energy_flow_data()
'''
@router.get("/monthly-energy-flow")
def get_updated_monthly_energy_flow(
    from_date: datetime | None = None,
    to_date: datetime | None = None,
    db: Session = Depends(get_orkuflaedi_session)
):
    print(f"Calling [GET] /{db_name}/updated-monthly-energy-flow")

    from_date, to_date = validate_date_range_helper(
        from_date,
        to_date,
        datetime(2025, 1, 1, 0, 0),
        datetime(2026, 1, 1, 0, 0)
    )

    results = get_updated_monthly_energy_flow_data(from_date, to_date, db)
    return results
'''
Endpoint 2: get_updated_monthly_customer_usage()
'''
@router.get("/monthly-customer-usage")
def get_updated_monthly_customer_usage(
    from_date: datetime | None = None,
    to_date: datetime | None = None,
    db: Session = Depends(get_orkuflaedi_session)
):
    print(f"Calling [GET] /{db_name}/updated-monthly-customer-usage")

    from_date, to_date = validate_date_range_helper(
        from_date,
        to_date,
        datetime(2025, 1, 1, 0, 0),
        datetime(2026, 1, 1, 0, 0)
    )

    results = get_updated_monthly_customer_usage_data(from_date, to_date, db)
    return results

'''
Endpoint 3: get_updated_monthly_plant_loss_ratios()
'''

# Task E1

'''
Endpoint 4: insert_measurements()
'''

# Task F1
'''
Endpoint 5: get_substations_gridflow()
'''