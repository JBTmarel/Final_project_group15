# Task C5

from fastapi import APIRouter, Depends, UploadFile, File, Form
from app.db.session import get_orkuflaedi_session
from sqlalchemy.orm import Session
from app.services.service import (
    get_updated_monthly_energy_flow_data,
    get_updated_monthly_customer_usage_data,
    get_updated_monthly_plant_loss_ratios_data,
    insert_measurements_data,
    get_substation_flow_data
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
@router.get("/updated/monthly-plant-loss-ratios")
def get_updated_monthly_plant_loss_ratios(
    from_date: datetime | None = None,
    to_date: datetime | None = None,
    db: Session = Depends(get_orkuflaedi_session)
):
    print(f"Calling [GET] /{db_name}/updated/monthly-plant-loss-ratios")
    from_date, to_date = validate_date_range_helper(
        from_date,
        to_date,
        datetime(2025, 1, 1, 0, 0),
        datetime(2026, 1, 1, 0, 0)
    )
    return get_updated_monthly_plant_loss_ratios_data(from_date, to_date, db)

# Task E1

'''
Endpoint 4: insert_measurements()
'''

#From legacy:
@router.post("/measurements-data")
async def insert_test_measurement(
    mode: str = Form(...),
    file: UploadFile = File(...),
    db: Session = Depends(get_orkuflaedi_session)
):
    print(f"Calling [POST] /{db_name}/measurements-data")

    result = await insert_measurements_data(file, db, mode)
    return result

# Task F1
'''
Endpoint 5: get_substations_gridflow()
'''

@router.get("/substation-gridflow")
def get_substation_gridflow(
    from_date: datetime | None = None,
    to_date: datetime | None = None,
    db: Session = Depends(get_orkuflaedi_session)
):
    print(f"Calling [GET] /{db_name}/substation-gridflow")
    from_date, to_date = validate_date_range_helper(
        from_date,
        to_date,
        datetime(2025, 1, 1, 0, 0),
        datetime(2026, 1, 1, 0, 0)
    )
    return get_substation_flow_data(from_date, to_date, db)
