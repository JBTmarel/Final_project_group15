# Task C5

from fastapi import APIRouter, Depends, UploadFile, File, Form
from app.db.session import get_orkuflaedi_session
from sqlalchemy.orm import Session
from app.services.service import (
    get_orku_einingar_data,
    get_notendur_skraning_data,
    get_orku_maelingar_data,
    insert_test_measurement_data
)
from app.utils.validate_date_range import validate_date_range_helper
from datetime import datetime
from app.services.service import (
    get_orku_einingar_data,
    get_notendur_skraning_data,
    get_orku_maelingar_data,
    insert_test_measurement_data,
    get_monthly_energy_flow_data,
    get_monthly_customer_usage_data,
    get_monthly_plant_loss_ratios_data
)

router = APIRouter()
db_name = "OrkuFlaediIsland"


'''
Endpoint 1: get_monthly_energy_flow()
'''
@router.get("/monthly-energy-flow")
def get_monthly_energy_flow_dataw(
    from_date: datetime | None = None,
    to_date: datetime | None = None,
    db: Session = Depends(get_orkuflaedi_session)
):
    print(f"Calling [GET] /{db_name}/monthly-energy-flow")

    from_date, to_date = validate_date_range_helper(
        from_date,
        to_date,
        datetime(2025, 1, 1, 0, 0),
        datetime(2026, 1, 1, 0, 0)
    )

    results = get_monthly_energy_flow_data(db, from_date, to_date)
    return results
'''
Endpoint 2: get_monthly_company_usage()
'''
@router.get("/monthly-customer-usage")
def get_monthly_customer_usage(
    from_date: datetime | None = None,
    to_date: datetime | None = None,
    db: Session = Depends(get_orkuflaedi_session)
):
    print(f"Calling [GET] /{db_name}/monthly-customer-usage")

    from_date, to_date = validate_date_range_helper(
        from_date,
        to_date,
        datetime(2025, 1, 1, 0, 0),
        datetime(2026, 1, 1, 0, 0)
    )

    results = get_monthly_customer_usage_data(db, from_date, to_date)
    return results

'''
Endpoint 3: get_monthly_plant_loss_ratios()
'''
@router.get("/monthly-plant-loss-ratios")
def get_monthly_plant_loss_ratios(
    from_date: datetime | None = None,
    to_date: datetime | None = None,
    db: Session = Depends(get_orkuflaedi_session)
):
    print(f"Calling [GET] /{db_name}/monthly-plant-loss-ratios")

    from_date, to_date = validate_date_range_helper(
        from_date,
        to_date,
        datetime(2025, 1, 1, 0, 0),
        datetime(2026, 1, 1, 0, 0)
    )

    results = get_monthly_plant_loss_ratios_data(db, from_date, to_date)
    return results
# Task E1

'''
Endpoint 4: insert_measurements()
'''

# Task F1
'''
Endpoint 5: get_substations_gridflow()
'''