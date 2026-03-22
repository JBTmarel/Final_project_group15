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

router = APIRouter()
db_name = "OrkuFlaediIsland"

'''
Endpoints already in place
'''

@router.get("/orku_einingar")
def get_orku_einingar(
    db: Session = Depends(get_orkuflaedi_session)
):
    print(f"Calling [GET] /{db_name}/orku_einingar")
    results = get_orku_einingar_data(db)
    return results


@router.get("/skradir_notendur")
def get_notendur_skraning(
    db: Session = Depends(get_orkuflaedi_session)
):
    print(f"Calling [GET] /{db_name}/skradir_notendur")
    results = get_notendur_skraning_data(db)
    return results


@router.get("/maelingar")
def get_orku_maelingar(
    from_date: datetime | None = None,
    to_date: datetime | None = None,
    eining: str | None = None,
    tegund: str | None = None,
    limit: int = 1000,
    offset: int = 0,
    db: Session = Depends(get_orkuflaedi_session)
):
    print(f"Calling [GET] /{db_name}/maelingar")

    from_date, to_date = validate_date_range_helper(
        from_date,
        to_date,
        datetime(2025, 1, 1, 0, 0),
        datetime(2025, 1, 2, 0, 0)
    )

    results = get_orku_maelingar_data(
        from_date,
        to_date,
        limit,
        offset,
        db,
        eining,
        tegund
    )
    return results


@router.post("/test-measurement-data")
async def insert_test_measurement(
    mode: str = Form(...),
    file: UploadFile = File(...),
    db: Session = Depends(get_orkuflaedi_session)
):
    print(f"Calling [POST] /{db_name}/test-measurement-data")

    result = await insert_test_measurement_data(file, db, mode)
    return result

# Task B2

'''
Endpoint 1: get_monthly_energy_flow()
'''

'''
Endpoint 2: get_monthly_company_usage()
'''

'''
Endpoint 3: get_monthly_plant_loss_ratios()
'''
