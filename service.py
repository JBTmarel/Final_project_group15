from fastapi import UploadFile, HTTPException
from sqlalchemy.orm import Session, aliased
from sqlalchemy import func, extract, text
from app.models.monthly_energy_flow_model import MonthlyPlantEnergyFlowModel
from app.models.monthly_company_usage_model import MonthlyCompanyUsageModel
from app.models.monthly_plant_loss_ratios import MonthlyPlantLossRatiosModel
from app.db.tables.orku_einingar import OrkuEiningar
from app.models.orku_einingar_model import OrkuEiningarModel
from app.db.tables.notendur_skraning import NotendurSkraning
from app.models.notendur_skraning_model import NotendurSkraningModel
from app.db.tables.orku_maelingar import OrkuMaelingar
from app.models.orku_maelingar_model import OrkuMaelingarModel
from app.db.tables.test_measurement import TestMeasurement
from app.models.parsed_data.test_measurement_data import TestMeasurementData
from app.parsers.parse_test_measurment_csv import parse_test_measurement_csv
from app.utils.validate_file_type import validate_file_type
from datetime import datetime

'''
Services already in place
'''
def get_orku_einingar_data(
    db: Session
):
    rows = db.query(OrkuEiningar).all()

    return [
        OrkuEiningarModel(
            id=row.id,
            heiti=row.heiti,
            tegund=row.tegund,
            tegund_stod=row.tegund_stod,
            eigandi=row.eigandi,
            ar_uppsett=row.ar_uppsett,
            manudir_uppsett=row.manudir_uppsett,
            dagur_uppsett=row.dagur_uppsett,
            X_HNIT=row.X_HNIT,
            Y_HNIT=row.Y_HNIT,
            tengd_stod=row.tengd_stod,
        ) 
        for row in rows
    ]

def get_notendur_skraning_data(
    db: Session
):
    rows = db.query(NotendurSkraning).all()

    return [
        NotendurSkraningModel(
            id=row.id,
            heiti=row.heiti,
            kennitala=row.kennitala,
            eigandi=row.eigandi,
            ar_stofnad=row.ar_stofnad,
            X_HNIT=row.X_HNIT,
            Y_HNIT=row.Y_HNIT,
        ) 
        for row in rows
    ]

def get_orku_maelingar_data(
    from_date: datetime,
    to_date: datetime,
    limit: int,
    offset: int,
    db: Session,
    eining: str | None = None,
    tegund: str | None = None,
):
    query = db.query(OrkuMaelingar).filter(
        OrkuMaelingar.timi >= from_date,
        OrkuMaelingar.timi <= to_date
    )

    if eining:
        query = query.filter(OrkuMaelingar.eining_heiti == eining)
    if tegund:
        query = query.filter(OrkuMaelingar.tegund_maelingar == tegund)

    rows = (
        query
        .order_by(OrkuMaelingar.timi)
        .limit(limit)
        .offset(offset)
        .all()
    )

    return [
        OrkuMaelingarModel(
            id=row.id,
            eining_heiti=row.eining_heiti,
            tegund_maelingar=row.tegund_maelingar,
            sendandi_maelingar=row.sendandi_maelingar,
            timi=row.timi,
            gildi_kwh=row.gildi_kwh,
            notandi_heiti=row.notandi_heiti
        )
        for row in rows
    ]

async def insert_test_measurement_data(
    file: UploadFile,
    db: Session,
    mode: str = "bulk"
):
    validate_file_type(
        file, 
        allowed_extensions=[".csv"]
    )

    raw_data = await file.read()
    raw_text = raw_data.decode()

    parsed_rows: list[TestMeasurementData]
    parsed_rows = parse_test_measurement_csv(raw_text)

    if not parsed_rows:
        raise HTTPException(status_code=400, detail="No valid rows found")

    try:
        if mode == "single":
            for row in parsed_rows:
                db.add(
                    TestMeasurement(
                        timi=row.timi,
                        value=row.value
                    )
                )
            db.commit()

        elif mode == "bulk":
            insert_data = [
                {
                    "timi": row.timi,
                    "value": row.value
                }
                for row in parsed_rows
            ]
            db.bulk_insert_mappings(TestMeasurement, insert_data)
            db.commit()

        elif mode == "fallback":
            for row in parsed_rows:
                try:
                    db.add(
                        TestMeasurement(
                            timi=row.timi,
                            value=row.value
                        )
                    )
                    db.flush()
                except Exception:
                    db.rollback()
                    continue
            db.commit()
        else:
            raise HTTPException(status_code=400, detail="Invalid mode")

        return {
            "status": 200,
            "rows_processed": len(parsed_rows),
            "mode": mode
        }
    
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    
# Task B2

'''
Service 1: get_monthly_energy_flow_data()
'''
def get_monthly_energy_flow_data(
    from_date: datetime,
    to_date: datetime,
    db: Session
):
    rows = (
        db.query(
            OrkuMaelingar.eining_heiti.label("power_plant_source"),
            extract("year", OrkuMaelingar.timi).label("year"),
            extract("month", OrkuMaelingar.timi).label("month"),
            OrkuMaelingar.tegund_maelingar.label("measurement_type"),
            func.sum(OrkuMaelingar.gildi_kwh).label("total_kwh")
        )
        .filter(
            OrkuMaelingar.timi >= from_date,
            OrkuMaelingar.timi <= to_date
        )
        .group_by(
            OrkuMaelingar.eining_heiti,
            extract("month", OrkuMaelingar.timi),
            extract("year", OrkuMaelingar.timi),
            OrkuMaelingar.tegund_maelingar
        )
        .order_by(
            OrkuMaelingar.eining_heiti,
            extract("month", OrkuMaelingar.timi).asc(),
            func.sum(OrkuMaelingar.gildi_kwh).desc()
        )
        .all()
    )

    return [
        MonthlyPlantEnergyFlowModel(
            power_plant_source=row.power_plant_source,
            measurement_type=row.measurement_type,
            year=int(row.year),
            month=int(row.month),
            total_kwh=row.total_kwh
        )
        for row in rows
    ]
'''
Service 2: get_monthly_company_usage_data()
'''
def get_monthly_company_usage_data(
    from_date: datetime,
    to_date: datetime,
    db: Session
):
    rows = (
        db.query(
            OrkuMaelingar.eining_heiti.label("power_plant_source"),
            extract("year", OrkuMaelingar.timi).label("year"),
            extract("month", OrkuMaelingar.timi).label("month"),
            OrkuMaelingar.notandi_heiti.label("customer_name"),
            func.sum(OrkuMaelingar.gildi_kwh).label("total_kwh")
        )
        .filter(
            OrkuMaelingar.timi >= '2025-01-01',
            OrkuMaelingar.timi <= '2026-01-01',
            OrkuMaelingar.tegund_maelingar == "Úttekt"
        )
        .group_by(
            OrkuMaelingar.eining_heiti,
            extract("year", OrkuMaelingar.timi),
            extract("month", OrkuMaelingar.timi),
            OrkuMaelingar.notandi_heiti
        )
        .order_by(
            OrkuMaelingar.eining_heiti,
            extract("month", OrkuMaelingar.timi).asc(),
            OrkuMaelingar.notandi_heiti.desc()
        )
        .all()
    )

    return [
        MonthlyPlantEnergyFlowModel(
            power_plant_source=row.power_plant_source,
            measurement_type=row.measurement_type,
            year=int(row.year),
            month=int(row.month),
            total_kwh=row.total_kwh
        )
        for row in rows
    ]

'''
Service 3: get_monthly_plant_loss_ratios_data()
'''

def get_monthly_plant_loss_ratios_data(
    from_date: datetime,
    to_date: datetime,
    db: Session
):
    # Subquery representing the view
    monthly_plant_totals = (
        db.query(
            OrkuMaelingar.eining_heiti.label("power_plant_source"),
            extract("year", OrkuMaelingar.timi).label("year"),
            extract("month", OrkuMaelingar.timi).label("month"),
            OrkuMaelingar.tegund_maelingar.label("tegund_maelingar"),
            func.sum(OrkuMaelingar.gildi_kwh).label("total_kwh")
        )
        .filter(
            OrkuMaelingar.timi >= from_date,
            OrkuMaelingar.timi <= to_date
        )
        .group_by(
            OrkuMaelingar.eining_heiti,
            extract("year", OrkuMaelingar.timi),
            extract("month", OrkuMaelingar.timi),
            OrkuMaelingar.tegund_maelingar
        )
        .subquery()
    )

    # Aliases for the three joins (f, i, u)
    f = aliased(monthly_plant_totals, name="f")
    i = aliased(monthly_plant_totals, name="i")
    u = aliased(monthly_plant_totals, name="u")

    rows = (
        db.query(
            f.c.power_plant_source,
            func.avg(
                (f.c.total_kwh - i.c.total_kwh) / f.c.total_kwh
            ).label("plant_to_substation_loss_ratio"),
            func.avg(
                (f.c.total_kwh - u.c.total_kwh) / f.c.total_kwh
            ).label("total_system_loss_ratio")
        )
        .join(i, 
            (f.c.power_plant_source == i.c.power_plant_source) &
            (f.c.month == i.c.month) &
            (f.c.year == i.c.year) &
            (i.c.tegund_maelingar == "Innmötun")
        )
        .join(u,
            (f.c.power_plant_source == u.c.power_plant_source) &
            (f.c.month == u.c.month) &
            (f.c.year == u.c.year) &
            (u.c.tegund_maelingar == "Úttekt")
        )
        .filter(f.c.tegund_maelingar == "Framleiðsla")
        .group_by(f.c.power_plant_source)
        .order_by(f.c.power_plant_source)
        .all()
    )

    return [
        MonthlyPlantLossRatiosModel(
            power_plant_source=row.power_plant_source,
            plant_to_substation_loss_ratio=row.plant_to_substation_loss_ratio,
            total_system_loss_ratio=row.total_system_loss_ratio
        )
        for row in rows
    ]

