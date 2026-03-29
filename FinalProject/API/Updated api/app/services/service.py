# Task C5
from fastapi import UploadFile, HTTPException
from sqlalchemy.orm import Session, aliased
from sqlalchemy import func, extract, text, union_all, literal
from datetime import datetime

from app.db.tables.production import Production
from app.db.tables.injects_to import InjectsTo
from app.db.tables.withdraws_from import WithdrawsFrom
from app.db.tables.station import Station
from app.db.tables.customer import Customer
from app.models.monthly_energy_flow_model import MonthlyPlantEnergyFlowModel
from app.models.monthly_company_usage_model import MonthlyCompanyUsageModel
from app.models.monthly_plant_loss_ratios import MonthlyPlantLossRatiosModel
from app.db.tables.test_measurement import TestMeasurement
from app.models.parsed_data.test_measurement_data import TestMeasurementData
from app.parsers.parse_test_measurment_csv import parse_test_measurement_csv
from app.utils.validate_file_type import validate_file_type

'''
Service 1: get_updated_monthly_energy_flow_data()
'''
def get_updated_monthly_energy_flow_data(
    from_date: datetime,
    to_date: datetime,
    db: Session
):
    # Production subquery
    production_q = db.query(
        Station.name.label("power_plant_source"),
        literal("Framleiðsla").label("measurement_type"),
        extract("year", Production.timestamp).label("year"),
        extract("month", Production.timestamp).label("month"),
        Production.value_kwh.label("value_kwh")
    ).join(
        Station, Production.power_plant_id == Station.id
    ).filter(
        Production.timestamp >= from_date,
        Production.timestamp <= to_date
    )

    # Injection subquery
    injection_q = db.query(
        Station.name.label("power_plant_source"),
        literal("Innmötun").label("measurement_type"),
        extract("year", InjectsTo.timestamp).label("year"),
        extract("month", InjectsTo.timestamp).label("month"),
        InjectsTo.value_kwh.label("value_kwh")
    ).join(
        Station, InjectsTo.power_plant_id == Station.id
    ).filter(
        InjectsTo.timestamp >= from_date,
        InjectsTo.timestamp <= to_date
    )

    # Withdrawal subquery
    withdrawal_q = db.query(
        Station.name.label("power_plant_source"),
        literal("Úttekt").label("measurement_type"),
        extract("year", WithdrawsFrom.timestamp).label("year"),
        extract("month", WithdrawsFrom.timestamp).label("month"),
        WithdrawsFrom.value_kwh.label("value_kwh")
    ).join(
        InjectsTo,
        (WithdrawsFrom.substation_id == InjectsTo.substation_id) &
        (WithdrawsFrom.timestamp == InjectsTo.timestamp)
    ).join(
        Station, InjectsTo.power_plant_id == Station.id
    ).filter(
        WithdrawsFrom.timestamp >= from_date,
        WithdrawsFrom.timestamp <= to_date
    )

    # Union all three
    combined = union_all(
        production_q.selectable,
        injection_q.selectable,
        withdrawal_q.selectable
    ).subquery()

    # Final aggregation
    rows = db.query(
        combined.c.power_plant_source,
        combined.c.measurement_type,
        combined.c.year,
        combined.c.month,
        func.sum(combined.c.value_kwh).label("total_kwh")
    ).group_by(
        combined.c.power_plant_source,
        combined.c.measurement_type,
        combined.c.year,
        combined.c.month
    ).order_by(
        combined.c.power_plant_source,
        combined.c.month.asc(),
        func.sum(combined.c.value_kwh).desc()
    ).all()

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
Service 2: get_updated_monthly_customer_usage_data()
'''
def get_updated_monthly_customer_usage_data(
    from_date: datetime,
    to_date: datetime,
    db: Session
):
    rows = (
        db.query(
            extract("year", WithdrawsFrom.timestamp).label("year"),
            extract("month", WithdrawsFrom.timestamp).label("month"),
            Customer.name.label("customer_name"),
            func.sum(WithdrawsFrom.value_kwh).label("total_kwh")
        )
        .join(Customer, WithdrawsFrom.customer_id == Customer.id)
        .filter(
            WithdrawsFrom.timestamp >= from_date,
            WithdrawsFrom.timestamp <= to_date
        )
        .group_by(
            extract("year", WithdrawsFrom.timestamp),
            extract("month", WithdrawsFrom.timestamp),
            Customer.name
        )
        .order_by(
            extract("month", WithdrawsFrom.timestamp).asc(),
            Customer.name.asc()
        )
        .all()
    )

    return [
        MonthlyCompanyUsageModel(
            power_plant_source="N/A",
            customer_name=row.customer_name,
            year=int(row.year),
            month=int(row.month),
            total_kwh=row.total_kwh
        )
        for row in rows
    ]


# Kannski óþarfi?

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
