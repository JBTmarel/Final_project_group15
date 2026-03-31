# Task C5
from fastapi import UploadFile, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import func, extract, union_all, literal
from datetime import datetime

from app.db.tables.production import Production
from app.db.tables.injects_to import InjectsTo
from app.db.tables.withdraws_from import WithdrawsFrom
from app.db.tables.station import Station
from app.db.tables.customer import Customer
from app.db.tables.v_monthly_plant_energy import VMonthlyPlantEnergy
from app.models.monthly_energy_flow_model import MonthlyPlantEnergyFlowModel
from app.models.monthly_company_usage_model import MonthlyCompanyUsageModel
from app.models.monthly_plant_loss_ratios import MonthlyPlantLossRatiosModel
from app.models.parsed_data.measurement_data import MeasurementData
from app.parsers.parse_test_measurment_csv import parse_measurements_csv
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


'''
Service 3: get_updated_monthly_plant_loss_ratios()
'''

def get_updated_monthly_plant_loss_ratios_data(
    from_date: datetime,
    to_date: datetime,
    db: Session
):
    rows = (
        db.query(
            VMonthlyPlantEnergy.power_plant_source,
            func.avg(
                (VMonthlyPlantEnergy.production_kwh - VMonthlyPlantEnergy.injection_kwh) /
                func.nullif(VMonthlyPlantEnergy.production_kwh, 0)
            ).label("plant_to_substation_loss_ratio"),
            func.avg(
                (VMonthlyPlantEnergy.production_kwh - VMonthlyPlantEnergy.attributed_withdrawal_kwh) /
                func.nullif(VMonthlyPlantEnergy.production_kwh, 0)
            ).label("total_system_loss_ratio")
        )
        .group_by(VMonthlyPlantEnergy.power_plant_source)
        .order_by(VMonthlyPlantEnergy.power_plant_source)
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

# Task E1
'''Service 4: insert_test_measurement_data()'''
async def insert_measurements_data(
    file: UploadFile,
    db: Session,
    mode: str = "bulk"
):
    validate_file_type(file, allowed_extensions=[".csv"])

    raw_data = await file.read()
    raw_text = raw_data.decode()

    parsed_rows: list[MeasurementData]
    parsed_rows = parse_measurements_csv(raw_text)

    if not parsed_rows:
        raise HTTPException(status_code=400, detail="No valid rows found")

    # Build lookup maps to convert text references to foreign key IDs
    station_map = {s.name: s.id for s in db.query(Station).all()}
    customer_map = {c.name: c.id for c in db.query(Customer).all()}

    def build_insert_objects(rows):
        objects = []
        for row in rows:
            if row.tegund_maelingar == "Framleiðsla":
                pp_id = station_map.get(row.eining_heiti)
                if pp_id:
                    objects.append(Production(
                        power_plant_id=pp_id,
                        timestamp=row.timi,
                        value_kwh=row.gildi_kwh
                    ))
            elif row.tegund_maelingar == "Innmötun":
                pp_id = station_map.get(row.eining_heiti)
                sub_id = station_map.get(row.sendandi_maelingar)
                if pp_id and sub_id:
                    objects.append(InjectsTo(
                        power_plant_id=pp_id,
                        production_timestamp=row.timi,
                        substation_id=sub_id,
                        timestamp=row.timi,
                        value_kwh=row.gildi_kwh
                    ))
            elif row.tegund_maelingar == "Úttekt":
                sub_id = station_map.get(row.sendandi_maelingar)
                customer_id = customer_map.get(row.notandi_heiti)
                if sub_id and customer_id:
                    objects.append(WithdrawsFrom(
                        customer_id=customer_id,
                        substation_id=sub_id,
                        timestamp=row.timi,
                        value_kwh=row.gildi_kwh
                    ))
        return objects

    try:
        if mode == "single":
            for obj in build_insert_objects(parsed_rows):
                db.add(obj)
            db.commit()

        elif mode == "bulk":
            for obj in build_insert_objects(parsed_rows):
                db.add(obj)
            db.commit()

        elif mode == "fallback":
            for obj in build_insert_objects(parsed_rows):
                try:
                    db.add(obj)
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