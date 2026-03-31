# Task C5
from statistics import mode

from fastapi import UploadFile, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import func, extract, union_all, literal
from datetime import datetime

from app.db.tables.station import Station   
from app.db.tables.production import Production      
from app.db.tables.injects_to import InjectsTo       
from app.db.tables.withdraws_from import WithdrawsFrom
from app.db.tables.customer import Customer
from app.db.tables.v_monthly_plant_energy import VMonthlyPlantEnergy
from app.db.tables.connects_to import ConnectsTo

from app.models.monthly_energy_flow_model import MonthlyPlantEnergyFlowModel
from app.models.monthly_company_usage_model import MonthlyCompanyUsageModel
from app.models.monthly_plant_loss_ratios import MonthlyPlantLossRatiosModel
from app.models.parsed_data.measurement_data import MeasurementData
from app.models.substation_flow import SubstationFlowModel

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
        Station, WithdrawsFrom.power_plant_source_id == Station.id
    ).filter(
        WithdrawsFrom.timestamp >= from_date,
        WithdrawsFrom.timestamp <= to_date,
        WithdrawsFrom.power_plant_source_id != None
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
            Station.name.label("power_plant_source"),
            extract("year", WithdrawsFrom.timestamp).label("year"),
            extract("month", WithdrawsFrom.timestamp).label("month"),
            Customer.name.label("customer_name"),
            func.sum(WithdrawsFrom.value_kwh).label("total_kwh")
        )
        .join(Customer, WithdrawsFrom.customer_id == Customer.id)
        .join(Station, WithdrawsFrom.power_plant_source_id == Station.id)
        .filter(
            WithdrawsFrom.timestamp >= from_date,
            WithdrawsFrom.timestamp <= to_date
        )
        .group_by(
            Station.name,
            Customer.name,
            extract("year", WithdrawsFrom.timestamp),
            extract("month", WithdrawsFrom.timestamp)
        )
        .order_by(
            Station.name.asc(),
            extract("month", WithdrawsFrom.timestamp).asc(),
            Customer.name.asc()
        )
        .all()
    )

    return [
        MonthlyCompanyUsageModel(
            power_plant_source=row.power_plant_source,
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

    station_map = {s.name: s.id for s in db.query(Station).all()}
    customer_map = {c.name: c.id for c in db.query(Customer).all()}

    def build_insert_objects(rows):
        productions = []
        injections = []
        withdrawals = []

        for row in rows:
            if row.tegund_maelingar == "Framleiðsla":
                pp_id = station_map.get(row.eining_heiti)
                if pp_id:
                    productions.append(Production(
                        power_plant_id=pp_id,
                        timestamp=row.timi,
                        value_kwh=row.gildi_kwh
                    ))
            elif row.tegund_maelingar == "Innmötun":
                pp_id = station_map.get(row.eining_heiti)
                sub_id = station_map.get(row.sendandi_maelingar)
                if pp_id and sub_id:
                    injections.append(InjectsTo(
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
                    withdrawals.append(WithdrawsFrom(
                        customer_id=customer_id,
                        substation_id=sub_id,
                        timestamp=row.timi,
                        value_kwh=row.gildi_kwh
                    ))

        return productions, injections, withdrawals

    productions, injections, withdrawals = build_insert_objects(parsed_rows)

    from sqlalchemy.dialects.postgresql import insert

    try:
        if mode == "single" or mode == "bulk":
            if productions:
                db.execute(
                    insert(Production).values([
                        {"power_plant_id": obj.power_plant_id, 
                        "timestamp": obj.timestamp, 
                        "value_kwh": obj.value_kwh}
                        for obj in productions
                    ]).on_conflict_do_nothing()
                )
                db.commit()

            if injections:
                db.execute(
                    insert(InjectsTo).values([
                        {"power_plant_id": obj.power_plant_id,
                        "production_timestamp": obj.production_timestamp,
                        "substation_id": obj.substation_id,
                        "timestamp": obj.timestamp,
                        "value_kwh": obj.value_kwh}
                        for obj in injections
                    ]).on_conflict_do_nothing()
                )
                db.commit()

            if withdrawals:
                db.execute(
                    insert(WithdrawsFrom).values([
                        {"customer_id": obj.customer_id,
                        "substation_id": obj.substation_id,
                        "timestamp": obj.timestamp,
                        "value_kwh": obj.value_kwh}
                        for obj in withdrawals
                    ]).on_conflict_do_nothing()
                )
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
    

# Task F
def get_substation_flow_data(
    from_date: datetime,
    to_date: datetime,
    db: Session
):
    # Get substation IDs by name
    s1 = db.query(Station).filter(Station.name == 'S1_Krókur').first()
    s2 = db.query(Station).filter(Station.name == 'S2_Rimakot').first()
    s3 = db.query(Station).filter(Station.name == 'S3_Vestmannaeyjar').first()

    # Get total injection at S1
    s1_injection = db.query(
        func.sum(InjectsTo.value_kwh)
    ).filter(
        InjectsTo.substation_id == s1.id,
        InjectsTo.timestamp >= from_date,
        InjectsTo.timestamp < to_date
    ).scalar() or 0

    # Get total injection at S2
    s2_injection = db.query(
        func.sum(InjectsTo.value_kwh)
    ).filter(
        InjectsTo.substation_id == s2.id,
        InjectsTo.timestamp >= from_date,
        InjectsTo.timestamp < to_date
    ).scalar() or 0

    # Get total withdrawal at S3
    s3_withdrawal = db.query(
        func.sum(WithdrawsFrom.value_kwh)
    ).filter(
        WithdrawsFrom.substation_id == s3.id,
        WithdrawsFrom.timestamp >= from_date,
        WithdrawsFrom.timestamp < to_date
    ).scalar() or 0

    # Get distances from connects_to
    conn_s1_s2 = db.query(ConnectsTo).filter(
        ConnectsTo.from_substation_id == s1.id,
        ConnectsTo.to_substation_id == s2.id
    ).first()

    conn_s2_s3 = db.query(ConnectsTo).filter(
        ConnectsTo.from_substation_id == s2.id,
        ConnectsTo.to_substation_id == s3.id
    ).first()

    d_s1_s2 = conn_s1_s2.distance if conn_s1_s2 else 0
    d_s2_s3 = conn_s2_s3.distance if conn_s2_s3 else 0
    total_distance = d_s1_s2 + d_s2_s3

    s1_injection = float(s1_injection or 0)
    s2_injection = float(s2_injection or 0)
    s3_withdrawal = float(s3_withdrawal or 0)

    # Calculate flows using conservation of energy
    total_system_loss = (s1_injection + s2_injection) - s3_withdrawal
    loss_s1_s2 = total_system_loss * (d_s1_s2 / total_distance) if total_distance else 0
    loss_s2_s3 = total_system_loss * (d_s2_s3 / total_distance) if total_distance else 0

    flow_out_s1_s2 = s1_injection - loss_s1_s2
    flow_in_s2_s3 = flow_out_s1_s2 + s2_injection

    # Build results
    results = [
        SubstationFlowModel(
            segment="S1_Krókur -> S2_Rimakot",
            distance_km=round(d_s1_s2, 2),
            flow_in_kwh=round(float(s1_injection), 2),
            loss_kwh=round(float(loss_s1_s2), 2),
            flow_out_kwh=round(float(flow_out_s1_s2), 2),
            loss_pct=round(float(loss_s1_s2 / s1_injection * 100) if s1_injection else 0, 4),
            efficiency_pct=round(float(flow_out_s1_s2 / s1_injection * 100) if s1_injection else 0, 4),
            max_capacity_mw=round(float(conn_s1_s2.max_capacity_mw or 0.0), 2)
        ),
        SubstationFlowModel(
            segment="S2_Rimakot -> S3_Vestmannaeyjar",
            distance_km=round(d_s2_s3, 2),
            flow_in_kwh=round(float(flow_in_s2_s3), 2),
            loss_kwh=round(float(loss_s2_s3), 2),
            flow_out_kwh=round(float(s3_withdrawal), 2),
            loss_pct=round(float(loss_s2_s3 / flow_in_s2_s3 * 100) if flow_in_s2_s3 else 0, 4),
            efficiency_pct=round(float(s3_withdrawal / flow_in_s2_s3 * 100) if flow_in_s2_s3 else 0, 4),
            max_capacity_mw=round(float(conn_s2_s3.max_capacity_mw or 0.0), 2)
        ),
        SubstationFlowModel(
            segment="TOTAL SYSTEM",
            distance_km=round(total_distance, 2),
            flow_in_kwh=round(float(s1_injection + s2_injection), 2),
            loss_kwh=round(float(total_system_loss), 2),
            flow_out_kwh=round(float(s3_withdrawal), 2),
            loss_pct=round(float(total_system_loss / (s1_injection + s2_injection) * 100) if (s1_injection + s2_injection) else 0, 4),
            efficiency_pct=round(float(s3_withdrawal / (s1_injection + s2_injection) * 100) if (s1_injection + s2_injection) else 0, 4),
            max_capacity_mw=round(float((conn_s1_s2.max_capacity_mw or 0.0) + (conn_s2_s3.max_capacity_mw or 0.0)), 2)
        )
    ]

    return results