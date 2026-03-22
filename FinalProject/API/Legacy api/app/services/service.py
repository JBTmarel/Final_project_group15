from fastapi import UploadFile, HTTPException
from sqlalchemy.orm import Session
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
from sqlalchemy import func



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
from sqlalchemy import text

def get_monthly_energy_flow_data(db, from_date, to_date):
    query = text("""
        SELECT
            eining_heiti AS power_plant_source,
            EXTRACT(YEAR FROM timi) AS year,
            EXTRACT(MONTH FROM timi) AS month,
            tegund_maelingar AS measurement_type,
            SUM(gildi_kwh) AS total_kwh
        FROM raforka_legacy.orku_maelingar
        WHERE timi >= :from_date
          AND timi < :to_date
        GROUP BY
            eining_heiti,
            EXTRACT(YEAR FROM timi),
            EXTRACT(MONTH FROM timi),
            tegund_maelingar
        ORDER BY
            power_plant_source,
            month ASC,
            total_kwh DESC
    """)

    result = db.execute(query, {"from_date": from_date, "to_date": to_date})

    return [
        {
            "power_plant_source": row.power_plant_source,
            "year": int(row.year),
            "month": int(row.month),
            "measurement_type": row.measurement_type,
            "total_kwh": float(row.total_kwh),
        }
        for row in result
    ]

'''
Service 2: get_monthly_company_usage_data()
'''
def get_monthly_customer_usage_data(db, from_date, to_date):
    query = text("""
        SELECT 
            eining_heiti AS power_plant_source,
            EXTRACT(YEAR FROM timi) AS year,
            EXTRACT(MONTH FROM timi) AS month,
            notandi_heiti AS customer_name,
            SUM(gildi_kwh) AS total_kwh
        FROM raforka_legacy.orku_maelingar
        WHERE tegund_maelingar = 'Úttekt'
          AND timi >= :from_date
          AND timi < :to_date
        GROUP BY
            eining_heiti,
            EXTRACT(YEAR FROM timi),
            EXTRACT(MONTH FROM timi),
            notandi_heiti
        ORDER BY
            power_plant_source,
            month ASC,
            customer_name ASC
    """)

    result = db.execute(query, {
        "from_date": from_date,
        "to_date": to_date
    })

    return [
        {
            "power_plant_source": row.power_plant_source,
            "year": int(row.year),
            "month": int(row.month),
            "customer_name": row.customer_name,
            "total_kwh": float(row.total_kwh) if row.total_kwh is not None else 0.0
        }
        for row in result
    ]

'''
Service 3: get_monthly_plant_loss_ratios_data()
'''
def get_monthly_plant_loss_ratios_data(db, from_date, to_date):
    create_view_query = text("""
        CREATE OR REPLACE VIEW monthly_plant_totals AS
        SELECT
            eining_heiti AS power_plant_source,
            EXTRACT(YEAR FROM timi) AS year,
            EXTRACT(MONTH FROM timi) AS month,
            tegund_maelingar,
            SUM(gildi_kwh) AS total_kwh
        FROM raforka_legacy.orku_maelingar
        GROUP BY
            eining_heiti,
            EXTRACT(YEAR FROM timi),
            EXTRACT(MONTH FROM timi),
            tegund_maelingar
    """)

    db.execute(create_view_query)
    db.commit()

    query = text("""
        SELECT
            power_plant_source,
            AVG((framleidsla - innmotun) / NULLIF(framleidsla, 0)) AS plant_to_substation_loss_ratio,
            AVG((framleidsla - uttekt) / NULLIF(framleidsla, 0)) AS total_system_loss_ratio
        FROM (
            SELECT
                power_plant_source,
                year,
                month,
                SUM(CASE WHEN tegund_maelingar = 'Framleiðsla' THEN total_kwh END) AS framleidsla,
                SUM(CASE WHEN tegund_maelingar = 'Innmötun' THEN total_kwh END) AS innmotun,
                SUM(CASE WHEN tegund_maelingar = 'Úttekt' THEN total_kwh END) AS uttekt
            FROM monthly_plant_totals
            WHERE make_date(year::int, month::int, 1) >= date_trunc('month', :from_date)
              AND make_date(year::int, month::int, 1) < date_trunc('month', :to_date)
            GROUP BY power_plant_source, year, month
        ) AS pivoted
        GROUP BY power_plant_source
        ORDER BY power_plant_source
    """)

    result = db.execute(query, {
        "from_date": from_date,
        "to_date": to_date
    })

    return [
        {
            "power_plant_source": row.power_plant_source,
            "plant_to_substation_loss_ratio": float(row.plant_to_substation_loss_ratio) if row.plant_to_substation_loss_ratio is not None else None,
            "total_system_loss_ratio": float(row.total_system_loss_ratio) if row.total_system_loss_ratio is not None else None
        }
        for row in result
    ]