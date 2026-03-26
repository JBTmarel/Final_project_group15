# Task C4

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

USER = "postgres"
PASSWORD = "221103"
PORT = "5432"
DATABASE_NAME = "OrkuflaediIsland"

# Database connection string
DATABASE_URL = f"postgresql://{USER}:{PASSWORD}@localhost:{PORT}/{DATABASE_NAME}"

# Create the SQLAlchemy engine (handles the actual DB connection pool)
engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True
)

def migrate():
    with engine.connect() as conn:
        try:
            # Step 1: Migrate owners
            print("Step 1: Migrating owners...")
            conn.execute(text("""
                INSERT INTO raforka_updated.owner (name)
                SELECT DISTINCT eigandi FROM raforka_legacy.orku_einingar
                WHERE eigandi IS NOT NULL
                UNION
                SELECT DISTINCT eigandi FROM raforka_legacy.notendur_skraning
                WHERE eigandi IS NOT NULL
            """))
            conn.commit()
            print("Step 1 ✅")

            # Step 2: Migrate stations
            print("Step 2: Migrating stations...")
            conn.execute(text("""
                INSERT INTO raforka_updated.station 
                    (name, type, station_type, installed_date, owner_id, X_COORDINATE, Y_COORDINATE)
                SELECT
                    oe.heiti,
                    oe.tegund,
                    oe.tegund_stod,
                    MAKE_DATE(oe.ar_uppsett, oe.manudir_uppsett, oe.dagur_uppsett),
                    o.id,
                    oe."X_HNIT",
                    oe."Y_HNIT"
                FROM raforka_legacy.orku_einingar oe
                JOIN raforka_updated.owner o ON oe.eigandi = o.name
            """))
            conn.commit()
            print("Step 2 ✅")

            # Step 3: Migrate power plants
            print("Step 3: Migrating power plants...")
            conn.execute(text("""
                INSERT INTO raforka_updated.power_plant (power_plant_id)
                SELECT s.id
                FROM raforka_updated.station s
                JOIN raforka_legacy.orku_einingar oe ON s.name = oe.heiti
                WHERE oe.tegund = 'virkjun'
            """))
            conn.commit()
            print("Step 3 ✅")

            # Step 4: Migrate substations
            print("Step 4: Migrating substations...")
            conn.execute(text("""
                INSERT INTO raforka_updated.substation (substation_id)
                SELECT s.id
                FROM raforka_updated.station s
                JOIN raforka_legacy.orku_einingar oe ON s.name = oe.heiti
                WHERE oe.tegund = 'stod'
            """))
            conn.commit()
            print("Step 4 ✅")

            # Step 5: Migrate customers
            print("Step 5: Migrating customers...")
            conn.execute(text("""
                INSERT INTO raforka_updated.customer 
                    (name, SSN, founded_year, owner_id, X_HNIT, Y_HNIT)
                SELECT
                    ns.heiti,
                    ns.kennitala,
                    ns.ar_stofnad,
                    o.id,
                    ns."X_HNIT",
                    ns."Y_HNIT"
                FROM raforka_legacy.notendur_skraning ns
                JOIN raforka_updated.owner o ON ns.eigandi = o.name
            """))
            conn.commit()
            print("Step 5 ✅")

            # Step 6: Migrate plant connections
            print("Step 6: Migrating plant connections...")
            conn.execute(text("""
                INSERT INTO raforka_updated.plant_connection 
                    (power_plant_id, substation_id)
                SELECT
                    pp.power_plant_id,
                    sub.substation_id
                FROM raforka_legacy.orku_einingar oe
                JOIN raforka_updated.station s_plant ON oe.heiti = s_plant.name
                JOIN raforka_updated.power_plant pp ON s_plant.id = pp.power_plant_id
                JOIN raforka_updated.station s_sub ON oe.tengd_stod = s_sub.name
                JOIN raforka_updated.substation sub ON s_sub.id = sub.substation_id
                WHERE oe.tegund = 'virkjun'
                AND oe.tengd_stod IS NOT NULL
            """))
            conn.commit()
            print("Step 6 ✅")

            # Step 7: Migrate substation connections
            print("Step 7: Migrating substation connections...")
            conn.execute(text("""
                INSERT INTO raforka_updated.substation_connection 
                    (from_substation_id, to_substation_id)
                SELECT
                    s_from.substation_id,
                    s_to.substation_id
                FROM raforka_legacy.orku_einingar oe
                JOIN raforka_updated.station st_from ON oe.heiti = st_from.name
                JOIN raforka_updated.substation s_from ON st_from.id = s_from.substation_id
                JOIN raforka_updated.station st_to ON oe.tengd_stod = st_to.name
                JOIN raforka_updated.substation s_to ON st_to.id = s_to.substation_id
                WHERE oe.tegund = 'stod'
                AND oe.tengd_stod IS NOT NULL
            """))
            conn.commit()
            print("Step 7 ✅")

            # Step 8: Migrate production (Framleiðsla)
            print("Step 8: Migrating production...")
            conn.execute(text("""
                INSERT INTO raforka_updated.production 
                    (power_plant_id, timestamp, value_kwh)
                SELECT
                    pp.power_plant_id,
                    om.timi,
                    om.gildi_kwh
                FROM raforka_legacy.orku_maelingar om
                JOIN raforka_updated.station s ON om.eining_heiti = s.name
                JOIN raforka_updated.power_plant pp ON s.id = pp.power_plant_id
                WHERE om.tegund_maelingar = 'Framleiðsla'
            """))
            conn.commit()
            print("Step 8 ✅")

            # Step 9: Migrate injection (Innmötun)
            print("Step 9: Migrating injection...")
            conn.execute(text("""
                INSERT INTO raforka_updated.injection 
                    (power_plant_id, substation_id, timestamp, value_kwh)
                SELECT
                    pp.power_plant_id,
                    sub.substation_id,
                    om.timi,
                    om.gildi_kwh
                FROM raforka_legacy.orku_maelingar om
                JOIN raforka_updated.station s_plant ON om.eining_heiti = s_plant.name
                JOIN raforka_updated.power_plant pp ON s_plant.id = pp.power_plant_id
                JOIN raforka_updated.station s_sub ON om.sendandi_maelingar = s_sub.name
                JOIN raforka_updated.substation sub ON s_sub.id = sub.substation_id
                WHERE om.tegund_maelingar = 'Innmötun'
            """))
            conn.commit()
            print("Step 9 ✅")

            # Step 10: Migrate withdrawal (Úttekt)
            print("Step 10: Migrating withdrawal...")
            conn.execute(text("""
                INSERT INTO raforka_updated.withdrawal 
                    (customer_id, timestamp, value_kwh)
                SELECT
                    c.id,
                    om.timi,
                    om.gildi_kwh
                FROM raforka_legacy.orku_maelingar om
                JOIN raforka_updated.customer c ON om.notandi_heiti = c.name
                WHERE om.tegund_maelingar = 'Úttekt'
            """))
            conn.commit()
            print("Step 10 ✅")

            print("\n✅ Migration completed successfully!")

        except Exception as e:
            conn.rollback()
            print(f"\n❌ Migration failed at: {e}")

if __name__ == "__main__":
    migrate()