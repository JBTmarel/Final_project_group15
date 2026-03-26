from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

# --- CONFIGURATION ---
USER = "postgres"
PASSWORD = "221103"
PORT = "5432"
HOST = "localhost"
DATABASE_NAME = "OrkuflaediIsland"

# SCHEMA NAMES - Change these to match your actual Postgres Schemas
LEGACY_SCHEMA = "raforka_legacy"
NEW_SCHEMA = "raforka_updated" 

DATABASE_URL = f"postgresql://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE_NAME}"
engine = create_engine(DATABASE_URL)

def run_step(conn, title, sql):
    print(f"Running {title}...")
    try:
        conn.execute(text(sql))
        print(f"{title} complete.")
    except Exception as e:
        print(f" Error in {title}: {e}")
        raise e

def migrate():
    with engine.begin() as conn:  # Handles TRANSACTION (Commit/Rollback)
        try:
            print("--- STARTING MIGRATION ---")

            # OPTIONAL: Clear new tables before migrating (CAREFUL: DELETES DATA IN NEW_SCHEMA)
            # run_step(conn, "Cleanup", f"TRUNCATE {NEW_SCHEMA}.production, {NEW_SCHEMA}.injection, {NEW_SCHEMA}.withdrawal, {NEW_SCHEMA}.plant_connection, {NEW_SCHEMA}.substation_connection, {NEW_SCHEMA}.power_plant, {NEW_SCHEMA}.substation, {NEW_SCHEMA}.customer, {NEW_SCHEMA}.station, {NEW_SCHEMA}.owner RESTART IDENTITY CASCADE;")

            # Step 1: Owners
            run_step(conn, "Step 1: Owners", f"""
                INSERT INTO {NEW_SCHEMA}.owner (name)
                SELECT DISTINCT eigandi FROM {LEGACY_SCHEMA}.orku_einingar WHERE eigandi IS NOT NULL
                UNION
                SELECT DISTINCT eigandi FROM {LEGACY_SCHEMA}.notendur_skraning WHERE eigandi IS NOT NULL
                ON CONFLICT (name) DO NOTHING;
            """)

            # Step 2: Stations
            run_step(conn, "Step 2: Base Stations", f"""
                INSERT INTO {NEW_SCHEMA}.station 
                    (name, type, station_type, installed_date, owner_id, x_coordinate, y_coordinate)
                SELECT
                    oe.heiti, oe.tegund, oe.tegund_stod,
                    MAKE_DATE(CAST(oe.ar_uppsett AS INT), CAST(oe.manudir_uppsett AS INT), CAST(oe.dagur_uppsett AS INT)),
                    o.id, oe."X_HNIT", oe."Y_HNIT"
                FROM {LEGACY_SCHEMA}.orku_einingar oe
                JOIN {NEW_SCHEMA}.owner o ON oe.eigandi = o.name;
            """)

            # Step 3 & 4: Sub-types (Specializing the Stations)
            run_step(conn, "Step 3: Power Plants", f"""
                INSERT INTO {NEW_SCHEMA}.power_plant (power_plant_id)
                SELECT s.id FROM {NEW_SCHEMA}.station s
                JOIN {LEGACY_SCHEMA}.orku_einingar oe ON s.name = oe.heiti
                WHERE oe.tegund = 'virkjun';
            """)

            run_step(conn, "Step 4: Substations", f"""
                INSERT INTO {NEW_SCHEMA}.substation (substation_id)
                SELECT s.id FROM {NEW_SCHEMA}.station s
                JOIN {LEGACY_SCHEMA}.orku_einingar oe ON s.name = oe.heiti
                WHERE oe.tegund = 'stod';
            """)

            # Step 5: Customers
            run_step(conn, "Step 5: Customers", f"""
                INSERT INTO {NEW_SCHEMA}.customer (name, ssn, founded_year, owner_id, x_coordinate, y_coordinate)
                SELECT ns.heiti, ns.kennitala, ns.ar_stofnad, o.id, ns."X_HNIT", ns."Y_HNIT"
                FROM {LEGACY_SCHEMA}.notendur_skraning ns
                JOIN {NEW_SCHEMA}.owner o ON ns.eigandi = o.name;
            """)

            # Step 6: Plant to Substation Connections
            run_step(conn, "Step 6: Plant Connections", f"""
                INSERT INTO {NEW_SCHEMA}.plant_connection (power_plant_id, substation_id)
                SELECT 
                    pp.power_plant_id, 
                    sub.substation_id
                FROM {LEGACY_SCHEMA}.orku_einingar oe
                JOIN {NEW_SCHEMA}.station s_p ON oe.heiti = s_p.name
                JOIN {NEW_SCHEMA}.power_plant pp ON s_p.id = pp.power_plant_id
                JOIN {NEW_SCHEMA}.station s_s ON oe.tengd_stod = s_s.name
                JOIN {NEW_SCHEMA}.substation sub ON s_s.id = sub.substation_id
                WHERE oe.tegund = 'virkjun' AND oe.tengd_stod IS NOT NULL;
            """)

            # Step 7: Substation to Substation Connections (Transmission Lines)
            run_step(conn, "Step 7: Substation Connections", f"""
                INSERT INTO {NEW_SCHEMA}.substation_connection (from_substation_id, to_substation_id)
                SELECT 
                    s_from.substation_id, 
                    s_to.substation_id
                FROM {LEGACY_SCHEMA}.orku_einingar oe
                JOIN {NEW_SCHEMA}.station st_from ON oe.heiti = st_from.name
                JOIN {NEW_SCHEMA}.substation s_from ON st_from.id = s_from.substation_id
                JOIN {NEW_SCHEMA}.station st_to ON oe.tengd_stod = st_to.name
                JOIN {NEW_SCHEMA}.substation s_to ON st_to.id = s_to.substation_id
                WHERE oe.tegund = 'stod' AND oe.tengd_stod IS NOT NULL;
            """)

            # Step 8: Production Measurements
            run_step(conn, "Step 8: Production Data", f"""
                INSERT INTO {NEW_SCHEMA}.production (power_plant_id, timestamp, value_kwh)
                SELECT pp.power_plant_id, om.timi, om.gildi_kwh
                FROM {LEGACY_SCHEMA}.orku_maelingar om
                JOIN {NEW_SCHEMA}.station s ON om.eining_heiti = s.name
                JOIN {NEW_SCHEMA}.power_plant pp ON s.id = pp.power_plant_id
                WHERE om.tegund_maelingar = 'Framleiðsla';
            """)

            # Step 9: Injection Measurements
            run_step(conn, "Step 9: Injection Data", f"""
                INSERT INTO {NEW_SCHEMA}.injection (power_plant_id, substation_id, timestamp, value_kwh)
                SELECT pp.power_plant_id, sub.substation_id, om.timi, om.gildi_kwh
                FROM {LEGACY_SCHEMA}.orku_maelingar om
                JOIN {NEW_SCHEMA}.station s_p ON om.eining_heiti = s_p.name
                JOIN {NEW_SCHEMA}.power_plant pp ON s_p.id = pp.power_plant_id
                JOIN {NEW_SCHEMA}.station s_s ON om.sendandi_maelingar = s_s.name
                JOIN {NEW_SCHEMA}.substation sub ON s_s.id = sub.substation_id
                WHERE om.tegund_maelingar = 'Innmötun';
            """)

            # Step 10: Withdrawal Measurements
            run_step(conn, "Step 10: Withdrawal Data", f"""
                INSERT INTO {NEW_SCHEMA}.withdrawal (customer_id, timestamp, value_kwh)
                SELECT c.id, om.timi, om.gildi_kwh
                FROM {LEGACY_SCHEMA}.orku_maelingar om
                JOIN {NEW_SCHEMA}.customer c ON om.notandi_heiti = c.name
                WHERE om.tegund_maelingar = 'Úttekt';
            """)

            print("\n MIGRATION FINISHED SUCCESSFULLY!")

        except SQLAlchemyError as e:
            print(f"\n DATABASE ERROR: {e}")
        except Exception as e:
            print(f"\n GENERAL ERROR: {e}")

if __name__ == "__main__":
    migrate()