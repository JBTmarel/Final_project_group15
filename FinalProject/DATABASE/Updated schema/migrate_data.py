from sqlalchemy import create_engine, text
import math

# --- CONFIGURATION ---
USER = "postgres"
PASSWORD = "221103"  # Replace with your actual password
PORT = "5432"
HOST = "localhost"
DATABASE_NAME = "OrkuflaediIsland"

LEGACY_SCHEMA = "raforka_legacy"
NEW_SCHEMA = "raforka_updated"

DATABASE_URL = f"postgresql://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE_NAME}"
engine = create_engine(DATABASE_URL)

def run_step(conn, title, sql):
    print(f"Running {title}...")
    try:
        conn.execute(text(sql))
        print(f" {title} complete.")
    except Exception as e:
        print(f" Error in {title}: {e}")
        raise e

def migrate():
    with engine.begin() as conn:
        print("--- STARTING MIGRATION ---")

        # Step 1: Owners
        run_step(conn, "Step 1: Owners", f"""
            INSERT INTO {NEW_SCHEMA}.owner (name)
            SELECT DISTINCT eigandi FROM {LEGACY_SCHEMA}.orku_einingar WHERE eigandi IS NOT NULL
            UNION
            SELECT DISTINCT eigandi FROM {LEGACY_SCHEMA}.notendur_skraning WHERE eigandi IS NOT NULL
            ON CONFLICT (name) DO NOTHING;
        """)

        # Step 2: Stations
        run_step(conn, "Step 2: Stations", f"""
            INSERT INTO {NEW_SCHEMA}.station (name, type, station_type, installed_date, owner_id, x_coordinates, y_coordinates)
            SELECT 
                oe.heiti, oe.tegund, oe.tegund_stod,
                CASE 
                    WHEN oe.ar_uppsett IS NOT NULL THEN MAKE_DATE(CAST(oe.ar_uppsett AS INT), CAST(oe.manudur_uppsett AS INT), CAST(oe.dagur_uppsett AS INT)) 
                    ELSE NULL 
                END,
                o.id, oe."X_HNIT", oe."Y_HNIT"
            FROM {LEGACY_SCHEMA}.orku_einingar oe
            JOIN {NEW_SCHEMA}.owner o ON oe.eigandi = o.name;
        """)

        # Step 3: Specializations
        run_step(conn, "Step 3: Plants & Substations", f"""
            INSERT INTO {NEW_SCHEMA}.power_plant (power_plant_id)
            SELECT id FROM {NEW_SCHEMA}.station WHERE type = 'virkjun';
            
            INSERT INTO {NEW_SCHEMA}.substation (substation_id)
            SELECT id FROM {NEW_SCHEMA}.station WHERE type = 'stod';
        """)

        # Step 4: Customers
        run_step(conn, "Step 4: Customers", f"""
            INSERT INTO {NEW_SCHEMA}.customer (name, ssn, founded_year, x_coordinates, y_coordinates, owner_id)
            SELECT ns.heiti, ns.kennitala, ns.ar_stofnad, ns."X_HNIT", ns."Y_HNIT", o.id
            FROM {LEGACY_SCHEMA}.notendur_skraning ns
            JOIN {NEW_SCHEMA}.owner o ON ns.eigandi = o.name;
        """)

        # Step 5: Production
        run_step(conn, "Step 5: Production", f"""
            INSERT INTO {NEW_SCHEMA}.production (power_plant_id, timestamp, value_kwh)
            SELECT pp.power_plant_id, om.timi, om.gildi_kwh
            FROM {LEGACY_SCHEMA}.orku_maelingar om
            JOIN {NEW_SCHEMA}.station s ON om.eining_heiti = s.name
            JOIN {NEW_SCHEMA}.power_plant pp ON s.id = pp.power_plant_id
            WHERE om.tegund_maelingar = 'Framleiðsla';
        """)

        # Step 6: Injects_to (Mapping Production to Substation)
        run_step(conn, "Step 6: Injects_to", f"""
            INSERT INTO {NEW_SCHEMA}.injects_to (power_plant_id, production_timestamp, substation_id, value_kwh, timestamp)
            SELECT pp.power_plant_id, om.timi, sub.substation_id, om.gildi_kwh, om.timi
            FROM {LEGACY_SCHEMA}.orku_maelingar om
            JOIN {NEW_SCHEMA}.station s_p ON om.eining_heiti = s_p.name
            JOIN {NEW_SCHEMA}.power_plant pp ON s_p.id = pp.power_plant_id
            JOIN {NEW_SCHEMA}.station s_s ON om.sendandi_maelingar = s_s.name
            JOIN {NEW_SCHEMA}.substation sub ON s_s.id = sub.substation_id
            WHERE om.tegund_maelingar = 'Innmötun';
        """)

        # Step 7: Withdraws_from
        run_step(conn, "Step 7: Withdraws_from", f"""
            INSERT INTO {NEW_SCHEMA}.withdraws_from (customer_id, substation_id, timestamp, value_kwh)
            SELECT c.id, sub.substation_id, om.timi, om.gildi_kwh
            FROM {LEGACY_SCHEMA}.orku_maelingar om
            JOIN {NEW_SCHEMA}.customer c ON om.notandi_heiti = c.name
            JOIN {NEW_SCHEMA}.station s ON om.eining_heiti = s.name
            JOIN {NEW_SCHEMA}.substation sub ON s.id = sub.substation_id
            WHERE om.tegund_maelingar = 'Úttekt';
        """)

        # Step 8: Connects_to (Substation Topology with Distance Calculation)
        run_step(conn, "Step 8: Substation Connections", f"""
            INSERT INTO {NEW_SCHEMA}.connects_to (from_substation_id, to_substation_id, distance)
            SELECT 
                s1.id, s2.id,
                SQRT(POWER(s2.x_coordinates - s1.x_coordinates, 2) + POWER(s2.y_coordinates - s1.y_coordinates, 2))
            FROM {LEGACY_SCHEMA}.orku_einingar oe
            JOIN {NEW_SCHEMA}.station s1 ON oe.heiti = s1.name
            JOIN {NEW_SCHEMA}.station s2 ON oe.tengd_stod = s2.name
            WHERE oe.tegund = 'stod' AND oe.tengd_stod IS NOT NULL;
        """)

        print("\n MIGRATION FINISHED!")

if __name__ == "__main__":
    migrate()