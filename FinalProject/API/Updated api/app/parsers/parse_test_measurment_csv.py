# Task E1

import csv
from io import StringIO
from datetime import datetime
from typing import List
from app.models.parsed_data.measurement_data import MeasurementData

def parse_measurements_csv(raw_text: str) -> List[MeasurementData]:
    rows = []
    reader = csv.DictReader(StringIO(raw_text))
    for row in reader:
        try:
            rows.append(
                MeasurementData(
                    eining_heiti=row["eining_heiti"],
                    tegund_maelingar=row["tegund_maelingar"],
                    sendandi_maelingar=row["sendandi_maelingar"],
                    timi=datetime.fromisoformat(row["timi"]),
                    gildi_kwh=float(row["gildi_kwh"]),
                    notandi_heiti=row["notandi_heiti"] if row["notandi_heiti"] else None
                )
            )
        except Exception:
            continue
    return rows