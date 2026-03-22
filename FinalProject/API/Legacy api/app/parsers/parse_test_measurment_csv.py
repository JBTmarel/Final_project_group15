import csv
from io import StringIO
from datetime import datetime
from typing import List
from app.models.parsed_data.test_measurement_data import TestMeasurementData 

def parse_test_measurement_csv(
    raw_text: str
)   -> List[TestMeasurementData]:
    
    rows = []
    reader = csv.DictReader(StringIO(raw_text))

    for index, row in enumerate(reader, start=1):
        try:
            rows.append(
                TestMeasurementData(
                    id=index,
                    timi=datetime.fromisoformat(row["timi"]),
                    value=float(row["value"])
                )
            )
        except Exception:
            continue

    return rows