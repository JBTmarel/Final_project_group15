from typing import List
from fastapi import HTTPException, UploadFile

def validate_file_type(
    file: UploadFile,
    allowed_extensions: List[str],
):
    filename = file.filename.lower()
    extensions = [ext.lower() for ext in allowed_extensions]

    if not any(filename.endswith(ext) for ext in extensions):
        raise HTTPException(
            status_code=400,
            detail={"message": f"Ógilt skráar tegund. Leyfðar tegundir eru: {', '.join(extensions)}"}
        )