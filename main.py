import os
from datetime import datetime, timezone

import psycopg2
from psycopg2.extras import Json
from fastapi import FastAPI, HTTPException, Request


app = FastAPI(title="Walchem Fluent Webhook")

DATABASE_URL = os.getenv("DATABASE_URL")


def get_connection():
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL is not configured")

    return psycopg2.connect(DATABASE_URL)


@app.get("/")
def root():
    return {
        "status": "Walchem webhook is running",
        "time": datetime.now(timezone.utc).isoformat()
    }


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/walchem-webhook")
async def walchem_webhook(request: Request):
    try:
        payload = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON")

    controller_id = (
        payload.get("controllerId")
        or payload.get("controller_id")
        or payload.get("id")
    )

    controller_name = (
        payload.get("controllerName")
        or payload.get("controller_name")
        or payload.get("name")
    )

    serial_number = (
        payload.get("serialNumber")
        or payload.get("serial-number")
        or payload.get("serial_number")
    )

    try:
        connection = get_connection()

        with connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO walchem_readings (
                        controller_id,
                        controller_name,
                        serial_number,
                        payload,
                        received_at
                    )
                    VALUES (%s, %s, %s, %s, %s)
                    """,
                    (
                        controller_id,
                        controller_name,
                        serial_number,
                        Json(payload),
                        datetime.now(timezone.utc)
                    )
                )

        connection.close()

    except Exception as error:
        print(f"Database error: {error}")
        raise HTTPException(
            status_code=500,
            detail="Webhook received, but database storage failed"
        )

    return {
        "received": True,
        "serial_number": serial_number
    }
