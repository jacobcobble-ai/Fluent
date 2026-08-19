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


def extract_serial_number(payload: dict):
    return (
        payload.get("serial-number")
        or payload.get("serialNumber")
        or payload.get("serial_number")
    )


def extract_controller_id(payload: dict):
    return (
        payload.get("controllerId")
        or payload.get("controller_id")
        or payload.get("id")
    )


def extract_controller_name(payload: dict):
    return (
        payload.get("controllerName")
        or payload.get("controller_name")
        or payload.get("name")
    )


def save_reading_payload(payload: dict):
    serial_number = extract_serial_number(payload)
    controller_id = extract_controller_id(payload)
    controller_name = extract_controller_name(payload)

    connection = None

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
                        datetime.now(timezone.utc),
                    ),
                )

    finally:
        if connection is not None:
            connection.close()

    return serial_number


async def process_reading_request(request: Request):
    try:
        payload = await request.json()
    except Exception:
        raise HTTPException(
            status_code=400,
            detail="Invalid JSON",
        )

    if not isinstance(payload, dict):
        raise HTTPException(
            status_code=400,
            detail="The JSON payload must be an object",
        )

    serial_number = extract_serial_number(payload)

    try:
        save_reading_payload(payload)

    except Exception as error:
        print(f"Database error: {error}")

        raise HTTPException(
            status_code=500,
            detail="Webhook received, but database storage failed",
        )

    print(
        "Fluent reading received:",
        {
            "serial_number": serial_number,
            "subscription_id": payload.get("subscription-id"),
            "reading_count": len(payload.get("readings") or []),
        },
    )

    return {
        "received": True,
        "serial_number": serial_number,
    }


@app.get("/")
def root():
    return {
        "status": "Walchem webhook is running",
        "time": datetime.now(timezone.utc).isoformat(),
    }


@app.get("/health")
def health():
    return {
        "status": "ok",
        "database_configured": bool(DATABASE_URL),
    }


@app.post("/fluent/readings")
async def fluent_readings(request: Request):
    return await process_reading_request(request)


@app.post("/walchem-webhook")
async def walchem_webhook(request: Request):
    return await process_reading_request(request)
