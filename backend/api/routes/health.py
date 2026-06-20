from fastapi import APIRouter, Request

router = APIRouter()


@router.get("/health")
async def health(request: Request):
    db_ok = True
    try:
        from database import engine
        with engine.connect():
            pass
    except Exception:
        db_ok = False

    flink_ok = await request.app.state.job_manager.health()
    return {"status": "ok", "db": db_ok, "flink": flink_ok}
