from fastapi import APIRouter
from fastapi.responses import Response

router = APIRouter(tags=["system"])


@router.get("/health")
def health() -> dict:
    return {"status": "ok"}


@router.get("/", include_in_schema=False)
def root() -> dict:
    return {"message": "stockProject FastAPI is running"}


@router.get("/favicon.ico", include_in_schema=False)
def favicon() -> Response:
    return Response(status_code=204)


