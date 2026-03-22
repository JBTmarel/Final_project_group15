from fastapi import APIRouter
from app.routes.endpoints import router as Legacy_router

router = APIRouter(prefix="/OrkuFlaediIsland")

router.include_router(Legacy_router)