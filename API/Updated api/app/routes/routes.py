from fastapi import APIRouter
from app.routes.endpoints import router as Updated_router

router = APIRouter(prefix="/UpdatedOrkuFlaediIsland")

router.include_router(Updated_router)