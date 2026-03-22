from fastapi import FastAPI
from app.routes.routes import router as OrkuFlaediIsland_routes

# Declare app 
app = FastAPI()

app.include_router(OrkuFlaediIsland_routes)
