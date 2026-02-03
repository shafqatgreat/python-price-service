from fastapi import FastAPI
from api.routes.prices import router as price_router

app = FastAPI()

# This makes the root / work so you don't get 404
@app.get("/")
async def root():
    return {"message": "Python Price Service is Live"}

app.include_router(price_router, prefix="/api")