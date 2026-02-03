from fastapi import FastAPI
from api.routes import prices

app = FastAPI()

# Include our modular routes
app.include_router(prices.router, prefix="/api")

@app.get("/")
async def root():
    return {"message": "Modular Price Scraper API is Live"}