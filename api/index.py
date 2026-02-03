from fastapi import FastAPI

app = FastAPI()

@app.get("/")
async def root():
    return {"message": "Python Price Service is Live"}

@app.get("/health")
async def health_check():
    return {
        "status": "UP",
        "service": "price-service-python",
        "message": "Basic FastAPI is running on Vercel"
    }

@app.get("/api/hello")
async def hello():
    return {"message": "Hello from Python!"}