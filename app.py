from fastapi import FastAPI
import asyncio
from data_collection import run_scheduler
from contextlib import asynccontextmanager

@asynccontextmanager
async def lifespan(app: FastAPI):
    print("✅ Data Collection Service Started")
    asyncio.create_task(run_scheduler())
    yield
    print("🛑 Service Stopped")

app = FastAPI(lifespan=lifespan)

@app.get("/")
async def home():
    return {"message": "Crypto Data Collection System Running 🚀"}
