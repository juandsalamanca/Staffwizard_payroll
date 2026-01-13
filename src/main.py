from fastapi import FastAPI
from contextlib import asynccontextmanager

app = FastAPI()

@app.get("/health")
async def health():
    return {"message": "Server is OK"}
