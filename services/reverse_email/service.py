from fastapi import FastAPI

service = FastAPI()

@service.get("/health")
def health():
    return {"status": "ok"}
