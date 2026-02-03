from pydantic import BaseModel

class PriceResponse(BaseModel):
    item: str
    price: str
    source: str
    status: str