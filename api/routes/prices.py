from fastapi import APIRouter
from api.services.scraper import scrape_online_price
from api.schemas.price import PriceResponse

router = APIRouter()

@router.get("/get-price", response_model=PriceResponse)
async def get_item_price(url: str, item_name: str = "Unknown"):
    data = scrape_online_price(url)
    return {
        "item": item_name,
        "price": data["price"],
        "source": data["source"],
        "status": data["status"]
    }