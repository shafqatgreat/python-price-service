from fastapi import APIRouter, HTTPException
from api.services.scraper import scrape_online_price
from api.schemas.price import PriceResponse
from api.services.helper.carrefour_bs import run_carrefour_scraper

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


@router.get("/scrape")
async def scrape_endpoint(category: str = "Fruits", subcategory: str = "Fresh"):
    # Target URL (Adjust as needed)
    target_url = "https://www.carrefour.pk/mafpak/en/c/FPAK1660000"
    
    try:
        data = await run_carrefour_scraper(target_url);
        return {"status": "success", "total_items": len(data), "data": data}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
