from fastapi import APIRouter, HTTPException
from app.controller.hp_controller import HPController
from app.models.hp_model import HPInterfaceList

router = APIRouter(prefix="/api/v1/hp", tags=["HP Switch"])
hp_controller = HPController()


@router.get("/interfaces", response_model=HPInterfaceList)
async def get_interfaces():
    try:
        return await hp_controller.get_interfaces()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
