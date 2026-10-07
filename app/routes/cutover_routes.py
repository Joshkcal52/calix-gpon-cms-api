"""
Cutover Routes (read-only)
Export a network or PON port from CMS in SMx's terms, and plan the SMx
requests that would recreate each subscriber on XGS. Nothing here changes CMS
and nothing is sent to SMx.
"""

import logging
from datetime import datetime, timezone
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Path, Query
from fastapi.responses import PlainTextResponse
from pydantic import BaseModel, ConfigDict

from app.dependencies import get_gateway
from app.legacy.export import (
    PORT_COLUMNS, SERVICE_COLUMNS, CutoverExport, export_network, export_ont, port_rows, service_rows,
    to_csv
)
from app.legacy.smx import SmxMap, SmxPlan, plan_export
from app.models.provisioning_model import NETWORK_PATTERN
from app.nbi.errors import NbiConflictError, NbiNotFoundError
from app.nbi.gateway import NbiGateway
from config import settings

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v2", tags=["XGS cutover (read-only)"])

NetworkPath = Path(..., pattern=NETWORK_PATTERN, description="CMS network name, e.g. NTWK-MOCK-E7")


def _mapping() -> SmxMap:
    try:
        return SmxMap.load(settings.smx_map_path)
    except (OSError, ValueError) as e:
        raise HTTPException(status_code=503, detail=f"The SMx mapping file {settings.smx_map_path} "
                                                    f"could not be loaded: {e}") from None


@router.get("/networks/{network}/export", response_model=CutoverExport,
            responses={200: {"content": {"text/csv": {}}}})
async def export(network: str = NetworkPath,
                 pon: Optional[str] = Query(None, pattern=r"^\d+/\d+/\d+$", description="Only this PON port"),
                 format: str = Query("json", pattern="^(json|csv)$"),
                 table: str = Query("services", pattern="^(services|ports)$", description="Which CSV"),
                 ports: bool = Query(True, description="Read port settings (one CMS call per port)"),
                 gateway: NbiGateway = Depends(get_gateway)):
    """
    Everything CMS holds for a network or one PON port, one record per ONT,
    laid out as subscriber, ONT and data services. Fields CMS cannot provide
    start with `fill_in_` and are empty. Only reads CMS; on a large network
    this takes a while (several calls per ONT, in parallel).
    """
    result = await export_network(gateway, network, pon, include_ports=ports)
    if format == "json":
        return result
    rows, columns = (service_rows(result), SERVICE_COLUMNS) if table == "services" else (port_rows(result),
                                                                                         PORT_COLUMNS)
    name = f"{network}{'-pon-' + pon.replace('/', '-') if pon else ''}.{table}.csv"
    return PlainTextResponse(to_csv(rows, columns), media_type="text/csv",
                             headers={"Content-Disposition": f'attachment; filename="{name}"'})


class PlanRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    export: CutoverExport
    subscriber_id: Optional[str] = None


@router.post("/smx/plans", response_model=SmxPlan)
async def plan(request: PlanRequest) -> SmxPlan:
    """
    Dry run: the SMx requests that would recreate the subscribers in an export
    (as returned by the export route, possibly with fill_in_ fields filled).
    Nothing is sent. A subscriber with an unmapped value fails on its own.
    """
    try:
        return plan_export(request.export, _mapping(), request.subscriber_id)
    except LookupError as e:
        raise HTTPException(status_code=404, detail=str(e)) from None


@router.get("/networks/{network}/subscribers/{subscriber_id}/smx-plan", response_model=SmxPlan)
async def subscriber_plan(network: str = NetworkPath, subscriber_id: str = Path(..., pattern=r"^[A-Za-z0-9_.-]{1,32}$"),
                          ont_id: Optional[int] = Query(None, ge=1, description="If the subscriber has several ONTs"),
                          gateway: NbiGateway = Depends(get_gateway)) -> SmxPlan:
    """
    Dry run for one subscriber, read live from CMS. The fill_in_ fields are
    empty, so the plan lists them under to_fill_in. Nothing is sent.
    """
    matches = await gateway.find_onts_by_subscriber(network, subscriber_id)
    if ont_id is not None:
        matches = [o for o in matches if o.ont_id == ont_id]
    if not matches:
        raise NbiNotFoundError(f"Subscriber {subscriber_id} has no ONT on {network}.")
    if len(matches) > 1:
        raise NbiConflictError(f"Subscriber {subscriber_id} has {len(matches)} ONTs on {network} "
                               f"({', '.join(str(o.ont_id) for o in matches)}). Pass ont_id to choose one.")
    record = await export_ont(gateway, network, matches[0].ont_id, include_ports=True)
    one = CutoverExport(network=network, exported_at=datetime.now(timezone.utc), records=[record])
    return plan_export(one, _mapping())
