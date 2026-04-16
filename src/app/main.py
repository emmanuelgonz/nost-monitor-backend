import logging
import os

from dotenv import load_dotenv
from fastapi import BackgroundTasks, Depends, FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import PlainTextResponse, RedirectResponse
from nost_tools.configuration import ConnectionConfig
from nost_tools.manager import Manager

from .auth import require_auth
from .schemas import (
    ExecuteRequest,
    InitRequest,
    StartRequest,
    StopRequest,
    UpdateRequest,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

__version__ = "0.1.0"

app = FastAPI(
    title="NOS-T Monitor Backend (Manager API)",
    description="Provides a RESTful HTTP interface to NOS-T manager functions.",
    version=__version__,
)

load_dotenv()

cors_origins = os.getenv("CORS_ORIGINS", "").split(",")
origins = [origin.strip() for origin in cors_origins if origin.strip()]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def _build_config() -> ConnectionConfig:
    return ConnectionConfig(
        rabbitmq_host=os.getenv("RABBITMQ_HOST"),
        rabbitmq_port=int(os.getenv("RABBITMQ_PORT", "5672")),
        keycloak_authentication=os.getenv("KEYCLOAK_AUTHENTICATION", "false").lower() == "true",
        keycloak_host=os.getenv("KEYCLOAK_HOST"),
        keycloak_port=int(os.getenv("KEYCLOAK_PORT", "8443")),
        keycloak_realm=os.getenv("KEYCLOAK_REALM"),
        client_id=os.getenv("CLIENT_ID"),
        virtual_host=os.getenv("RABBITMQ_VHOST", "/"),
        is_tls=os.getenv("IS_TLS", "true").lower() == "true",
    )


MANAGERS: dict[tuple[str, str], Manager] = {}


def get_manager(prefix: str, auth: dict) -> Manager:
    user_sub = auth["claims"].get("sub", "unknown")
    access_token = auth["access_token"]
    refresh_token = auth["refresh_token"]
    if not refresh_token:
        raise HTTPException(
            status_code=400,
            detail="X-Refresh-Token header required to start a Manager session",
        )

    key = (prefix, user_sub)
    manager = MANAGERS.get(key)
    if manager is not None:
        manager.refresh_token = refresh_token
        try:
            manager.update_connection_credentials(access_token)
        except Exception as err:
            logger.warning("Failed to update cached Manager credentials: %s", err)
        return manager

    manager = Manager(setup_signal_handlers=False)
    manager.start_up(
        prefix,
        _build_config(),
        set_offset=False,
        access_token=access_token,
        refresh_token=refresh_token,
    )
    MANAGERS[key] = manager
    return manager


@app.get("/", include_in_schema=False)
async def docs_redirect():
    return RedirectResponse(url="/docs")


@app.get("/status/{prefix}", tags=["manager"], response_class=PlainTextResponse)
def get_scenario_mode(prefix: str, auth: dict = Depends(require_auth)):
    """Reports the current scenario execution mode for a prefix."""
    return get_manager(prefix, auth).simulator.get_mode()


@app.post("/init/{prefix}", tags=["manager"])
def run_init_command(
    prefix: str,
    request: InitRequest,
    auth: dict = Depends(require_auth),
):
    """Issues the init command to initialize a new scenario execution."""
    logger.info("user=%s action=init prefix=%s", auth["claims"].get("sub"), prefix)
    try:
        get_manager(prefix, auth).init(
            request.sim_start_time, request.sim_stop_time, request.required_apps
        )
    except RuntimeError as err:
        raise HTTPException(status_code=400, detail=str(err))


@app.post("/start/{prefix}", tags=["manager"])
def run_start_command(
    prefix: str,
    request: StartRequest,
    auth: dict = Depends(require_auth),
):
    """Issues the start command to start a new scenario execution."""
    logger.info("user=%s action=start prefix=%s", auth["claims"].get("sub"), prefix)
    try:
        get_manager(prefix, auth).start(
            request.sim_start_time,
            request.sim_stop_time,
            request.start_time,
            request.time_step,
            request.time_scale_factor,
            request.time_status_step,
            request.time_status_init,
        )
    except RuntimeError as err:
        raise HTTPException(status_code=400, detail=str(err))


@app.post("/stop/{prefix}", tags=["manager"])
def run_stop_command(
    prefix: str,
    request: StopRequest,
    auth: dict = Depends(require_auth),
):
    """Issues the stop command to stop a scenario execution."""
    logger.info("user=%s action=stop prefix=%s", auth["claims"].get("sub"), prefix)
    try:
        get_manager(prefix, auth).stop(request.sim_stop_time)
    except RuntimeError as err:
        raise HTTPException(status_code=400, detail=str(err))


@app.post("/update/{prefix}", tags=["manager"])
def run_update_command(
    prefix: str,
    request: UpdateRequest,
    auth: dict = Depends(require_auth),
):
    """Issues the update command to change the time scale factor of a scenario execution."""
    logger.info("user=%s action=update prefix=%s", auth["claims"].get("sub"), prefix)
    try:
        get_manager(prefix, auth).update(request.time_scale_factor, request.sim_update_time)
    except RuntimeError as err:
        raise HTTPException(status_code=400, detail=str(err))


@app.post("/testScript/{prefix}", tags=["manager"], status_code=202)
def execute_text_plan(
    prefix: str,
    request: ExecuteRequest,
    background_tasks: BackgroundTasks,
    auth: dict = Depends(require_auth),
):
    """Executes a test plan to manage the end-to-end scenario execution (runs in background)."""
    user_sub = auth["claims"].get("sub")
    logger.info("user=%s action=testScript prefix=%s submitted", user_sub, prefix)

    manager = get_manager(prefix, auth)
    background_tasks.add_task(
        manager.execute_test_plan,
        request.sim_start_time,
        request.sim_stop_time,
        request.start_time,
        request.time_step,
        request.time_scale_factor,
        [u.to_manager_format() for u in request.time_scale_updates],
        request.time_status_step,
        request.time_status_init,
        request.command_lead,
        request.required_apps,
        request.init_retry_delay_s,
        request.init_max_retry,
    )
    return {"status": "accepted", "user": user_sub, "prefix": prefix}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("main:app", port=3000, reload=True)
