import logging
import os
import threading
from time import monotonic

from dotenv import load_dotenv
from fastapi import BackgroundTasks, Depends, FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import PlainTextResponse, RedirectResponse
from nost_tools.configuration import ConnectionConfig
from nost_tools.manager import Manager
from pika.exceptions import (
    AMQPConnectionError,
    ChannelClosedByBroker,
    ProbableAccessDeniedError,
    ProbableAuthenticationError,
    UnroutableError,
)

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


MANAGER_IDLE_TIMEOUT_S = int(os.getenv("MANAGER_IDLE_TIMEOUT_S", "1800"))
MANAGERS: dict[tuple[str, str], tuple[Manager, float]] = {}


def _is_manager_healthy(manager: Manager) -> bool:
    flag = getattr(manager, "_is_connected", None)
    return bool(flag and flag.is_set())


def _shutdown_manager_async(manager: Manager, label: str) -> None:
    def _run() -> None:
        try:
            manager.stop_application()
        except Exception as err:
            logger.warning("Failed to shut down Manager %s: %s", label, err)

    threading.Thread(target=_run, name=f"mgr-shutdown-{label}", daemon=True).start()


def _evict_expired_managers() -> None:
    now = monotonic()
    stale = [
        key for key, (_, last_used) in MANAGERS.items()
        if now - last_used > MANAGER_IDLE_TIMEOUT_S
    ]
    for key in stale:
        entry = MANAGERS.pop(key, None)
        if entry is None:
            continue
        manager, _ = entry
        prefix, user_sub = key
        logger.info("Evicting idle Manager for prefix=%s user=%s", prefix, user_sub)
        _shutdown_manager_async(manager, f"{prefix}:{user_sub}")


def get_manager(prefix: str, auth: dict) -> Manager:
    _evict_expired_managers()

    user_sub = auth["claims"].get("sub", "unknown")
    access_token = auth["access_token"]
    refresh_token = auth["refresh_token"]
    if not refresh_token:
        raise HTTPException(
            status_code=400,
            detail="X-Refresh-Token header required to start a Manager session",
        )

    key = (prefix, user_sub)
    entry = MANAGERS.get(key)
    if entry is not None:
        manager, _ = entry
        if not _is_manager_healthy(manager):
            logger.info("Evicting unhealthy Manager for prefix=%s user=%s", prefix, user_sub)
            MANAGERS.pop(key, None)
            _shutdown_manager_async(manager, f"{prefix}:{user_sub}")
            entry = None
    if entry is not None:
        manager, _ = entry
        manager.refresh_token = refresh_token
        try:
            manager.update_connection_credentials(access_token)
        except Exception as err:
            logger.warning("Failed to update cached Manager credentials: %s", err)
        MANAGERS[key] = (manager, monotonic())
        return manager

    manager = Manager(setup_signal_handlers=False)
    manager.start_up(
        prefix,
        _build_config(),
        set_offset=False,
        access_token=access_token,
        refresh_token=refresh_token,
    )
    MANAGERS[key] = (manager, monotonic())
    return manager


BROKER_ERRORS = (
    ChannelClosedByBroker,
    AMQPConnectionError,
    UnroutableError,
)


def _broker_error_to_http(err: Exception) -> HTTPException:
    if isinstance(err, ChannelClosedByBroker):
        code = err.reply_code
        if code == 403:
            return HTTPException(403, f"Broker refused operation: {err.reply_text}")
        if code == 404:
            return HTTPException(404, f"Broker: {err.reply_text}")
        return HTTPException(400, f"Broker rejected request: {err.reply_text}")
    if isinstance(err, (ProbableAccessDeniedError, ProbableAuthenticationError)):
        return HTTPException(403, f"Broker denied connection: {err}")
    if isinstance(err, AMQPConnectionError):
        return HTTPException(502, f"Broker unreachable: {err}")
    if isinstance(err, UnroutableError):
        return HTTPException(400, "Message was not routable: check prefix.")
    return HTTPException(500, f"Unexpected broker error: {err}")


def _evict_manager(prefix: str, auth: dict) -> None:
    user_sub = auth["claims"].get("sub", "unknown")
    entry = MANAGERS.pop((prefix, user_sub), None)
    if entry is not None:
        manager, _ = entry
        _shutdown_manager_async(manager, f"{prefix}:{user_sub}")


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
    except BROKER_ERRORS as err:
        logger.warning("broker error on init prefix=%s: %s", prefix, err)
        _evict_manager(prefix, auth)
        raise _broker_error_to_http(err) from err


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
    except BROKER_ERRORS as err:
        logger.warning("broker error on start prefix=%s: %s", prefix, err)
        _evict_manager(prefix, auth)
        raise _broker_error_to_http(err) from err


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
    except BROKER_ERRORS as err:
        logger.warning("broker error on stop prefix=%s: %s", prefix, err)
        _evict_manager(prefix, auth)
        raise _broker_error_to_http(err) from err


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
    except BROKER_ERRORS as err:
        logger.warning("broker error on update prefix=%s: %s", prefix, err)
        _evict_manager(prefix, auth)
        raise _broker_error_to_http(err) from err


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

    def _run_test_plan_safely() -> None:
        try:
            manager.execute_test_plan(
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
        except BROKER_ERRORS as err:
            logger.warning("broker error in testScript prefix=%s: %s", prefix, err)
            _evict_manager(prefix, auth)
        except Exception as err:
            logger.exception("testScript failed prefix=%s: %s", prefix, err)
            _evict_manager(prefix, auth)

    background_tasks.add_task(_run_test_plan_safely)
    return {"status": "accepted", "user": user_sub, "prefix": prefix}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("main:app", port=3000, reload=True)
