import logging
import os

from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import PlainTextResponse, RedirectResponse
from nost_tools.configuration import ConnectionConfig
from nost_tools.manager import Manager

from .schemas import (
    ExecuteRequest,
    InitRequest,
    StartRequest,
    StopRequest,
    UpdateRequest,
)

# configure logging
logging.basicConfig(level=logging.INFO)

# set version number
__version__ = "0.0.1"

# create application
app = FastAPI(
    title="NOS-T Monitor Backend (Manager API)",
    description="Provides a RESTful HTTP interface to NOS-T manager functions.",
    version=__version__,
)

# load environment variables from the .env file
load_dotenv()

# Configure CORS middleware
cors_origins = os.getenv("CORS_ORIGINS", "").split(",")
origins = [origin.strip() for origin in cors_origins]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

config = ConnectionConfig(
    username=os.getenv("USERNAME"),
    password=os.getenv("PASSWORD"),
    rabbitmq_host=os.getenv("RABBITMQ_HOST"),
    rabbitmq_port=os.getenv("RABBITMQ_PORT"),
    keycloak_authentication=os.getenv("KEYCLOAK_AUTHENTICATION").lower() == "true",
    keycloak_host=os.getenv("KEYCLOAK_HOST"),
    keycloak_port=os.getenv("KEYCLOAK_PORT"),
    keycloak_realm=os.getenv("KEYCLOAK_REALM"),
    client_id=os.getenv("CLIENT_ID"),
    client_secret_key=os.getenv("CLIENT_SECRET_KEY"),
    virtual_host=os.getenv("VIRTUAL_HOST"),
    is_tls=os.getenv("IS_TLS").lower() == "true",
)

MANAGERS = {}


def get_manager(prefix):
    if prefix in MANAGERS:
        return MANAGERS[prefix]
    else:
        MANAGERS[prefix] = Manager(setup_signal_handlers=False)
        MANAGERS[prefix].start_up(prefix, config)
        return MANAGERS[prefix]


@app.get("/", include_in_schema=False)
async def docs_redirect():
    return RedirectResponse(url="/docs")


@app.get("/status/{prefix}", tags=["manager"], response_class=PlainTextResponse)
def get_scenario_mode(prefix: str):
    """
    Reports the current scenario execution mode for a prefix.
    """
    return get_manager(prefix).simulator.get_mode()


@app.post("/init/{prefix}", tags=["manager"])
def run_init_command(prefix: str, request: InitRequest):
    """
    Issues the init command to initialize a new scenario execution.
    """
    try:
        get_manager(prefix).init(
            request.sim_start_time, request.sim_stop_time, request.required_apps
        )
    except RuntimeError as err:
        raise HTTPException(status_code=400, detail=str(err))


@app.post("/start/{prefix}", tags=["manager"])
def run_start_command(prefix: str, request: StartRequest):
    """
    Issues the start command to start a new scenario execution.
    """
    try:
        get_manager(prefix).start(
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
def run_stop_command(prefix: str, request: StopRequest):
    """
    Issues the stop command to stop a scenario execution.
    """
    try:
        get_manager(prefix).stop(request.sim_stop_time)
    except RuntimeError as err:
        raise HTTPException(status_code=400, detail=str(err))


@app.post("/update/{prefix}", tags=["manager"])
def run_update_command(prefix: str, request: UpdateRequest):
    """
    Issues the update command to change the time scale factor of a scenario execution.
    """
    try:
        get_manager(prefix).update(request.time_scale_factor, request.sim_update_time)
    except RuntimeError as err:
        raise HTTPException(status_code=400, detail=str(err))


@app.post("/testScript/{prefix}", tags=["manager"])
def execute_text_plan(prefix: str, request: ExecuteRequest):
    """
    Executes a test plan to manage the end-to-end scenario execution.
    """
    # TODO execute_test_plan is a blocking call; consider running in a background thread
    try:
        get_manager(prefix).execute_test_plan(
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
    except RuntimeError as err:
        raise HTTPException(status_code=400, detail=str(err))


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("main:app", port=3000, reload=True)
