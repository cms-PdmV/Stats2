"""
This module scans all the ReReco workflows
available in Stats2 request database and retrieves from
ReqMgr2 and DBS the data related to lumisection progress
"""
import os
import pprint
import datetime
from copy import deepcopy
from stats_update import StatsUpdate
from utils import (
    make_request,
    setup_console_logging
)

# Set up the logger
setup_console_logging()

# Required environment variables
REQ_VARIABLES = ["DB_URL", "STATS_DB_AUTH_HEADER", "USERCRT", "USERKEY"]

# Check the required variables are set before starting the execution
MISSING_VARIABLES = []
for env in REQ_VARIABLES:
    if not os.getenv(env):
        MISSING_VARIABLES.append(env)

if MISSING_VARIABLES:
    raise RuntimeError(f"Please set the following env variables: {MISSING_VARIABLES}")

# Start the execution
stats_handler: StatsUpdate = StatsUpdate()
logger = stats_handler.logger


def retrieve_all_from_request_type(type: str, limit: int = 2**64) -> list[dict]:
    """
    Queries Stats2 'requests' database and returns all
    the documents that match an specific 'RequestType'

    Args:
        type (str): Desired 'RequestType' to return
        limit (int): Maximum number of documents to retrieved.

    Returns:
        list[dict]: Request documents that match with the requested
            'RequestType'
    """
    # Mango query
    query = {
        "selector": {
            "RequestType": {"$eq": type}
        },
        "limit": limit,
        "skip": 0,
        "execution_stats": True
    }

    # HTTP Request
    # INFO: Temporarily remove the environment variables 
    # USERCRT and USERKEY so that make_request doesn't crash
    USERCRT = os.getenv("USERCRT", None)
    USERKEY = os.getenv("USERKEY", None)
    os.environ.pop("USERCRT", None)
    os.environ.pop("USERKEY", None)

    headers = {
        "Accept": "application/json",
        "Content-type": "application/json",
        "Authorization": f"Basic {os.getenv('STATS_DB_AUTH_HEADER')}"
    }
    result = make_request(
        host=os.getenv("DB_URL"),
        query_url="/requests/_find",
        data=query,
        timeout=240,
        headers=headers
    )

    # Restore them
    os.environ["USERCRT"] = USERCRT
    os.environ["USERKEY"] = USERKEY

    if not result:
        logger.error("Unable to perform the mango query: %s", pprint.pformat(query))
        return []
    
    execution_stats = result.get("execution_stats", {})
    docs = result.get("docs", [])
    logger.info(
        "Execution time: %s\n",
        pprint.pformat(execution_stats)
    )

    return docs


def include_lumisections(stats_req: dict) -> dict:
    """
    For the given Stats2 request, retrieve from ReqMgr2
    and DBS the number of lumisections processed and include this
    information into the given document.

    Args:
        stats_req (dict): Stats2 request data

    Returns
        dict: Stats2 request with lumisection data included.
    """
    def __update_event_history(history_timestamp: dict, set_default: bool = True) -> dict:
        """
        Updates a record for the 'EventNumberHistory' object including the
        lumisection record per each registered dataset.

        Args:
            history_timestamp (dict): 'EventNumberHistory' object included into Stats2 request.
            set_default (bool): If True, sets the lumisection value ('Lumis') as zero
                otherwise, this queries DBS and returns the current value for the dataset.

        Returns:
            dict: Event history record with lumisection data included.
        """
        history = deepcopy(history_timestamp)
        datasets: dict = history.get("Datasets", {})
        for dataset_name, dataset_record in datasets.items():
            if not set_default:
                # Query DBS
                dbs_lumis = stats_handler.get_dataset_lumisections(dataset_name=dataset_name)
                dataset_record["Lumis"] = dbs_lumis
            else:
                # Set zero
                dataset_record["Lumis"] = 0

            # Update the content
            history["Datasets"][dataset_name] = dataset_record
            pass

        return history


    # Start function
    name = "RequestName"
    lumis = "TotalInputLumis"
    history = "EventNumberHistory"
    request = deepcopy(stats_req)

    # Retrieve the lumisection information.
    workflow_name: str = request.get(name, "")
    reqmgr_data: dict = stats_handler.get_new_dict_from_reqmgr2(
        workflow_name=workflow_name
    )
    reqmgr_lumis: int = reqmgr_data.get(lumis, 0)
    request[lumis] = reqmgr_lumis

    # Update the dataset history.
    # Include the lumis only the last record.
    history_data: list[dict] = request.get(history, [])
    if history_data:
        history_updated: list[dict] = []
        history_data = sorted(
            history_data, 
            key=lambda v: v.get("Time", 0),
            reverse=True
        )

        # Update the most recent history
        history_updated.append(
            __update_event_history(
                history_timestamp=history_data.pop(0),
                set_default=False
            )
        )

        for h in history_data:
            # Set the remaining values as zero
            history_updated.append(
                __update_event_history(
                    history_timestamp=h,
                )
            )

        # Update the history
        request[history] = history_updated

    
    return request


def execute() -> None:
    """
    Execute all the operations to fill the remaining
    attributes for ReReco workflows
    """
    start_time = datetime.datetime.now()

    # Retrieve all the workflows
    rereco_workflows = retrieve_all_from_request_type(
        type="ReReco"
    )

    # Lumisections included
    rereco_lumisections = []
    for idx, stats_req in enumerate(rereco_workflows):
        logger.info(
            "%s/%s Updating Stats2 request (%s)",
            idx + 1,
            len(rereco_workflows),
            stats_req.get("_id"),
        )
        updated = include_lumisections(stats_req)
        rereco_lumisections.append(updated)
    
    end_time = datetime.datetime.now()

    logger.info(
        "Updated documents (%s)",
        len(rereco_lumisections),
    )
    logger.debug(
        "Documents content: %s",
        pprint.pformat(rereco_lumisections)
    )
    logger.info(
        "Elapsed time: %s",
        end_time - start_time
    )

    pass


if __name__ == "__main__":
    execute()
