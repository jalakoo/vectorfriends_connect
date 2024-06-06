from dataclasses import dataclass
from basicauth import decode
from neo4j import GraphDatabase, basic_auth
import functions_framework
import os

# Note: Pydantic does not appear to work properly in Google Cloud Functions

HOST = os.environ.get("NEO4J_URI")
PASSWORD = os.environ.get("NEO4J_PASSWORD")
USER = os.environ.get("NEO4J_USER", "neo4j")
DATABASE = os.environ.get("NEO4J_DATABASE", "neo4j")


@dataclass(frozen=True)
class ConnectData:
    user1: str
    user2: str


def upload_to_neo4j(query, params):
    try:
        with GraphDatabase.driver(
            HOST, auth=basic_auth(USER, PASSWORD), database=DATABASE
        ) as driver:
            return driver.execute_query(query, params)
    except Exception as e:
        print(f"Upload query error: {e}")
        return None


def make_connection(data: ConnectData):

    # TODO: Check Neo4j credentials + database is accessible

    print(f"Request received: {data}")

    # Create User and Tenant records, and remove any prior tech relationships
    query_0 = """
    MATCH (u:User {email: $email1}), (u2:User {email: $email2})
    MERGE (u)-[r:CONNECTED_TO]->(u2)
    RETURN u, u2
"""
    params_0 = {
        "email1": data.user1,
        "email2": data.user2,
    }
    records, query_0_result, keys = upload_to_neo4j(query_0, params_0)
    print(
        f"Connect users records: {records} result counters: {query_0_result.counters}, keys: {keys}"
    )

    if len(records) == 0:
        return "One or more of the User emails were not found", 404

    if query_0_result.counters.contains_updates is False:
        return "Users already connected", 200

    return "OK", 200


@functions_framework.http
def connect(request):

    # Optional Basic Auth
    basic_user = os.environ.get("BASIC_AUTH_USER", None)
    basic_password = os.environ.get("BASIC_AUTH_PASSWORD", None)
    if basic_user and basic_password:
        auth_header = request.headers.get("Authorization")
        if auth_header is None:
            return "Missing authorization credentials", 401
        request_username, request_password = decode(auth_header)
        if request_username != basic_user or request_password != basic_password:
            return "Unauthorized", 401

    payload = request.get_json(silent=True)

    if payload:
        try:
            form = ConnectData(**payload)
            return make_connection(form)
        except Exception as e:
            return f"Invalid payload: {e}", 400
