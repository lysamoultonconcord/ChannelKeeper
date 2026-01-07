from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional
import streamlit as st
import snowflake.connector
from snowflake.connector.cursor import DictCursor

from .constants import TABLE_FQN


@dataclass
class SnowflakeCfg:
    account: str
    user: str
    password: Optional[str] = None
    authenticator: Optional[str] = None
    role: Optional[str] = None
    warehouse: Optional[str] = None
    database: Optional[str] = None
    schema: Optional[str] = None


def get_snowflake_cfg() -> SnowflakeCfg:
    s = st.secrets["snowflake"]
    return SnowflakeCfg(
        account=s["account"],
        user=s["user"],
        password=s.get("password"),              # optional
        authenticator=s.get("authenticator"),    # optional (e.g. externalbrowser)
        role=s.get("role"),
        warehouse=s.get("warehouse"),
        database=s.get("database"),
        schema=s.get("schema"),
    )


@st.cache_resource
def get_connection():
    """
    Cached Snowflake connection so Streamlit reruns don't keep re-authing.
    Supports either:
      - SSO via authenticator=externalbrowser (no password)
      - Password auth via password=...
    """
    cfg = get_snowflake_cfg()

    conn_kwargs = {
        "account": cfg.account,
        "user": cfg.user,
        "role": cfg.role,
        "warehouse": cfg.warehouse,
        "database": cfg.database,
        "schema": cfg.schema,
    }

    if cfg.authenticator:
        conn_kwargs["authenticator"] = cfg.authenticator
    else:
        # If no authenticator, we require password
        if not cfg.password:
            raise KeyError(
                'Missing Snowflake auth. Add either snowflake.password or snowflake.authenticator to secrets.toml'
            )
        conn_kwargs["password"] = cfg.password

    return snowflake.connector.connect(**conn_kwargs)


def fetch_channel(channel_id: str) -> Optional[Dict[str, Any]]:
    sql = f"""
        SELECT *
        FROM {TABLE_FQN}
        WHERE CHANNEL_ID = %s
        LIMIT 1
    """
    con = get_connection()
    with con.cursor(DictCursor) as cur:
        cur.execute(sql, (channel_id,))
        return cur.fetchone()


def merge_upsert(payload: Dict[str, Any]) -> None:
    cols = [
        "CHANNEL_ID",
        "CHANNEL_TITLE",
        "DATE_CREATED",
        "URL",
        "ARTIST_NAME",
        "STATUS",
        "LABEL_PUB",
        "LMS",
        "LOGIN_AFFILIATION",
        "NETWORK",
        "ACCESS_LEVEL",
        "GAIN_CREATE",
        "DATE_GAINED",
        "OAC",
        "VERIFIED",
        "VEVO_ID",
        "OAC_REQUESTED",
        "OAC_DATE_REQUESTED",
        "OAC_MERGE_CONFIRMATION_DATE",
        "NOTES",
        "YPP_STATUS",
        "ACCESS_LOST",
        "DATE_OF_LOSS",
        "UPDATED_BY",
    ]

    vals = tuple(payload.get(c) for c in cols)

    merge_sql = f"""
    MERGE INTO {TABLE_FQN} t
    USING (SELECT
      %s AS CHANNEL_ID,
      %s AS CHANNEL_TITLE,
      %s AS DATE_CREATED,
      %s AS URL,
      %s AS ARTIST_NAME,
      %s AS STATUS,
      %s AS LABEL_PUB,
      %s AS LMS,
      %s AS LOGIN_AFFILIATION,
      %s AS NETWORK,
      %s AS ACCESS_LEVEL,
      %s AS GAIN_CREATE,
      %s AS DATE_GAINED,
      %s AS OAC,
      %s AS VERIFIED,
      %s AS VEVO_ID,
      %s AS OAC_REQUESTED,
      %s AS OAC_DATE_REQUESTED,
      %s AS OAC_MERGE_CONFIRMATION_DATE,
      %s AS NOTES,
      %s AS YPP_STATUS,
      %s AS ACCESS_LOST,
      %s AS DATE_OF_LOSS,
      %s AS UPDATED_BY
    ) s
    ON t.CHANNEL_ID = s.CHANNEL_ID
    WHEN MATCHED THEN UPDATE SET
      CHANNEL_TITLE = s.CHANNEL_TITLE,
      DATE_CREATED = s.DATE_CREATED,
      URL = s.URL,
      ARTIST_NAME = s.ARTIST_NAME,
      STATUS = s.STATUS,
      LABEL_PUB = s.LABEL_PUB,
      LMS = s.LMS,
      LOGIN_AFFILIATION = s.LOGIN_AFFILIATION,
      NETWORK = s.NETWORK,
      ACCESS_LEVEL = s.ACCESS_LEVEL,
      GAIN_CREATE = s.GAIN_CREATE,
      DATE_GAINED = s.DATE_GAINED,
      OAC = s.OAC,
      VERIFIED = s.VERIFIED,
      VEVO_ID = s.VEVO_ID,
      OAC_REQUESTED = s.OAC_REQUESTED,
      OAC_DATE_REQUESTED = s.OAC_DATE_REQUESTED,
      OAC_MERGE_CONFIRMATION_DATE = s.OAC_MERGE_CONFIRMATION_DATE,
      NOTES = s.NOTES,
      YPP_STATUS = s.YPP_STATUS,
      ACCESS_LOST = s.ACCESS_LOST,
      DATE_OF_LOSS = s.DATE_OF_LOSS,
      UPDATED_AT = CURRENT_TIMESTAMP(),
      UPDATED_BY = s.UPDATED_BY
    WHEN NOT MATCHED THEN INSERT (
      CHANNEL_ID, CHANNEL_TITLE, DATE_CREATED, URL, ARTIST_NAME, STATUS, LABEL_PUB, LMS,
      LOGIN_AFFILIATION, NETWORK, ACCESS_LEVEL, GAIN_CREATE, DATE_GAINED,
      OAC, VERIFIED, VEVO_ID, OAC_REQUESTED, OAC_DATE_REQUESTED, OAC_MERGE_CONFIRMATION_DATE,
      NOTES, YPP_STATUS, ACCESS_LOST, DATE_OF_LOSS, UPDATED_BY
    ) VALUES (
      s.CHANNEL_ID, s.CHANNEL_TITLE, s.DATE_CREATED, s.URL, s.ARTIST_NAME, s.STATUS, s.LABEL_PUB, s.LMS,
      s.LOGIN_AFFILIATION, s.NETWORK, s.ACCESS_LEVEL, s.GAIN_CREATE, s.DATE_GAINED,
      s.OAC, s.VERIFIED, s.VEVO_ID, s.OAC_REQUESTED, s.OAC_DATE_REQUESTED, s.OAC_MERGE_CONFIRMATION_DATE,
      s.NOTES, s.YPP_STATUS, s.ACCESS_LOST, s.DATE_OF_LOSS, s.UPDATED_BY
    );
    """

    con = get_connection()
    with con.cursor() as cur:
        cur.execute(merge_sql, vals)
    con.commit()
