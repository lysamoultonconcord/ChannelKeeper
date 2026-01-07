from __future__ import annotations

import streamlit as st
from datetime import date
from typing import Any, Dict, Optional

from src.constants import (
    YOUTUBE_CHANNEL_URL_PREFIX,
    STATUS_OPTIONS,
    LOGIN_AFFILIATION_OPTIONS,
    ACCESS_LEVEL_OPTIONS,
    GAIN_CREATE_OPTIONS,
    YPP_STATUS_OPTIONS,
    YN_OPTIONS,
)
from src.db import fetch_channel, merge_upsert
from src.youtube import fetch_channel_info


st.set_page_config(page_title="Channel Master Editor", layout="wide")
st.title("Channel Master Editor")

# Session state
if "db_row" not in st.session_state:
    st.session_state.db_row = None
if "api_info" not in st.session_state:
    st.session_state.api_info = None
if "channel_id" not in st.session_state:
    st.session_state.channel_id = ""


def _norm_str(v: Any) -> str:
    return "" if v is None else str(v)


def _norm_opt(v: str) -> Optional[str]:
    v2 = v.strip()
    return v2 if v2 else None


def _bool_from_db(v: Any) -> bool:
    return bool(v) if v is not None else False


def _yn_from_db(v: Any) -> str:
    if v is None:
        return ""
    vv = str(v).strip().upper()
    if vv in ("Y", "YES", "TRUE", "T", "1"):
        return "Y"
    if vv in ("N", "NO", "FALSE", "F", "0"):
        return "N"
    return ""


with st.sidebar:
    st.header("Lookup")
    channel_id = st.text_input(
        "CHANNEL_ID (UC…)",
        value=st.session_state.channel_id,
        placeholder="UCAItxpwVeBAHTXryyrtHt6w",
    ).strip()

    auto_refresh = st.checkbox("Pull/refresh YouTube fields on lookup", value=True)
    updated_by = st.text_input("UPDATED_BY", value="Lysa")

    colA, colB = st.columns(2)
    lookup_clicked = colA.button("Lookup", type="primary", disabled=not channel_id)
    clear_clicked = colB.button("Clear")

    st.caption("Tip: Lookup loads existing record (if any) and can refresh YouTube title + created date.")

if clear_clicked:
    st.session_state.channel_id = ""
    st.session_state.db_row = None
    st.session_state.api_info = None
    st.rerun()

if lookup_clicked:
    st.session_state.channel_id = channel_id
    st.session_state.db_row = fetch_channel(channel_id)

    st.session_state.api_info = None
    if auto_refresh:
        try:
            st.session_state.api_info = fetch_channel_info(channel_id)
        except Exception as e:
            st.warning(f"YouTube API refresh failed: {e}")

# Get current context
channel_id = st.session_state.channel_id.strip()
db_row: Dict[str, Any] = st.session_state.db_row or {}
api_info = st.session_state.api_info

if channel_id:
    computed_url = f"{YOUTUBE_CHANNEL_URL_PREFIX}{channel_id}"
else:
    computed_url = ""

# API overlay for two fields
api_title = api_info.channel_title if api_info else None
api_date_created = api_info.date_created if api_info else None

channel_title_default = api_title if api_title else db_row.get("CHANNEL_TITLE")
date_created_default = api_date_created if api_date_created else db_row.get("DATE_CREATED")

# Header status
if channel_id:
    if st.session_state.db_row:
        st.success("Loaded existing record from Snowflake.")
    else:
        st.info("No record found — you are creating a new record.")
else:
    st.info("Enter a CHANNEL_ID and click Lookup.")

st.subheader("Edit / Create Channel")

# Disable form if no channel_id selected
form_disabled = not bool(channel_id)

with st.form("channel_form"):
    c1, c2, c3 = st.columns(3)

    with c1:
        st.text_input("CHANNEL_ID", value=channel_id, disabled=True)
        st.text_input("URL (computed)", value=computed_url, disabled=True)

        channel_title = st.text_input(
            "CHANNEL_TITLE (API)",
            value=_norm_str(channel_title_default),
            disabled=form_disabled,
        )

        artist_name = st.text_input(
            "ARTIST_NAME",
            value=_norm_str(db_row.get("ARTIST_NAME")),
            disabled=form_disabled,
        )

        status_val = _norm_str(db_row.get("STATUS"))
        status = st.selectbox(
            "STATUS",
            STATUS_OPTIONS,
            index=STATUS_OPTIONS.index(status_val) if status_val in STATUS_OPTIONS else 0,
            disabled=form_disabled,
        )

        # Stored as VARCHAR in table, but you want Y/N semantics
        label_pub_db = _yn_from_db(db_row.get("LABEL_PUB"))
        label_pub = st.selectbox(
            "LABEL_PUB (Y/N)",
            YN_OPTIONS,
            index=YN_OPTIONS.index(label_pub_db) if label_pub_db in YN_OPTIONS else 0,
            disabled=form_disabled,
        )

        lms_db = _yn_from_db(db_row.get("LMS"))
        lms = st.selectbox(
            "LMS (Y/N)",
            YN_OPTIONS,
            index=YN_OPTIONS.index(lms_db) if lms_db in YN_OPTIONS else 0,
            disabled=form_disabled,
        )

    with c2:
        login_aff_val = _norm_str(db_row.get("LOGIN_AFFILIATION"))
        login_affiliation = st.selectbox(
            "LOGIN_AFFILIATION",
            LOGIN_AFFILIATION_OPTIONS,
            index=LOGIN_AFFILIATION_OPTIONS.index(login_aff_val) if login_aff_val in LOGIN_AFFILIATION_OPTIONS else 0,
            disabled=form_disabled,
        )

        network = st.text_input(
            "NETWORK",
            value=_norm_str(db_row.get("NETWORK")),
            disabled=form_disabled,
        )

        access_level_val = _norm_str(db_row.get("ACCESS_LEVEL"))
        access_level = st.selectbox(
            "ACCESS_LEVEL",
            ACCESS_LEVEL_OPTIONS,
            index=ACCESS_LEVEL_OPTIONS.index(access_level_val) if access_level_val in ACCESS_LEVEL_OPTIONS else 0,
            disabled=form_disabled,
        )

        gain_create_val = _norm_str(db_row.get("GAIN_CREATE"))
        gain_create = st.selectbox(
            "GAIN_CREATE",
            GAIN_CREATE_OPTIONS,
            index=GAIN_CREATE_OPTIONS.index(gain_create_val) if gain_create_val in GAIN_CREATE_OPTIONS else 0,
            disabled=form_disabled,
        )

        date_gained = st.date_input(
            "DATE_GAINED",
            value=db_row.get("DATE_GAINED"),
            disabled=form_disabled,
        )

        # API-managed read-only
        st.date_input(
            "DATE_CREATED (YouTube publishedAt)",
            value=date_created_default,
            disabled=True,
        )

        oac = st.checkbox("OAC", value=_bool_from_db(db_row.get("OAC")), disabled=form_disabled)
        verified = st.checkbox("VERIFIED", value=_bool_from_db(db_row.get("VERIFIED")), disabled=form_disabled)
        vevo_id = st.text_input("VEVO_ID", value=_norm_str(db_row.get("VEVO_ID")), disabled=form_disabled)

    with c3:
        oac_requested = st.checkbox(
            "OAC_REQUESTED",
            value=_bool_from_db(db_row.get("OAC_REQUESTED")),
            disabled=form_disabled,
        )

        oac_date_requested = st.date_input(
            "OAC_DATE_REQUESTED",
            value=db_row.get("OAC_DATE_REQUESTED"),
            disabled=form_disabled,
        )

        oac_merge_confirmation_date = st.date_input(
            "OAC_MERGE_CONFIRMATION_DATE",
            value=db_row.get("OAC_MERGE_CONFIRMATION_DATE"),
            disabled=form_disabled,
        )

        notes = st.text_area(
            "NOTES",
            value=_norm_str(db_row.get("NOTES")),
            height=140,
            disabled=form_disabled,
        )

        ypp_val = _norm_str(db_row.get("YPP_STATUS"))
        ypp_status = st.selectbox(
            "YPP_STATUS",
            YPP_STATUS_OPTIONS,
            index=YPP_STATUS_OPTIONS.index(ypp_val) if ypp_val in YPP_STATUS_OPTIONS else 0,
            disabled=form_disabled,
        )

        access_lost = st.checkbox(
            "ACCESS_LOST",
            value=_bool_from_db(db_row.get("ACCESS_LOST")),
            disabled=form_disabled,
        )

        date_of_loss = st.date_input(
            "DATE_OF_LOSS",
            value=db_row.get("DATE_OF_LOSS"),
            disabled=form_disabled,
        )

    save = st.form_submit_button("Save (Upsert to Snowflake)", type="primary", disabled=form_disabled)

if save:
    payload = {
        "CHANNEL_ID": channel_id,

        # API-managed
        "CHANNEL_TITLE": _norm_opt(channel_title),
        "DATE_CREATED": api_date_created,  # use API as source of truth when available
        "URL": computed_url,

        # Manual fields
        "ARTIST_NAME": _norm_opt(artist_name),
        "STATUS": _norm_opt(status),
        "LABEL_PUB": _norm_opt(label_pub),  # "Y"/"N"/None
        "LMS": _norm_opt(lms),              # "Y"/"N"/None

        "LOGIN_AFFILIATION": _norm_opt(login_affiliation),
        "NETWORK": _norm_opt(network),
        "ACCESS_LEVEL": _norm_opt(access_level),

        "GAIN_CREATE": _norm_opt(gain_create),
        "DATE_GAINED": date_gained if isinstance(date_gained, date) else None,

        "OAC": bool(oac),
        "VERIFIED": bool(verified),
        "VEVO_ID": _norm_opt(vevo_id),

        "OAC_REQUESTED": bool(oac_requested),
        "OAC_DATE_REQUESTED": oac_date_requested if isinstance(oac_date_requested, date) else None,
        "OAC_MERGE_CONFIRMATION_DATE": oac_merge_confirmation_date if isinstance(oac_merge_confirmation_date, date) else None,

        "NOTES": _norm_opt(notes),
        "YPP_STATUS": _norm_opt(ypp_status),

        "ACCESS_LOST": bool(access_lost),
        "DATE_OF_LOSS": date_of_loss if isinstance(date_of_loss, date) else None,

        "UPDATED_BY": _norm_opt(updated_by),
    }

    try:
        merge_upsert(payload)
        st.success("Saved to Snowflake.")
        # Refresh the record so the UI shows the saved version
        st.session_state.db_row = fetch_channel(channel_id)
    except Exception as e:
        st.error(f"Save failed: {e}")

# Optional: show API debug
if api_info:
    with st.expander("YouTube API data used"):
        st.write(
            {
                "channel_title": api_info.channel_title,
                "date_created": str(api_info.date_created) if api_info.date_created else None,
            }
        )
