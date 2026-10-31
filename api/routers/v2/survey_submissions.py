"""Own-survey endpoints accepting only scoped, Firebase-verified app assertions."""

from __future__ import annotations

import os
from typing import Annotated, Any

import jwt
from fastapi import APIRouter, Depends, HTTPException, Query, Request, Security
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from api.auth import get_bearer_token
from trr_backend.services import survey_submissions as service

router = APIRouter(prefix="/surveys", tags=["survey-submissions-v2"])
SURVEY_BEARER = HTTPBearer(
    scheme_name="SurveyOwnerAssertion",
    bearerFormat="JWT",
    auto_error=False,
    description="Short-lived survey:own assertion minted after Firebase user verification in TRR-APP.",
)


def require_survey_user(
    request: Request,
    _credentials: HTTPAuthorizationCredentials | None = Security(SURVEY_BEARER),
) -> dict[str, Any]:
    token = get_bearer_token(request)
    if not token:
        raise HTTPException(401, "Authentication required")
    secret = (os.getenv("TRR_INTERNAL_ADMIN_SHARED_SECRET") or "").strip()
    if not secret:
        raise HTTPException(503, "Survey authentication unavailable")
    try:
        claims = jwt.decode(
            token,
            secret,
            algorithms=["HS256"],
            audience="trr-backend-survey-submissions",
            issuer="trr-app-internal",
            options={"require": ["sub", "exp", "iat", "nbf", "iss", "aud"]},
        )
        if claims.get("scope") != "survey:own" or claims.get("provider") != "firebase":
            raise jwt.InvalidTokenError("invalid survey scope")
        if not isinstance(claims["sub"], str) or not claims["sub"].strip():
            raise jwt.InvalidTokenError("missing owner")
        if claims["exp"] - claims["iat"] > 120 or claims["exp"] <= claims["iat"]:
            raise jwt.InvalidTokenError("invalid assertion lifetime")
    except (jwt.InvalidTokenError, TypeError, ValueError) as exc:
        raise HTTPException(401, "Invalid survey authentication") from exc
    return {"uid": claims["sub"], "email": claims.get("email")}


SurveyUser = Annotated[dict[str, Any], Depends(require_survey_user)]


def _call(operation, *args):
    try:
        return operation(*args)
    except PermissionError as exc:
        raise HTTPException(403, str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc


@router.get("/x")
def get_x(user: SurveyUser):
    return {"state": service.x_state(_call(service.read_response, user, "survey_x"))}


@router.post("/x")
def post_x(user: SurveyUser, payload: dict[str, Any]):
    return _call(service.submit_x, user, payload)


@router.get("/global-profile")
def get_profile(user: SurveyUser):
    return {"response": _call(service.read_response, user, "global_profile")}


@router.post("/global-profile")
def post_profile(user: SurveyUser, payload: dict[str, Any]):
    return _call(service.submit_profile, user, payload)


@router.get("/show-responses")
def get_show(
    user: SurveyUser,
    survey_key: str = Query(alias="surveyKey"),
    season_id: str = Query(alias="seasonId"),
    episode_id: str = Query(alias="episodeId"),
):
    return {"response": _call(service.read_response, user, survey_key, season_id, episode_id)}


@router.post("/show-responses")
def post_show(user: SurveyUser, payload: dict[str, Any]):
    return _call(service.submit_show, user, payload)
