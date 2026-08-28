import os
import httpx
from mcp.server import MCPServer


# =========================
# API-FOOTBALL
# =========================

API_KEY = (os.environ.get("API_FOOTBALL_KEY") or "").strip()
BASE_URL = "https://v3.football.api-sports.io"


# =========================
# ZYLA / FLASHSCORE
# =========================

ZYLA_API_KEY = (os.environ.get("ZYLA_API_KEY") or "").strip()

ZYLA_LIVE_URL = (
    "https://zylalabs.com/api/12518/"
    "flashscore+-+live+api/23856/get+live+matches"
)


# =========================
# MCP SERVER
# =========================

mcp = MCPServer("Hidden Signal Live")


# =========================
# API-FOOTBALL REQUEST
# =========================

async def api_get(endpoint: str, params: dict | None = None):
    if not API_KEY:
        return {
            "error": "API_FOOTBALL_KEY is not configured",
            "diagnostic": {
                "key_loaded": False,
                "key_length": 0,
            },
        }

    headers = {
        "x-apisports-key": API_KEY,
    }

    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.get(
                f"{BASE_URL}{endpoint}",
                headers=headers,
                params=params or {},
            )

            try:
                data = response.json()
            except Exception:
                data = {
                    "raw_response": response.text
                }

            return {
                "source": "api-football",
                "diagnostic": {
                    "key_loaded": True,
                    "key_length": len(API_KEY),
                    "http_status": response.status_code,
                    "header_used": "x-apisports-key",
                },
                "api_response": data,
            }

    except Exception as e:
        return {
            "error": str(e),
            "source": "api-football",
        }


# =========================
# ZYLA REQUEST
# =========================

async def zyla_get(url: str, params: dict | None = None):
    if not ZYLA_API_KEY:
        return {
            "error": "ZYLA_API_KEY is not configured",
            "diagnostic": {
                "key_loaded": False,
                "key_length": 0,
            },
        }

    headers = {
        "Authorization": f"Bearer {ZYLA_API_KEY}",
    }

    try:
        async with httpx.AsyncClient(timeout=60.0) as client:
            response = await client.get(
                url,
                headers=headers,
                params=params or {},
            )

            try:
                data = response.json()
            except Exception:
                data = {
                    "raw_response": response.text
                }

            return {
                "source": "zyla-flashscore",
                "diagnostic": {
                    "key_loaded": True,
                    "key_length": len(ZYLA_API_KEY),
                    "http_status": response.status_code,
                },
                "api_response": data,
            }

    except Exception as e:
        return {
            "error": str(e),
            "source": "zyla-flashscore",
        }


# ==================================================
# ZYLA / FLASHSCORE LIVE
# ==================================================

@mcp.tool()
async def get_zyla_live_matches():
    """
    Get all football matches currently live
    from the Zyla FlashScore Live API.
    """

    return await zyla_get(
        ZYLA_LIVE_URL,
        {
            "sport_id": 1,
        },
    )


# ==================================================
# API-FOOTBALL TOOLS
# ==================================================

@mcp.tool()
async def get_live_matches():
    """Get all football matches that are live right now."""
    return await api_get(
        "/fixtures",
        {
            "live": "all",
        },
    )


@mcp.tool()
async def get_fixture_details(fixture_id: int):
    """Get current details for a specific fixture."""
    return await api_get(
        "/fixtures",
        {
            "id": fixture_id,
        },
    )


@mcp.tool()
async def get_fixture_statistics(fixture_id: int):
    """
    Get live match statistics:
    shots, shots on target, possession,
    corners, fouls and other available data.
    """
    return await api_get(
        "/fixtures/statistics",
        {
            "fixture": fixture_id,
        },
    )


@mcp.tool()
async def get_fixture_events(fixture_id: int):
    """
    Get match events:
    goals, cards, substitutions,
    penalties and VAR events when available.
    """
    return await api_get(
        "/fixtures/events",
        {
            "fixture": fixture_id,
        },
    )


@mcp.tool()
async def get_fixture_lineups(fixture_id: int):
    """Get lineups, formations and available player information."""
    return await api_get(
        "/fixtures/lineups",
        {
            "fixture": fixture_id,
        },
    )


@mcp.tool()
async def get_head_to_head(
    team1_id: int,
    team2_id: int,
    last: int = 10,
):
    """Get recent head-to-head matches between two teams."""

    return await api_get(
        "/fixtures/headtohead",
        {
            "h2h": f"{team1_id}-{team2_id}",
            "last": last,
        },
    )


@mcp.tool()
async def get_team_last_matches(
    team_id: int,
    season: int,
    last: int = 10,
):
    """Get a team's latest matches for form analysis."""

    return await api_get(
        "/fixtures",
        {
            "team": team_id,
            "season": season,
            "last": last,
        },
    )


@mcp.tool()
async def get_injuries(fixture_id: int):
    """Get available injury information for a fixture."""

    return await api_get(
        "/injuries",
        {
            "fixture": fixture_id,
        },
    )


@mcp.tool()
async def get_prematch_odds(fixture_id: int):
    """Get available bookmaker pre-match odds for a fixture."""

    return await api_get(
        "/odds",
        {
            "fixture": fixture_id,
        },
    )


@mcp.tool()
async def get_live_odds(fixture_id: int):
    """Get available live/in-play odds for a fixture."""

    return await api_get(
        "/odds/live",
        {
            "fixture": fixture_id,
        },
    )


# =========================
# START SERVER
# =========================

if __name__ == "__main__":
    port = int(os.environ.get("PORT", "10000"))

    mcp.run(
        transport="streamable-http",
        host="0.0.0.0",
        port=port,
    )
