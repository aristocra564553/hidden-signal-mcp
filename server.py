import os
import httpx
from mcp.server import MCPServer


# =========================
# CONFIG
# =========================

API_KEY = (os.environ.get("API_FOOTBALL_KEY") or "").strip()
ZYLA_API_KEY = (os.environ.get("ZYLA_API_KEY") or "").strip()

API_FOOTBALL_BASE_URL = "https://v3.football.api-sports.io"

ZYLA_BASE_URL = (
    "https://zylalabs.com/api/12518/"
    "flashscore+-+live+api"
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
            "source": "api-football",
            "diagnostic": {
                "key_loaded": False,
            },
        }

    headers = {
        "x-apisports-key": API_KEY,
    }

    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.get(
                f"{API_FOOTBALL_BASE_URL}{endpoint}",
                headers=headers,
                params=params or {},
            )

        try:
            data = response.json()
        except Exception:
            data = {
                "raw_response": response.text,
            }

        return {
            "source": "api-football",
            "diagnostic": {
                "key_loaded": True,
                "http_status": response.status_code,
            },
            "api_response": data,
        }

    except Exception as e:
        return {
            "source": "api-football",
            "error": str(e),
        }


# =========================
# ZYLA REQUEST
# =========================

async def zyla_get(
    endpoint_id: int,
    endpoint_slug: str,
    params: dict | None = None,
):
    if not ZYLA_API_KEY:
        return {
            "error": "ZYLA_API_KEY is not configured",
            "source": "zyla-flashscore",
            "diagnostic": {
                "key_loaded": False,
            },
        }

    url = (
        f"{ZYLA_BASE_URL}/"
        f"{endpoint_id}/"
        f"{endpoint_slug}"
    )

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
                "raw_response": response.text,
            }

        return {
            "source": "zyla-flashscore",
            "diagnostic": {
                "key_loaded": True,
                "http_status": response.status_code,
                "endpoint_id": endpoint_id,
            },
            "api_response": data,
        }

    except Exception as e:
        return {
            "source": "zyla-flashscore",
            "error": str(e),
        }


# ==================================================
# ZYLA / FLASHSCORE TOOLS
# ==================================================

@mcp.tool()
async def get_zyla_live_matches():
    """
    Get all football matches currently live from Zyla FlashScore.
    """

    return await zyla_get(
        23856,
        "get+live+matches",
        {
            "sport_id": 1,
        },
    )


@mcp.tool()
async def get_zyla_match_details(match_id: str):
    """
    Get detailed information for a Zyla FlashScore match.
    """

    return await zyla_get(
        23859,
        "get+match+details",
        {
            "match_id": match_id,
        },
    )


@mcp.tool()
async def get_zyla_match_summary(match_id: str):
    """
    Get match events, goals, cards and substitutions from Zyla.
    """

    return await zyla_get(
        23860,
        "get+match+summary",
        {
            "match_id": match_id,
        },
    )


@mcp.tool()
async def get_zyla_match_stats(match_id: str):
    """
    Get live match statistics from Zyla.
    """

    return await zyla_get(
        23861,
        "get+match+stats",
        {
            "match_id": match_id,
        },
    )


@mcp.tool()
async def get_zyla_match_odds(match_id: str):
    """
    Get available match odds from Zyla.
    """

    return await zyla_get(
        23865,
        "get+match+odds",
        {
            "match_id": match_id,
        },
    )


# ==================================================
# API-FOOTBALL TOOLS
# ==================================================

@mcp.tool()
async def get_live_matches():
    """Get all API-Football matches that are live right now."""

    return await api_get(
        "/fixtures",
        {
            "live": "all",
        },
    )


@mcp.tool()
async def get_fixture_details(fixture_id: int):
    """Get fixture details from API-Football."""

    return await api_get(
        "/fixtures",
        {
            "id": fixture_id,
        },
    )


@mcp.tool()
async def get_fixture_statistics(fixture_id: int):
    """Get fixture live statistics from API-Football."""

    return await api_get(
        "/fixtures/statistics",
        {
            "fixture": fixture_id,
        },
    )


@mcp.tool()
async def get_fixture_events(fixture_id: int):
    """Get goals, cards, substitutions and VAR events."""

    return await api_get(
        "/fixtures/events",
        {
            "fixture": fixture_id,
        },
    )


@mcp.tool()
async def get_fixture_lineups(fixture_id: int):
    """Get lineups and formations."""

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
    """Get recent head-to-head matches."""

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
    """Get latest matches for a team."""

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
    """Get injury information."""

    return await api_get(
        "/injuries",
        {
            "fixture": fixture_id,
        },
    )


@mcp.tool()
async def get_prematch_odds(fixture_id: int):
    """Get pre-match bookmaker odds."""

    return await api_get(
        "/odds",
        {
            "fixture": fixture_id,
        },
    )


@mcp.tool()
async def get_live_odds(fixture_id: int):
    """Get live bookmaker odds."""

    return await api_get(
        "/odds/live",
        {
            "fixture": fixture_id,
        },
    )

# ==================================================
# HIDDEN SIGNAL - ZYLA MATCH ANALYZER
# ==================================================

def _dedupe_zyla(value):
    """
    Remove exact duplicate items from Zyla responses.
    Works recursively with dictionaries and lists.
    """

    if isinstance(value, dict):
        return {
            key: _dedupe_zyla(item)
            for key, item in value.items()
        }

    if isinstance(value, list):
        cleaned = []
        seen = set()

        for item in value:
            item = _dedupe_zyla(item)

            try:
                marker = repr(item)
            except Exception:
                marker = str(item)

            if marker not in seen:
                seen.add(marker)
                cleaned.append(item)

        return cleaned

    return value


@mcp.tool()
async def analyze_zyla_match(match_id: str):
    """
    Collect all available live data for one Zyla match:
    details, statistics, events and live odds.

    Returns one cleaned data package for Hidden Signal analysis.
    """

    details = await zyla_get(
        23859,
        "get+match+details",
        {
            "match_id": match_id,
        },
    )

    stats = await zyla_get(
        23861,
        "get+match+stats",
        {
            "match_id": match_id,
        },
    )

    summary = await zyla_get(
        23860,
        "get+match+summary",
        {
            "match_id": match_id,
        },
    )

    odds = await zyla_get(
        23865,
        "get+match+odds",
        {
            "match_id": match_id,
        },
    )

    package = {
        "source": "hidden-signal-zyla",
        "match_id": match_id,
        "details": details,
        "statistics": stats,
        "events": summary,
        "odds": odds,
    }

    return _dedupe_zyla(package)
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
