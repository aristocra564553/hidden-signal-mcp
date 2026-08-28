import os
import httpx
from mcp.server import MCPServer


# =========================
# НАСТРОЙКИ API-FOOTBALL
# =========================

API_KEY = (os.environ.get("API_FOOTBALL_KEY") or "").strip()
BASE_URL = "https://v3.football.api-sports.io"

mcp = MCPServer("Hidden Signal Live")


# =========================
# ОСНОВНОЙ ЗАПРОС К API
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
                "diagnostic": {
                    "key_loaded": True,
                    "key_length": len(API_KEY),
                    "http_status": response.status_code,
                    "header_used": "x-apisports-key",
                    "base_url": BASE_URL,
                },
                "api_response": data,
            }

    except Exception as e:
        return {
            "error": str(e),
            "diagnostic": {
                "key_loaded": True,
                "key_length": len(API_KEY),
            },
        }


# =========================
# LIVE МАТЧИ
# =========================

@mcp.tool()
async def get_live_matches():
    """Get all football matches that are live right now."""
    return await api_get(
        "/fixtures",
        {
            "live": "all",
        },
    )


# =========================
# ДЕТАЛИ МАТЧА
# =========================

@mcp.tool()
async def get_fixture_details(fixture_id: int):
    """Get current details for a specific fixture."""
    return await api_get(
        "/fixtures",
        {
            "id": fixture_id,
        },
    )


# =========================
# LIVE СТАТИСТИКА
# =========================

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


# =========================
# СОБЫТИЯ МАТЧА
# =========================

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


# =========================
# СОСТАВЫ
# =========================

@mcp.tool()
async def get_fixture_lineups(fixture_id: int):
    """Get lineups, formations and available player information."""
    return await api_get(
        "/fixtures/lineups",
        {
            "fixture": fixture_id,
        },
    )


# =========================
# ЛИЧНЫЕ ВСТРЕЧИ
# =========================

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


# =========================
# ПОСЛЕДНИЕ МАТЧИ КОМАНДЫ
# =========================

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


# =========================
# ТРАВМЫ
# =========================

@mcp.tool()
async def get_injuries(fixture_id: int):
    """Get available injury information for a fixture."""

    return await api_get(
        "/injuries",
        {
            "fixture": fixture_id,
        },
    )


# =========================
# ПРЕМАТЧ КОЭФФИЦИЕНТЫ
# =========================

@mcp.tool()
async def get_prematch_odds(fixture_id: int):
    """Get available bookmaker pre-match odds for a fixture."""

    return await api_get(
        "/odds",
        {
            "fixture": fixture_id,
        },
    )


# =========================
# LIVE КОЭФФИЦИЕНТЫ
# =========================

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
# ЗАПУСК MCP
# =========================

if __name__ == "__main__":
    port = int(os.environ.get("PORT", "10000"))

    mcp.run(
        transport="streamable-http",
        host="0.0.0.0",
        port=port,
    )
