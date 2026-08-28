import os
import httpx
from mcp.server import MCPServer

API_KEY = os.environ.get("API_FOOTBALL_KEY")
BASE_URL = "https://v3.football.api-sports.io"

mcp = MCPServer("Hidden Signal Live")
async def api_get(endpoint: str, params: dict | None = None):
    if not API_KEY:
        return {"error": "API_FOOTBALL_KEY is not configured"}

    headers = {
        "x-apisports-key": API_KEY,
    }

    async with httpx.AsyncClient(timeout=20.0) as client:
        response = await client.get(
            f"{BASE_URL}{endpoint}",
            headers=headers,
            params=params or {},
        )
        response.raise_for_status()
        return response.json()
            f"{BASE_URL}{endpoint}",
            headers=headers,
            params=params or {},
        )
        response.raise_for_status()
        return response.json()


@mcp.tool()
async def get_live_matches():
    """Get all football matches that are live right now."""
    return await api_get("/fixtures", {"live": "all"})


@mcp.tool()
async def get_fixture_details(fixture_id: int):
    """Get current details for a specific fixture."""
    return await api_get("/fixtures", {"id": fixture_id})


@mcp.tool()
async def get_fixture_statistics(fixture_id: int):
    """Get live match statistics: shots, possession, corners and more."""
    return await api_get("/fixtures/statistics", {"fixture": fixture_id})


@mcp.tool()
async def get_fixture_events(fixture_id: int):
    """Get match events: goals, cards, substitutions and VAR events."""
    return await api_get("/fixtures/events", {"fixture": fixture_id})


@mcp.tool()
async def get_fixture_lineups(fixture_id: int):
    """Get lineups and formations for a fixture."""
    return await api_get("/fixtures/lineups", {"fixture": fixture_id})


@mcp.tool()
async def get_head_to_head(team1_id: int, team2_id: int, last: int = 10):
    """Get recent head-to-head matches between two teams."""
    return await api_get(
        "/fixtures/headtohead",
        {"h2h": f"{team1_id}-{team2_id}", "last": last},
    )


@mcp.tool()
async def get_team_last_matches(team_id: int, season: int, last: int = 10):
    """Get a team's latest matches for form analysis."""
    return await api_get(
        "/fixtures",
        {"team": team_id, "season": season, "last": last},
    )


@mcp.tool()
async def get_injuries(fixture_id: int):
    """Get injuries and unavailable players for a fixture."""
    return await api_get("/injuries", {"fixture": fixture_id})


@mcp.tool()
async def get_prematch_odds(fixture_id: int):
    """Get available bookmaker odds for a fixture."""
    return await api_get("/odds", {"fixture": fixture_id})


@mcp.tool()
async def get_live_odds(fixture_id: int):
    """Get available live/in-play odds for a fixture."""
    return await api_get("/odds/live", {"fixture": fixture_id})


if __name__ == "__main__":
   mcp.run(
    transport="streamable-http",
    host="0.0.0.0",
    port=int(os.environ.get("PORT", "10000")),
)
