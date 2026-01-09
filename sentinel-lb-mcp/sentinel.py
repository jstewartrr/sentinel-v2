#!/usr/bin/env python3
"""
Sovereign Mind Sentinel - Load Balanced MCP Monitor
====================================================
Monitors all backends, keeps connections active, auto-remediates issues,
and escalates to Hive Mind when human intervention is needed.

Runs every 30 seconds.
"""

import os
import sys
import json
import time
import asyncio
import aiohttp
import logging
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

# Configuration
CONFIG = {
    "check_interval_seconds": 30,
    "lb_mcp_url": os.getenv("LB_MCP_URL", "https://mcp.abbi-ai.com"),
    "gateway_v3_url": os.getenv("GATEWAY_V3_URL", "https://cv-sm-gateway-v3.lemoncoast-87756bcf.eastus.azurecontainerapps.io"),
    "snowflake_mcp_url": os.getenv("SNOWFLAKE_MCP_URL", "https://cv-sm-snowflake-20260105.lemoncoast-87756bcf.eastus.azurecontainerapps.io"),
    "resource_group": os.getenv("RESOURCE_GROUP", "SovereignMind-RG"),
}

# Backend to container mapping for auto-remediation
BACKEND_CONTAINER_MAP = {
    "m365": "cv-sm-m365-20260105",
    "asana": "cv-sm-asana-20260105",
    "github": "cv-sm-github-20260105",
    "dealcloud": "cv-sm-dealcloud-20260105",
    "dropbox": "cv-sm-dropbox-20260105",
    "make": None,
    "elevenlabs": "cv-sm-elevenlabs-20260105",
    "simli": "cv-sm-simli-20260105",
    "gemini": "cv-sm-gemini-20260105",
    "figma": "cv-sm-figma-20260105",
    "tailscale": "cv-sm-tailscale-20260105",
    "openai": "cv-sm-openai-20260105",
    "vercel": "cv-sm-vercel-20260105",
    "cloudflare": "cv-sm-cloudflare-20260105",
    "snowflake": "cv-sm-snowflake-20260105",
    "azure-cli": "cv-sm-azure-cli-20260105",
    "azure": "cv-sm-azure-cli-20260105",
    "googledrive": "cv-sm-google-drive-20260105",
    "google-drive": "cv-sm-google-drive-20260105",
    "grok": "grok-mcp",
    "vertex": "vertex-ai-mcp",
    "notebooklm": "notebooklm-mcp",
    "vectorizer": None,
    "gateway-v3": "cv-sm-gateway-v3",
}


async def check_gateway_health(session: aiohttp.ClientSession, url: str, name: str) -> Dict:
    health_url = f"{url}/health"
    try:
        async with session.get(health_url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
            if resp.status == 200:
                data = await resp.json()
                return {
                    "name": name, "url": url, "status": "healthy",
                    "total_tools": data.get("health", {}).get("total_tools", 0),
                    "backends": data.get("health", {}).get("backends", {}),
                    "error": None
                }
            else:
                return {"name": name, "url": url, "status": "error", "total_tools": 0, "backends": {}, "error": f"HTTP {resp.status}"}
    except asyncio.TimeoutError:
        return {"name": name, "url": url, "status": "timeout", "total_tools": 0, "backends": {}, "error": "Connection timeout"}
    except Exception as e:
        return {"name": name, "url": url, "status": "error", "total_tools": 0, "backends": {}, "error": str(e)}


async def restart_container(container_name: str) -> Dict:
    import subprocess
    logger.info(f"🔄 Attempting to restart container: {container_name}")
    try:
        cmd = ["az", "containerapp", "revision", "restart", "--name", container_name, "--resource-group", CONFIG["resource_group"]]
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
        if result.returncode == 0:
            logger.info(f"✅ Successfully restarted: {container_name}")
            return {"success": True, "container": container_name, "action": "restart"}
        else:
            cmd2 = ["az", "containerapp", "update", "--name", container_name, "--resource-group", CONFIG["resource_group"], "--min-replicas", "1"]
            result2 = subprocess.run(cmd2, capture_output=True, text=True, timeout=120)
            if result2.returncode == 0:
                logger.info(f"✅ Force-scaled container: {container_name}")
                return {"success": True, "container": container_name, "action": "scale"}
            else:
                logger.error(f"❌ Failed to restart: {container_name} - {result.stderr}")
                return {"success": False, "container": container_name, "error": result.stderr}
    except Exception as e:
        logger.error(f"❌ Exception restarting {container_name}: {e}")
        return {"success": False, "container": container_name, "error": str(e)}


async def log_to_hive_mind(session: aiohttp.ClientSession, category: str, summary: str, details: Dict, priority: str = "MEDIUM", tags: List[str] = None) -> bool:
    mcp_url = f"{CONFIG['snowflake_mcp_url']}/mcp"
    timestamp = datetime.now(timezone.utc).isoformat()
    details_json = json.dumps(details).replace("'", "''")
    tags_str = ",".join(tags or ["sentinel", "lb-mcp"])
    query = f"""INSERT INTO SOVEREIGN_MIND.RAW.HIVE_MIND (CREATED_AT, CATEGORY, SUMMARY, DETAILS, PRIORITY, SOURCE, WORKSTREAM, TAGS) VALUES ('{timestamp}', '{category}', '{summary.replace("'", "''")}', PARSE_JSON('{details_json}'), '{priority}', 'SENTINEL_LB_MCP', 'INFRASTRUCTURE', '{tags_str}')"""
    payload = {"jsonrpc": "2.0", "id": 1, "method": "tools/call", "params": {"name": "snowflake_execute_query", "arguments": {"query": query}}}
    try:
        async with session.post(mcp_url, json=payload, timeout=aiohttp.ClientTimeout(total=15)) as resp:
            if resp.status == 200:
                logger.info(f"📝 Logged to Hive Mind: {category} - {summary[:50]}...")
                return True
            else:
                logger.warning(f"⚠️ Hive Mind log failed: HTTP {resp.status}")
                return False
    except Exception as e:
        logger.warning(f"⚠️ Hive Mind log exception: {e}")
        return False


async def call_for_help(session: aiohttp.ClientSession, issue: str, details: Dict) -> None:
    await log_to_hive_mind(session, category="ALERT", summary=f"🚨 SENTINEL NEEDS HELP: {issue}", details={**details, "escalation_time": datetime.now(timezone.utc).isoformat(), "requires_human": True, "sentinel": "lb-mcp"}, priority="HIGH", tags=["sentinel", "escalation", "needs-help", "lb-mcp"])
    logger.error(f"🚨 ESCALATED TO HIVE MIND: {issue}")


async def remediate_backend(session: aiohttp.ClientSession, backend_name: str, error: str) -> bool:
    container = BACKEND_CONTAINER_MAP.get(backend_name.lower())
    if container is None:
        logger.info(f"⏭️ Backend {backend_name} has no mapped container - skipping remediation")
        return False
    result = await restart_container(container)
    if result.get("success"):
        await log_to_hive_mind(session, category="ACTION_ITEM", summary=f"✅ Auto-remediated {backend_name} by {result.get('action', 'restart')}", details={"backend": backend_name, "container": container, "action": result.get("action"), "original_error": error}, priority="MEDIUM", tags=["sentinel", "remediation", "success"])
        return True
    else:
        await call_for_help(session, f"Failed to remediate {backend_name}", {"backend": backend_name, "container": container, "original_error": error, "remediation_error": result.get("error")})
        return False


async def keep_connections_alive(session: aiohttp.ClientSession, health_data: Dict) -> None:
    backends = health_data.get("backends", {})
    for name, info in backends.items():
        if info.get("status") == "healthy":
            logger.debug(f"💓 Backend {name} alive with {info.get('tools', 0)} tools")


async def run_health_check() -> Dict:
    async with aiohttp.ClientSession() as session:
        lb_health = await check_gateway_health(session, CONFIG["lb_mcp_url"], "Load Balanced MCP")
        v3_health = await check_gateway_health(session, CONFIG["gateway_v3_url"], "Gateway V3")
        timestamp = datetime.now(timezone.utc).isoformat()
        issues = []
        remediations = []
        if lb_health["status"] == "healthy":
            for backend, info in lb_health.get("backends", {}).items():
                if info.get("status") != "healthy":
                    issues.append({"gateway": "lb_mcp", "backend": backend, "status": info.get("status"), "error": info.get("error")})
                    success = await remediate_backend(session, backend, info.get("error", "unknown"))
                    remediations.append({"backend": backend, "success": success})
        else:
            issues.append({"gateway": "lb_mcp", "backend": "gateway", "status": lb_health["status"], "error": lb_health["error"]})
        if v3_health["status"] != "healthy":
            issues.append({"gateway": "v3", "backend": "gateway", "status": v3_health["status"], "error": v3_health["error"]})
        if lb_health["status"] == "healthy":
            await keep_connections_alive(session, lb_health)
        if v3_health["status"] == "healthy":
            await keep_connections_alive(session, v3_health)
        healthy_count = sum(1 for b in lb_health.get("backends", {}).values() if b.get("status") == "healthy")
        total_count = len(lb_health.get("backends", {}))
        if issues:
            await log_to_hive_mind(session, category="HEALTH_CHECK", summary=f"⚠️ LB MCP: {healthy_count}/{total_count} backends healthy, {len(issues)} issues", details={"timestamp": timestamp, "lb_mcp_tools": lb_health.get("total_tools", 0), "v3_tools": v3_health.get("total_tools", 0), "issues": issues, "remediations": remediations}, priority="HIGH" if len(issues) > 3 else "MEDIUM", tags=["sentinel", "health-check", "issues"])
        else:
            await log_to_hive_mind(session, category="HEALTH_CHECK", summary=f"✅ LB MCP healthy: {lb_health.get('total_tools', 0)} tools, V3 backup: {v3_health.get('total_tools', 0)} tools", details={"timestamp": timestamp, "lb_mcp_tools": lb_health.get("total_tools", 0), "v3_tools": v3_health.get("total_tools", 0), "lb_backends": healthy_count, "v3_backends": len(v3_health.get("backends", {}))}, priority="LOW", tags=["sentinel", "health-check", "healthy"])
        return {"timestamp": timestamp, "lb_mcp": lb_health, "gateway_v3": v3_health, "issues": issues, "remediations": remediations}


async def main():
    logger.info("🛡️ Sentinel for Load Balanced MCP starting...")
    logger.info(f"   LB MCP URL: {CONFIG['lb_mcp_url']}")
    logger.info(f"   Gateway V3 URL: {CONFIG['gateway_v3_url']}")
    logger.info(f"   Check interval: {CONFIG['check_interval_seconds']}s")
    while True:
        try:
            result = await run_health_check()
            lb_status = "✅" if result["lb_mcp"]["status"] == "healthy" else "❌"
            v3_status = "✅" if result["gateway_v3"]["status"] == "healthy" else "❌"
            logger.info(f"🔍 Check complete: LB {lb_status} ({result['lb_mcp'].get('total_tools', 0)} tools) | V3 {v3_status} ({result['gateway_v3'].get('total_tools', 0)} tools) | Issues: {len(result['issues'])}")
        except Exception as e:
            logger.error(f"❌ Health check failed: {e}")
        await asyncio.sleep(CONFIG["check_interval_seconds"])


if __name__ == "__main__":
    asyncio.run(main())
