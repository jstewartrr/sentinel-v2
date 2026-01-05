"""
SENTINEL v2.0 - Persistent Infrastructure Monitor
Deployed to Azure East, Azure West, and AWS for triple redundancy.
Each instance monitors all gateways and can trigger remediation.
"""

import os
import json
import asyncio
import aiohttp
from datetime import datetime, timezone
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import snowflake.connector
from contextlib import asynccontextmanager
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("sentinel")

# Configuration
REGION = os.getenv("SENTINEL_REGION", "unknown")
CHECK_INTERVAL = int(os.getenv("CHECK_INTERVAL", "300"))  # 5 minutes default

# Snowflake connection
SF_CONFIG = {
    "account": os.getenv("SNOWFLAKE_ACCOUNT", "RDPIWHY-LIB16928"),
    "user": os.getenv("SNOWFLAKE_USER", "JOHN_CLAUDE"),
    "password": os.getenv("SNOWFLAKE_PASSWORD"),
    "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE", "SM_WH"),
    "database": os.getenv("SNOWFLAKE_DATABASE", "SOVEREIGN_MIND"),
    "schema": os.getenv("SNOWFLAKE_SCHEMA", "RAW"),
}

# Endpoints to monitor
GATEWAYS = {
    "azure-east": {
        "url": "https://sm-mcp-gateway.lemoncoast-87756bcf.eastus.azurecontainerapps.io",
        "health": "/health",
        "tools": "/tools"
    },
    "azure-west": {
        "url": "https://sm-mcp-gateway-west.nicecliff-a1c1a3b6.westus2.azurecontainerapps.io",
        "health": "/health", 
        "tools": "/tools"
    },
    "aws-east": {
        "url": "https://mamktfczh9.us-east-1.awsapprunner.com",
        "health": "/health",
        "tools": "/tools"
    },
    "vercel": {
        "url": "https://sm-mcp-gateway-vercel.vercel.app",
        "health": "/health",
        "tools": "/tools"
    },
    "cloudflare-lb": {
        "url": "https://sm-mcp-lb.jstewart-12a.workers.dev",
        "health": "/health",
        "tools": None
    }
}

# Critical MCPs to verify
CRITICAL_MCPS = [
    {"name": "snowflake-east", "url": "https://cvsm-snowflake-mcp-app-20260104.blackwave-ab9bb807.eastus.azurecontainerapps.io/mcp"},
    {"name": "azure-cli-east", "url": "https://cvsm-azure-cli-mcp-app-20260104.blackwave-ab9bb807.eastus.azurecontainerapps.io/mcp"},
    {"name": "snowflake-vercel", "url": "https://cvsm-snowflake-vercel.vercel.app/mcp"},
    {"name": "azure-cli-vercel", "url": "https://cvsm-azure-cli-vercel.vercel.app/mcp"},
]

app = FastAPI(title=f"Sentinel v2 - {REGION}")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

# Background task handle
monitor_task = None

def get_snowflake_conn():
    """Get Snowflake connection"""
    return snowflake.connector.connect(**SF_CONFIG)

async def check_endpoint(session: aiohttp.ClientSession, name: str, url: str, timeout: int = 10):
    """Check a single endpoint"""
    start = datetime.now(timezone.utc)
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=timeout)) as resp:
            elapsed = (datetime.now(timezone.utc) - start).total_seconds() * 1000
            body = await resp.text()
            return {
                "name": name,
                "url": url,
                "status_code": resp.status,
                "response_time_ms": round(elapsed),
                "healthy": resp.status == 200,
                "body_preview": body[:500] if body else None,
                "checked_at": start.isoformat()
            }
    except asyncio.TimeoutError:
        return {
            "name": name,
            "url": url,
            "status_code": 0,
            "response_time_ms": timeout * 1000,
            "healthy": False,
            "error": "TIMEOUT",
            "checked_at": start.isoformat()
        }
    except Exception as e:
        return {
            "name": name,
            "url": url,
            "status_code": 0,
            "response_time_ms": 0,
            "healthy": False,
            "error": str(e),
            "checked_at": start.isoformat()
        }

async def check_mcp_tools(session: aiohttp.ClientSession, name: str, url: str):
    """Check MCP tools endpoint using JSON-RPC"""
    start = datetime.now(timezone.utc)
    try:
        payload = {"jsonrpc": "2.0", "method": "tools/list", "id": 1}
        async with session.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=15)) as resp:
            elapsed = (datetime.now(timezone.utc) - start).total_seconds() * 1000
            if resp.status == 200:
                data = await resp.json()
                tools = data.get("result", {}).get("tools", [])
                return {
                    "name": name,
                    "url": url,
                    "healthy": True,
                    "tool_count": len(tools),
                    "response_time_ms": round(elapsed),
                    "checked_at": start.isoformat()
                }
            return {
                "name": name,
                "url": url,
                "healthy": False,
                "status_code": resp.status,
                "response_time_ms": round(elapsed),
                "checked_at": start.isoformat()
            }
    except Exception as e:
        return {
            "name": name,
            "url": url,
            "healthy": False,
            "error": str(e),
            "checked_at": start.isoformat()
        }

async def run_health_check():
    """Run comprehensive health check"""
    results = {
        "sentinel_region": REGION,
        "checked_at": datetime.now(timezone.utc).isoformat(),
        "gateways": [],
        "critical_mcps": [],
        "summary": {"healthy": 0, "unhealthy": 0, "total": 0}
    }
    
    async with aiohttp.ClientSession() as session:
        # Check gateways
        for gw_name, gw_config in GATEWAYS.items():
            health_url = gw_config["url"] + gw_config["health"]
            check = await check_endpoint(session, gw_name, health_url)
            
            # Get tool count if available
            if gw_config.get("tools") and check["healthy"]:
                tools_url = gw_config["url"] + gw_config["tools"]
                try:
                    async with session.get(tools_url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                        if resp.status == 200:
                            data = await resp.json()
                            check["tool_count"] = data.get("total_tools", len(data.get("tools", [])))
                except:
                    pass
            
            results["gateways"].append(check)
            results["summary"]["total"] += 1
            if check["healthy"]:
                results["summary"]["healthy"] += 1
            else:
                results["summary"]["unhealthy"] += 1
        
        # Check critical MCPs
        for mcp in CRITICAL_MCPS:
            check = await check_mcp_tools(session, mcp["name"], mcp["url"])
            results["critical_mcps"].append(check)
            results["summary"]["total"] += 1
            if check["healthy"]:
                results["summary"]["healthy"] += 1
            else:
                results["summary"]["unhealthy"] += 1
    
    return results

def log_to_snowflake(results: dict):
    """Log health check results to Snowflake"""
    try:
        conn = get_snowflake_conn()
        cur = conn.cursor()
        
        # Log each gateway
        for gw in results["gateways"]:
            cur.execute("""
                INSERT INTO MCP_HEALTH_LOG (GATEWAY, ENDPOINT, STATUS_CODE, RESPONSE_TIME_MS, 
                    HEALTHY_BACKENDS, TOTAL_BACKENDS, TOTAL_TOOLS, RAW_RESPONSE)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                gw["name"],
                gw["url"],
                str(gw.get("status_code", 0)),
                gw.get("response_time_ms", 0),
                1 if gw["healthy"] else 0,
                1,
                gw.get("tool_count", 0),
                json.dumps(gw)
            ))
        
        # Log critical MCPs
        for mcp in results["critical_mcps"]:
            cur.execute("""
                INSERT INTO MCP_HEALTH_LOG (GATEWAY, ENDPOINT, STATUS_CODE, RESPONSE_TIME_MS,
                    HEALTHY_BACKENDS, TOTAL_BACKENDS, TOTAL_TOOLS, RAW_RESPONSE)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                f"mcp-{mcp['name']}",
                mcp["url"],
                str(mcp.get("status_code", 200 if mcp["healthy"] else 0)),
                mcp.get("response_time_ms", 0),
                1 if mcp["healthy"] else 0,
                1,
                mcp.get("tool_count", 0),
                json.dumps(mcp)
            ))
        
        # Log summary to HIVE_MIND if issues detected
        if results["summary"]["unhealthy"] > 0:
            unhealthy_items = [g["name"] for g in results["gateways"] if not g["healthy"]]
            unhealthy_items += [m["name"] for m in results["critical_mcps"] if not m["healthy"]]
            
            cur.execute("""
                INSERT INTO HIVE_MIND (SOURCE, CATEGORY, WORKSTREAM, SUMMARY, PRIORITY, STATUS)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (
                f"SENTINEL_{REGION.upper()}",
                "HEALTH_ALERT",
                "INFRASTRUCTURE_MONITORING",
                f"SENTINEL ALERT [{REGION}]: {results['summary']['unhealthy']}/{results['summary']['total']} endpoints unhealthy. Issues: {', '.join(unhealthy_items)}",
                "HIGH" if results["summary"]["unhealthy"] > 2 else "MEDIUM",
                "ACTIVE"
            ))
        
        conn.commit()
        cur.close()
        conn.close()
        logger.info(f"Logged health check to Snowflake: {results['summary']}")
    except Exception as e:
        logger.error(f"Failed to log to Snowflake: {e}")

async def monitor_loop():
    """Background monitoring loop"""
    logger.info(f"Sentinel [{REGION}] starting monitoring loop (interval: {CHECK_INTERVAL}s)")
    while True:
        try:
            results = await run_health_check()
            log_to_snowflake(results)
            logger.info(f"Health check complete: {results['summary']['healthy']}/{results['summary']['total']} healthy")
        except Exception as e:
            logger.error(f"Monitor loop error: {e}")
        await asyncio.sleep(CHECK_INTERVAL)

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Start background monitoring on startup"""
    global monitor_task
    monitor_task = asyncio.create_task(monitor_loop())
    yield
    if monitor_task:
        monitor_task.cancel()

app.router.lifespan_context = lifespan

@app.get("/")
async def root():
    return {
        "service": "Sentinel v2",
        "region": REGION,
        "status": "operational",
        "check_interval_seconds": CHECK_INTERVAL
    }

@app.get("/health")
async def health():
    return {"status": "healthy", "region": REGION, "timestamp": datetime.now(timezone.utc).isoformat()}

@app.get("/check")
async def manual_check():
    """Run manual health check"""
    results = await run_health_check()
    log_to_snowflake(results)
    return results

@app.get("/status")
async def status():
    """Get current infrastructure status"""
    results = await run_health_check()
    return {
        "region": REGION,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "summary": results["summary"],
        "gateways": {g["name"]: {"healthy": g["healthy"], "tools": g.get("tool_count", "N/A")} for g in results["gateways"]},
        "mcps": {m["name"]: {"healthy": m["healthy"], "tools": m.get("tool_count", "N/A")} for m in results["critical_mcps"]}
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", "8080")))
