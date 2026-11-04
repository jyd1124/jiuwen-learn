# coding: utf-8
# Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved

"""Simple FastAPI Plugin Example

A standalone Python file that can be deployed directly as a plugin.
"""

import argparse
import uvicorn
from fastapi import FastAPI
from fastapi.responses import JSONResponse

# Create FastAPI app
app = FastAPI(title="Simple Plugin", version="1.0.0")


@app.get("/")
async def root():
    """Root endpoint"""
    return JSONResponse({
        "message": "Hello World from simple_plugin!",
        "type": "plugin",
        "status": "running"
    })


@app.get("/tools")
async def list_tools():
    """List available tools"""
    return JSONResponse({
        "tools": [
            {"name": "current_weather", "description": "Query current weather"},
            {"name": "weather_forecast", "description": "Query weather forecast"}
        ]
    })


@app.get("/tool/{tool_name}")
async def call_tool(tool_name: str):
    """Call a tool"""
    return JSONResponse({
        "tool": tool_name,
        "result": f"Executed {tool_name} successfully"
    })


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description="Simple FastAPI Plugin")
    parser.add_argument("--host", default="127.0.0.1", help="Host to bind to")
    parser.add_argument("--port", type=int, default=8091, help="Port to bind to")
    args = parser.parse_args()

    # Start uvicorn server
    uvicorn.run(
        app,
        host=args.host,
        port=args.port,
        log_level="info"
    )


if __name__ == "__main__":
    main()
