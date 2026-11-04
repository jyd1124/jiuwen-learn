# coding: utf-8
# Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved

"""Simple FastAPI Agent Example

A standalone Python package that can be deployed directly.
"""

import argparse
import uvicorn
from fastapi import FastAPI
from fastapi.responses import JSONResponse

app = FastAPI(title="Simple Agent", version="1.0.0")


@app.get("/")
async def root():
    """Root endpoint"""
    return JSONResponse({
        "message": "Hello World from simple_agent!",
        "status": "running"
    })


@app.get("/health")
async def health():
    """Health check"""
    return JSONResponse({
        "status": "healthy"
    })


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description="Simple FastAPI Agent")
    parser.add_argument("--host", default="127.0.0.1", help="Host to bind to")
    parser.add_argument("--port", type=int, default=8090, help="Port to bind to")
    args = parser.parse_args()

    uvicorn.run(
        app,
        host=args.host,
        port=args.port,
        log_level="info"
    )
