from fastapi import FastAPI, Request, Form, Response, Query
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from pathlib import Path
import urllib.parse

from starlette.responses import RedirectResponse

# from shared_endpoints import router as offers_router  # ADD THIS


app = FastAPI()

# Add CORS middleware for dashboard API
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In production, specify your dashboard URL
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Try to connect to MongoDB with timeout
MONGODB_AVAILABLE = False
store = None

try:
    from ScrapeTargetStore import ScrapeTargetStore

    store = ScrapeTargetStore()
    MONGODB_AVAILABLE = True
    print("✓ MongoDB is ready")
except Exception as e:
    print(f"✗ MongoDB failed: {e}")
    print("  Server will run in VIEW-ONLY mode with mock data")

Path("static").mkdir(exist_ok=True)
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")
templates.env.filters["urlencode"] = lambda v: urllib.parse.quote(str(v))

# Mock data for when MongoDB is not available
MOCK_TARGETS = []

# Import dashboard router
try:
    from dashboard import router as dashboard_router

    # app.include_router(offers_router)  # ADD THIS

    app.include_router(dashboard_router)
    print("✓ Dashboard API endpoints loaded")
    print("✓ shared_endpoints API endpoints loaded")
except Exception as e:
    print(f"✗ Dashboard API failed to load: {e}")


@app.get("/", response_class=HTMLResponse)
async def home(request: Request):
    """Redirect home to dashboard"""
    return RedirectResponse(url="/dashboard")

@app.get("/api/targets")
async def get_targets(
        search: str = Query("", description="Search query for username or platform"),
        view_filter: str = Query("active", description="Filter: 'active', 'inactive', or 'all'"),
        page: int = Query(1, ge=1, description="Page number"),
        limit: int = Query(50, ge=10, le=200, description="Items per page")
):
    """API endpoint for searching and paginating targets"""
    if not MONGODB_AVAILABLE:
        return JSONResponse({
            "targets": MOCK_TARGETS,
            "total": len(MOCK_TARGETS),
            "page": page,
            "limit": limit,
            "total_pages": 1
        })

    try:
        # Build query filter
        query_filter = {}

        # Filter by active status based on view_filter
        if view_filter == "active":
            query_filter["active"] = True
        elif view_filter == "inactive":
            query_filter["active"] = False
        # If view_filter == "all", don't add active filter

        # Add search filter if provided
        if search.strip():
            # Case-insensitive search on username (value) and platform
            query_filter["$or"] = [
                {"value": {"$regex": search, "$options": "i"}},
                {"platform": {"$regex": search, "$options": "i"}}
            ]

        # Get total count
        total = store.collection.count_documents(query_filter)

        # Calculate pagination
        skip = (page - 1) * limit
        total_pages = (total + limit - 1) // limit  # Ceiling division

        # Fetch targets with pagination
        targets = list(store.collection.find(
            query_filter,
            {"_id": 0}
        ).sort("added_at", -1).skip(skip).limit(limit))

        # Convert datetime objects to strings for JSON serialization
        for target in targets:
            if "added_at" in target and target["added_at"]:
                target["added_at"] = target["added_at"].isoformat()
            if "last_scraped" in target and target["last_scraped"]:
                target["last_scraped"] = target["last_scraped"].isoformat()

        return JSONResponse({
            "targets": targets,
            "total": total,
            "page": page,
            "limit": limit,
            "total_pages": total_pages,
            "has_more": page < total_pages
        })

    except Exception as e:
        print(f"MongoDB query failed: {e}")
        return JSONResponse({
            "error": str(e),
            "targets": [],
            "total": 0,
            "page": page,
            "limit": limit,
            "total_pages": 0
        }, status_code=500)


# @app.get("/dashboard", response_class=HTMLResponse)
# async def dashboard_page(request: Request):
#     """Serve the dashboard HTML page"""
#     return templates.TemplateResponse("dashboard.html", {
#         "request": request
#     })
@app.get("/dashboard", response_class=HTMLResponse)
async def dashboard_page(request: Request, message: str = None):
    """Serve the unified dashboard"""
    return templates.TemplateResponse("dashboard.html", {
        "request": request,
        "message": message
    })

@app.get("/dashboard/scraping", response_class=HTMLResponse)
async def scraping_dashboard(request: Request):
    return templates.TemplateResponse("scraping_dashboard.html", {"request": request})


@app.get("/dashboard/offers", response_class=HTMLResponse)
async def offers_dashboard(request: Request):
    return templates.TemplateResponse("offers_dashboard.html", {"request": request})


#
# @app.post("/add", response_class=HTMLResponse)
# async def add(request: Request, platform: str = Form(...), target: str = Form(...)):
#     if not MONGODB_AVAILABLE:
#         message = "⚠️ Database not available - running in view-only mode"
#     else:
#         try:
#             # Check if target already exists
#             existing_target = store.collection.find_one({
#                 "value": target,
#                 "platform": platform
#             })
#
#             if existing_target:
#                 # Target already exists
#                 status = "active" if existing_target.get("active", True) else "paused"
#                 message = f"⚠️ Target '{target}' already exists on {platform} (Status: {status})"
#             else:
#                 # Add new target
#                 store.add_target(
#                     platform=platform,
#                     target_type="profile",
#                     value=target,
#                     added_by="user"
#                 )
#                 message = f"✓ Added {target}"
#         except Exception as e:
#             message = f"✗ Error: {e}"
#
#     return templates.TemplateResponse("index.html", {
#         "request": request,
#         "message": message
#     })
@app.post("/add", response_class=HTMLResponse)
async def add(request: Request, platform: str = Form(...), target: str = Form(...)):
    if not MONGODB_AVAILABLE:
        message = "⚠️ Database not available - running in view-only mode"
    else:
        try:
            existing_target = store.collection.find_one({
                "value": target,
                "platform": platform
            })

            if existing_target:
                status = "active" if existing_target.get("active", True) else "paused"
                message = f"⚠️ Target '{target}' already exists on {platform} (Status: {status})"
            else:
                store.add_target(
                    platform=platform,
                    target_type="profile",
                    value=target,
                    added_by="user"
                )
                message = f"✓ Added {target}"
        except Exception as e:
            message = f"✗ Error: {e}"

    # Redirect to dashboard with message and targets tab active
    return templates.TemplateResponse("dashboard.html", {
        "request": request,
        "message": message
    })

@app.post("/toggle/{value}")
async def toggle(value: str):
    """Toggle active status (pause/resume) instead of deleting"""
    if not MONGODB_AVAILABLE:
        return Response(status_code=503)

    try:
        value = urllib.parse.unquote(value)
        target = store.collection.find_one({"value": value})

        if target:
            new_status = not target.get("active", True)
            store.collection.update_one(
                {"value": value},
                {"$set": {"active": new_status}}
            )
            return Response(status_code=200)
        return Response(status_code=404)
    except Exception as e:
        print(f"Toggle failed: {e}")
        return Response(status_code=500)


@app.delete("/delete/{value}")
async def delete(value: str):
    """Permanent delete - removes from database entirely"""
    if not MONGODB_AVAILABLE:
        return Response(status_code=503)

    try:
        value = urllib.parse.unquote(value)
        result = store.collection.delete_one({"value": value})
        return Response(status_code=204 if result.deleted_count else 404)
    except Exception as e:
        print(f"Delete failed: {e}")
        return Response(status_code=500)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="127.0.0.1", port=8000)