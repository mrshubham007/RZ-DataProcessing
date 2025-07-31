import azure.functions as func
import datetime   
import json
import logging
import psycopg2
import os
from urllib.parse import urlparse
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

app = func.FunctionApp()

def get_db_connection():
    """Get PostgreSQL database connection from environment variable"""
    database_url = os.getenv("DATABASE_URL")
    
    if not database_url:
        raise ValueError("DATABASE_URL environment variable is not set")
    
    # Parse the database URL
    parsed = urlparse(database_url)
    
    conn = psycopg2.connect(
        host=parsed.hostname,
        port=parsed.port,
        database=parsed.path[1:],  # Remove leading slash
        user=parsed.username,
        password=parsed.password,
        sslmode='require'
    )
    
    return conn

# 1. Get all sites from the sites table
@app.route(route="GetAllSites", auth_level=func.AuthLevel.FUNCTION)
def GetAllSites(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('GetAllSites function triggered.')

    try:
        # Get database connection
        with get_db_connection() as conn:
            cursor = conn.cursor()
            
            # Get all sites with site_name
            cursor.execute("""
                SELECT site_id, site_name 
                FROM sites 
                ORDER BY site_id
            """)
            
            sites = []
            for row in cursor.fetchall():
                sites.append({
                    "siteId": row[0],
                    "siteName": row[1]
                })
            
            cursor.close()

        # Return success response
        return func.HttpResponse(
            json.dumps({
                "status": "success",
                "message": "Successfully retrieved all sites",
                "sites": sites,
                "totalSites": len(sites),
                "timestamp": datetime.datetime.now().isoformat()
            }, indent=2),
            status_code=200,
            mimetype="application/json"
        )

    except psycopg2.Error as db_error:
        logging.error(f"PostgreSQL error: {str(db_error)}")
        return func.HttpResponse(
            json.dumps({
                "error": "Database connection failed",
                "details": str(db_error),
                "type": "database_error"
            }),
            status_code=500,
            mimetype="application/json"
        )
    
    except Exception as e:
        logging.error(f"General error: {str(e)}")
        return func.HttpResponse(
            json.dumps({
                "error": f"Function execution failed: {str(e)}",
                "type": "general_error"
            }),
            status_code=500,
            mimetype="application/json"
        )

# 2. Get active sites (sites that have metric data)
@app.route(route="GetActiveSites", auth_level=func.AuthLevel.FUNCTION)
def GetActiveSites(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('GetActiveSites function triggered.')

    try:
        # Get database connection
        with get_db_connection() as conn:
            cursor = conn.cursor()
            
            # Get active sites (sites that have data in MetricData table)
            cursor.execute("""
                SELECT DISTINCT s.site_id, s.site_name 
                FROM sites s
                INNER JOIN metricdata m ON s.site_id = m.siteid
                WHERE s.site_id IS NOT NULL 
                ORDER BY s.site_id
            """)
            
            sites = []
            for row in cursor.fetchall():
                sites.append({
                    "siteId": row[0],
                    "siteName": row[1]
                })
            
            # Get total row count for additional info
            cursor.execute("SELECT COUNT(*) FROM metricdata")
            total_records = cursor.fetchone()[0]
            
            cursor.close()

        # Return success response
        return func.HttpResponse(
            json.dumps({
                "status": "success",
                "message": "Successfully retrieved active sites",
                "activeSites": sites,
                "totalActiveSites": len(sites),
                "totalRecords": total_records,
                "timestamp": datetime.datetime.now().isoformat()
            }, indent=2),
            status_code=200,
            mimetype="application/json"
        )

    except psycopg2.Error as db_error:
        logging.error(f"PostgreSQL error: {str(db_error)}")
        return func.HttpResponse(
            json.dumps({
                "error": "Database connection failed",
                "details": str(db_error),
                "type": "database_error"
            }),
            status_code=500,
            mimetype="application/json"
        )
    
    except Exception as e:
        logging.error(f"General error: {str(e)}")
        return func.HttpResponse(
            json.dumps({
                "error": f"Function execution failed: {str(e)}",
                "type": "general_error"
            }),
            status_code=500,
            mimetype="application/json"
        )

# 3. Test function for basic connectivity (no database)
@app.route(route="ConnectivityTest", auth_level=func.AuthLevel.FUNCTION)
def ConnectivityTest(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('ConnectivityTest function triggered.')
    
    return func.HttpResponse(
        json.dumps({
            "message": "Azure Function is working!",
            "timestamp": datetime.datetime.now().isoformat(),
            "environment_check": {
                "DATABASE_URL": "Set" if os.getenv("DATABASE_URL") else "Not set"
            }
        }, indent=2),
        status_code=200,
        mimetype="application/json"
    )

# 4. Test database connectivity
@app.route(route="DatabaseTest", auth_level=func.AuthLevel.FUNCTION)
def DatabaseTest(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('DatabaseTest function triggered.')
    
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT version()")
            db_version = cursor.fetchone()[0]
            cursor.close()
            
        return func.HttpResponse(
            json.dumps({
                "message": "Database connection successful!",
                "database_version": db_version,
                "timestamp": datetime.datetime.now().isoformat()
            }, indent=2),
            status_code=200,
            mimetype="application/json"
        )
        
    except Exception as e:
        logging.error(f"Database test error: {str(e)}")
        return func.HttpResponse(
            json.dumps({
                "error": f"Database connection failed: {str(e)}",
                "type": "database_connection_error"
            }),
            status_code=500,
            mimetype="application/json"
        )

# 5. Fetch recent metric data with site names
@app.route(route="FetchRecentMetricData", auth_level=func.AuthLevel.FUNCTION)
def FetchRecentMetricData(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('FetchRecentMetricData function triggered.')
    
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            
            # Get recent data sample with site names
            cursor.execute("""
                SELECT 
                    m.siteid, 
                    s.site_name,
                    m.recordedat, 
                    COALESCE(m.total_emissions_s1_and_s2, 0) as emissions,
                    COALESCE(m.edc, 0) as energy_consumption,
                    COALESCE(m.pue, 0) as power_usage_effectiveness
                FROM metricdata m
                LEFT JOIN sites s ON m.siteid = s.site_id
                WHERE m.siteid IS NOT NULL 
                ORDER BY m.recordedat DESC
                LIMIT 50
            """)
            
            recent_data = []
            for row in cursor.fetchall():
                recent_data.append({
                    "siteId": row[0],
                    "siteName": row[1] if row[1] else "Unknown",
                    "recordedAt": row[2].strftime("%Y-%m-%d %H:%M:%S") if row[2] else "N/A",
                    "emissions": float(row[3]) if row[3] else 0,
                    "energyConsumption": float(row[4]) if row[4] else 0,
                    "powerUsageEffectiveness": float(row[5]) if row[5] else 0
                })
            
            cursor.close()

        return func.HttpResponse(
            json.dumps({
                "message": "🚀 Live data from our data centers!",
                "description": "Most recent metrics from MetricData table with site information",
                "recentData": recent_data,
                "dataPoints": len(recent_data),
                "retrievedAt": datetime.datetime.now().isoformat()
            }, indent=2),
            status_code=200,
            mimetype="application/json"
        )

    except Exception as e:
        logging.error(f"FetchRecentMetricData error: {str(e)}")
        return func.HttpResponse(
            json.dumps({
                "error": f"Failed to fetch recent data: {str(e)}",
                "type": "data_fetch_error"
            }),
            status_code=500,
            mimetype="application/json"
        )

# 6. Get site details by ID
@app.route(route="GetSiteById", auth_level=func.AuthLevel.FUNCTION)
def GetSiteById(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('GetSiteById function triggered.')
    
    # Get site_id from query parameters
    site_id = req.params.get('site_id')
    
    if not site_id:
        return func.HttpResponse(
            json.dumps({
                "error": "site_id parameter is required",
                "usage": "Add ?site_id=<site_id> to the URL"
            }),
            status_code=400,
            mimetype="application/json"
        )
    
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            
            # Get specific site details
            cursor.execute("""
                SELECT site_id, site_name 
                FROM sites 
                WHERE site_id = %s
            """, (site_id,))
            
            site_row = cursor.fetchone()
            
            if not site_row:
                cursor.close()
                return func.HttpResponse(
                    json.dumps({
                        "error": f"Site with ID '{site_id}' not found"
                    }),
                    status_code=404,
                    mimetype="application/json"
                )
            
            site_info = {
                "siteId": site_row[0],
                "siteName": site_row[1]
            }
            
            # Get recent metric data for this site
            cursor.execute("""
                SELECT 
                    recordedat, 
                    COALESCE(total_emissions_s1_and_s2, 0) as emissions,
                    COALESCE(edc, 0) as energy_consumption,
                    COALESCE(pue, 0) as power_usage_effectiveness
                FROM metricdata 
                WHERE siteid = %s 
                ORDER BY recordedat DESC
                LIMIT 10
            """, (site_id,))
            
            recent_metrics = []
            for row in cursor.fetchall():
                recent_metrics.append({
                    "recordedAt": row[0].strftime("%Y-%m-%d %H:%M:%S") if row[0] else "N/A",
                    "emissions": float(row[1]) if row[1] else 0,
                    "energyConsumption": float(row[2]) if row[2] else 0,
                    "powerUsageEffectiveness": float(row[3]) if row[3] else 0
                })
            
            cursor.close()

        return func.HttpResponse(
            json.dumps({
                "status": "success",
                "site": site_info,
                "recentMetrics": recent_metrics,
                "metricsCount": len(recent_metrics),
                "timestamp": datetime.datetime.now().isoformat()
            }, indent=2),
            status_code=200,
            mimetype="application/json"
        )

    except Exception as e:
        logging.error(f"GetSiteById error: {str(e)}")
        return func.HttpResponse(
            json.dumps({
                "error": f"Failed to get site details: {str(e)}",
                "type": "site_fetch_error"
            }),
            status_code=500,
            mimetype="application/json"
        )