import os
import asyncio
import uuid
from fastapi import FastAPI
from fastapi.responses import FileResponse

from container_distribution import distribute_containers
from db_manage import ContainerDatabase
import config

db = ContainerDatabase()
db.load_data_from_csv(csv_file_path='combined_results.csv')

script_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(script_dir)

app = FastAPI()

@app.get("/distribute-containers")
async def container_placing(m: int = None, b: int = None):
    if m is None:
        return {
            "message": "m is required"
        }
    if b is None:
        return {
            "message": "b is required"
        }
    
    unique_id = str(uuid.uuid4())
    filename = f"containers_m{m}_b{b}_{unique_id}.csv"
    output_path = os.path.join(config.path_to_output, filename)
    try:
        await asyncio.to_thread(distribute_containers, m, b, output_file=output_path)
    except ValueError as e:
        return {"message": str(e)}
    
    return filename
