from fastapi import FastAPI, UploadFile, File, HTTPException
from pydantic import BaseModel
from databse import Database

app = FastAPI()
database = Database()

class FileUpload(BaseModel):
    file: UploadFile

@app.post("/upload/")
async def upload_file(file: UploadFile = File(...)):
    database.insert_transactions_from_pdf(file.file)
    return {"message": "File uploaded successfully"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
