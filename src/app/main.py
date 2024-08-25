import asyncio
import os
import uuid
from hachoir.metadata import extractMetadata
from datetime import datetime

from fastapi.responses import RedirectResponse, ORJSONResponse, Response, FileResponse
from fastapi import (
    Depends,
    FastAPI,
    status,
    HTTPException,
    File,
    UploadFile,
)
import logging


from sql_driver import SqlDriver
from cloud_driver import CloudDriver
from config import CFG



logger = logging.getLogger(__name__)

app = FastAPI()
db_driver = SqlDriver()
cloud = CloudDriver()

@app.get('/', response_class=RedirectResponse)
def home_page():
    return '/docs'

@app.on_event('startup')
async def startup_event():
    asyncio.create_task(delete_data())


@app.post("/det_file_by_uid", status_code=status.HTTP_200_OK)
async def det_file_by_uid(filename):
    
    if "." in filename: 
        try:
             if not os.path.isfile(os.path.join(CFG.UPLOAD_DIRECTORY, filename)):
                try:
                    if cloud.download(filename): # если записи нет в локальном хранилище, то загружаем его из облака
                        FileResponse(os.path.join(CFG.UPLOAD_DIRECTORY, filename))
                except Exception as error:
                    raise HTTPException(status_code=404, detail=f"Error: {error}")
             else:
                 return FileResponse(os.path.join(CFG.UPLOAD_DIRECTORY, filename))
        except Exception as error:
            raise HTTPException(status_code=404, detail=f"Error: {error}")
    else:
        format = db_driver.get_format_by_uid(filename) #Если расширение файла не указано, то получаем его из бд
        if format is None:
            raise HTTPException(status_code=404, detail="File not found")
        else:
            if not os.path.isfile(os.path.join(CFG.UPLOAD_DIRECTORY, filename+"."+format)):
                try:
                    if cloud.download(filename+"."+format):
                        FileResponse(os.path.join(CFG.UPLOAD_DIRECTORY, filename+"."+format))
                except Exception as error:
                    raise HTTPException(status_code=404, detail=f"Error: {error}")
            else:
                return FileResponse(os.path.join(CFG.UPLOAD_DIRECTORY, filename+"."+format))


@app.post("/upload/")
async def upload_file(file: UploadFile = File(...)):
    logger.info(file)

    uid = make_UID()
    name = file.filename
    size = file.size
    timestamp = int(datetime.now().timestamp()*1000)
    content_type = file.content_type
    format = name.split(".")[-1]


    file_path = os.path.join(CFG.UPLOAD_DIRECTORY, uid+"."+format)

 
    with open(file_path, "wb") as buffer: #Сохраняем файл локально
        content = await file.read()
        buffer.write(content)

    data_to_db = {
        'uid': uid,
        'name': name,
        'size': size,
        'content_type': content_type,
        'upload_timestamp' : timestamp,
        "format" : format,
        "last_use": timestamp,
        "is_local": True

    }
        
    db_driver.insert_file(data_to_db) #Сохраняем метаданные в бд
    await cloud.upload(uid+"."+format, file) #Загружаем файл в облако
    
    return {"file": "Uploaded"}    

def make_UID():
    return str(uuid.uuid4())

async def delete_data(): # Функция для удаления старых файлов. Запускается раз в 10 секунд
    while True:
        old_files = db_driver.get_old_files()
        if old_files != []:
            file_list = [e[0]+'.'+e[1] for e in old_files]
            logger.info(f"delete list :{file_list}")
            delete_files(file_list)
        await asyncio.sleep(10)  


def delete_files(file_names):

    for file_name in file_names:
        file_path = os.path.join(CFG.UPLOAD_DIRECTORY, file_name)
        try:
            if os.path.isfile(file_path):
                os.remove(file_path)
                logger.info(f"Файл '{file_name}' успешно удален.")
            else:
                logger.warning(f"Файл '{file_name}' не найден.")
        except Exception as e:
            logger.warning(f"Произошла ошибка при удалении файла '{file_name}': {e}")        






