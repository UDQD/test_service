from typing import Any
# import yadisk_async
import os
import io
import logging

from config import CFG

logger = logging.getLogger(__name__)



# Заглушка API облака
class CloudDriver:

    def __init__(self) -> None:
        pass

    async def upload(self, filename, file):
        pass

    def download(self, filename):
        return 0
    

    
# API яндекс диска

# class CloudDriver:

#     def __init__(self) -> None:
#         self.disk = yadisk_async.YaDisk(token=CFG.Y_TOKEN)

#     async def upload(self, filename, file):
#         try:
#             path_to_save = os.path.join(CFG.CLOUD_PATH, filename)
#             file_stream = io.BytesIO(file.read())
#             await self.disk.upload(file_stream, f'/{path_to_save}')
#         except Exception as error:
#             logger.warning(f"error in cloud upload: {error}")

#     def download(self, filename):
#         try:
#             self.disk.download(os.path.join(CFG.CLOUD_PATH,filename), os.path.join(CFG.UPLOAD_DIRECTORY),filename)
#             return 1
#         except Exception as error:
#             logger.warning(f"error in cloud download: {error}")
#             return 0 