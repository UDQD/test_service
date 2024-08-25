import logging
from sqlalchemy import create_engine, Column, Integer, String, MetaData, select, Boolean, update
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import NoResultFound
from datetime import datetime

from config import CFG

logger = logging.getLogger(__name__)

Base = declarative_base()


class File(Base):
    __tablename__ = CFG.TABLE

    uid = Column(Integer, primary_key=True)
    name = Column(String)
    size = Column(Integer)
    content_type = Column(String)
    upload_timestamp = Column(Integer)
    format = Column(String)
    last_use = Column(Integer)
    is_local = Column(Boolean)


class SqlDriver:
    def __init__(self) -> None:
        DATABASE_URI = f'postgresql://{CFG.USER}:{CFG.PASSWORD}@{CFG.DB_HOST}/{CFG.DB_NAME}'
        self.engine = create_engine(DATABASE_URI)
        
        metadata = MetaData()
        Session = sessionmaker(bind=self.engine)
        self.session = Session()

    def insert_file(self, data):

        try:

            file_entry = File(**data)
            

            self.session.add(file_entry)
            self.session.commit()

            logger.info("Запись добавлена успешно.")
        except Exception as e:
            self.session.rollback()  

            logger.warning(f"Произошла ошибка при добавлении записи: {e}")
        finally:
            self.session.close()  

    def get_format_by_uid(self, uid):

        try:
            # Получаем формат файла и обновляем last_use
            query = select(File).where(File.uid == uid)
            result = self.session.execute(query).scalar_one() 
            format = result.format
            result.last_use = int(datetime.now().timestamp()*1000)
            self.session.commit()
            return format
        except NoResultFound:
            return None  
        except Exception as e:
            logger.warning(f"An error occurred: {e}")
            return None
    
    def get_old_files(self):
        try:
            timerange = int(datetime.now().timestamp()*1000) - CFG.DELETE_PERIOD
            query = select(File).where(
                File.last_use < timerange,
                File.is_local.is_(True)
            )
            result = self.session.execute(query)
            entries_to_update = result.scalars().all()
            
            # Создание списка uid и формат
            updated_entries = [(entry.uid, entry.format) for entry in entries_to_update]
            uids = [e[0] for e in updated_entries]
            formats = [e[1] for e in updated_entries]
            
            # Обновление значений is_local на False
            if entries_to_update:
                update_stmt = update(File).where(File.uid.in_([entry for entry in uids])).values(is_local=False)
                self.session.execute(update_stmt)
                self.session.commit()
            
            # Возвращаем обновленные записи
            
            return updated_entries
        except Exception as error:
            logger.warning(f"Ошибка при поиске старых файлов: {error}")
    

