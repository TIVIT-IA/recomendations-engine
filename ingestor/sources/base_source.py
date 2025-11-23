# ingestor/src/sources/base_source.py
from abc import ABC, abstractmethod

class BaseSource(ABC):

    @abstractmethod
    async def fetch(self):
        """Debe retornar una lista de dicts con:
           {"raw": {...datos del trabajador...}, "source": "identificador_origen"}
        """
        pass
