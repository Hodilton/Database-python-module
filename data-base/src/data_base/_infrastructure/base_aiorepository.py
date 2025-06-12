import aiomysql
from typing import Tuple, Optional, List, Union
from .aiomysql.aiomysql_repository import AioMySQLRepository

class BaseAioRepository(AioMySQLRepository):
    def __init__(self, pool: aiomysql.Pool, queries: dict, table_name: Optional[str] = None) -> None:
        super().__init__(pool, queries)
        self._table_name = table_name

    def _resolve_query(self, key_path: str) -> str:
        query = self._queries
        for key in key_path.split("."):
            query = query[key]
        return query

    async def execute(self, query_key: str, params: Tuple = ()) -> int:
        query = self._resolve_query(query_key)
        return await super().execute(query, params)

    async def fetch_one(self, query_key: str, params: Tuple = ()) -> Optional[dict]:
        query = self._resolve_query(query_key)
        result = await super().fetch(query, params, fetch_mode="one")
        return result if isinstance(result, dict) else None

    async def fetch_all(self, query_key: str, params: Tuple = ()) -> Union[List[dict], None]:
        query = self._resolve_query(query_key)
        result = await super().fetch(query, params, fetch_mode="all")
        return  result if isinstance(result, list) else None

    async def create(self) -> int:
        return await self.execute("create")

    async def drop(self) -> int:
        return await self.execute("drop")

    async def insert(self, data: Tuple) -> int:
        return await self.execute("insert", data)

    async def update(self, data: Tuple, condition: Tuple) -> int:
        return await self.execute("update", (*data, *condition))

    async def delete(self, name: str, condition: Tuple) -> int:
        query = self._resolve_query(f"delete.{name}")
        return await super().execute(query, condition)
