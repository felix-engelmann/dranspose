import aiohttp
import pytest

from dranspose.protocol import EnsembleState
from tests.fixtures import controller


@pytest.mark.asyncio
async def test_status(controller: None) -> None:
    async with aiohttp.ClientSession() as session:
        st = await session.get("http://localhost:5000/api/v1/status")
        state = await st.json()
        assert st.status == 200
