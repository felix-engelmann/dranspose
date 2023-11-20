import aiohttp
import pytest

from dranspose.protocol import EnsembleState
from tests.fixtures import controller

@pytest.mark.asyncio
async def test_status(controller):
    async with aiohttp.ClientSession() as session:
        st = await session.get("http://localhost:5000/api/v1/config")
        state = EnsembleState.model_validate(await st.json())
        print("content", state.ingesters)

        