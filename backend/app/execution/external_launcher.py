import logging
from pathlib import Path
from typing import List, Optional
from app.execution.nina_client import nina_client

logger = logging.getLogger("ExternalLauncher")


class ExternalScriptLauncher:
    """
    Запускает внешние batch/PowerShell скрипты через N.I.N.A.
    """

    async def run_script(
        self,
        script_path: Path,
        args: List[str] = None,
        wait_for_completion: bool = True,
        timeout: int = 60,
    ) -> dict:
        """
        Запускает внешний скрипт.
        """
        if not script_path.exists():
            logger.error(f"❌ Script not found: {script_path}")
            return {"status": "error", "message": "File not found"}

        logger.info(f"📜 Launching external script: {script_path.name}")

        try:
            payload = {
                "path": str(script_path),
                "arguments": " ".join(args) if args else "",
                "waitForExit": wait_for_completion,
                "timeout": timeout,
            }

            response = await nina_client.post(
                "script/external/execute", json_data=payload
            )

            logger.info(f"✅ External script completed: {response}")
            return response
        except Exception as e:
            logger.error(f"❌ Failed to launch external script: {e}")
            return {"status": "error", "message": str(e)}


external_launcher = ExternalScriptLauncher()
