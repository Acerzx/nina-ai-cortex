import logging
import base64
from app.execution.nina_client import nina_client

logger = logging.getLogger("PythonBridge")


class PythonBridge:
    """
    Выполняет Python-код внутри N.I.N.A. через плагин nina.plugin.python.
    """

    async def execute(self, code: str, description: str = "AI Script") -> dict:
        """
        Отправляет Python-код на выполнение в N.I.N.A.
        Код передается в Base64 для избежания проблем с экранированием.
        """
        logger.info(f"🐍 Executing Python script: {description}")
        logger.debug(f"Code:\n{code}")

        try:
            # Кодируем в Base64
            code_b64 = base64.b64encode(code.encode("utf-8")).decode("utf-8")

            response = await nina_client.post(
                "script/python/execute",
                json_data={"script": code_b64, "description": description},
            )

            logger.info(f"✅ Python script executed successfully")
            return response
        except Exception as e:
            logger.error(f"❌ Failed to execute Python script: {e}")
            return {"status": "error", "message": str(e)}

    async def inject_shutdown_intercept(self, delay_minutes: int = 5):
        """
        Специальный метод: инжектит скрипт для отмены Shutdown PC.
        Создает MessageBox или задержку внутри N.I.N.A.
        """
        code = f"""
import System.Windows
from NINA.Core.Utility.Notification import Success

# Отмена стандартного shutdown через создание задержки
System.Threading.Thread.Sleep({delay_minutes * 60 * 1000})
Success("Shutdown intercepted by AI Cortex. Delayed for {delay_minutes} minutes.")
"""
        return await self.execute(code, "Shutdown Interceptor")


python_bridge = PythonBridge()
