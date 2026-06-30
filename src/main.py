import argparse
import asyncio
import logging
import sys
from pathlib import Path

from src.config import Settings
from src.core.app import Application

logger = logging.getLogger(__name__)


def setup_logging(level: str = "INFO") -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper()),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


async def main() -> None:
    parser = argparse.ArgumentParser(description="Торговый бот")
    parser.add_argument(
        "--config",
        type=str,
        default="config/config.yaml",
        help="Путь к файлу конфигурации",
    )
    parser.add_argument(
        "--mode",
        type=str,
        choices=["signal", "real"],
        default=None,
        help="Режим работы (переопределяет конфиг)",
    )
    parser.add_argument(
        "--log-level",
        type=str,
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Уровень логирования",
    )
    args = parser.parse_args()

    setup_logging(args.log_level)

    config_path = Path(args.config)
    if not config_path.exists():
        logger.error(f"Файл конфигурации не найден: {config_path}")
        sys.exit(1)

    settings = Settings.from_yaml(config_path)
    if args.mode:
        settings.trading.mode = args.mode

    app = Application(settings)
    try:
        await app.start()
        await app.wait()
    except Exception:
        logger.exception("Критическая ошибка")
    finally:
        await app.stop()


if __name__ == "__main__":
    asyncio.run(main())
