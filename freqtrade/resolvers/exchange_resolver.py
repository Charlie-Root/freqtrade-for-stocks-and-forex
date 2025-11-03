"""
This module loads custom exchanges
"""

import logging
from inspect import isclass
from typing import Any

import freqtrade.exchange as exchanges
from freqtrade.constants import Config, ExchangeConfig
from freqtrade.exchange import MAP_EXCHANGE_CHILDCLASS, Exchange
from freqtrade.resolvers.iresolver import IResolver


logger = logging.getLogger(__name__)


class ExchangeResolver(IResolver):
    """
    This class contains all the logic to load a custom exchange class
    """

    object_type = Exchange

    @staticmethod
    def load_exchange(
        config: Config,
        *,
        exchange_config: ExchangeConfig | None = None,
        validate: bool = True,
        load_leverage_tiers: bool = False,
    ) -> Exchange:
        """
        Load the custom class from config parameter
        :param exchange_name: name of the Exchange to load
        :param config: configuration dictionary
        """
        exchange_name: str = config["exchange"]["name"]
        # Map exchange name to avoid duplicate classes for identical exchanges
        exchange_name = MAP_EXCHANGE_CHILDCLASS.get(exchange_name, exchange_name)
        exchange_name = exchange_name.title()
        exchange = None
        try:

            exchange = ExchangeResolver._load_exchange(
                exchange_name,
                kwargs={
                    "config": config,
                    "validate": validate,
                    "exchange_config": exchange_config,
                    "load_leverage_tiers": load_leverage_tiers,
                },
            )
        except ImportError as e:
            logger.info(
                f"No {exchange_name} specific subclass found. Using the generic class instead."
            )
            logger.info("Make sure your exchange is supported by CCXT.")
            # log the import error at debug level
            logger.debug(f"ImportError details: {e}", exc_info=True)
        except Exception as e:
            logger.warning(
                f"Failed to instantiate {exchange_name} exchange class: {e}. Using the generic class instead."
            )
            logger.debug(f"Instantiation error details: {e}", exc_info=True)
        if not exchange:
            exchange = Exchange(
                config,
                validate=validate,
                exchange_config=exchange_config,
            )
        return exchange

    @staticmethod
    def _load_exchange(exchange_name: str, kwargs: dict) -> Exchange:
        """
        Loads the specified exchange.
        Only checks for exchanges exported in freqtrade.exchanges
        :param exchange_name: name of the module to import
        :return: Exchange instance or None
        """

        logger.debug(f"Available exchanges in freqtrade.exchange: {[name for name in dir(exchanges) if not name.startswith('_')]}")

        try:
            ex_class = getattr(exchanges, exchange_name)
        except AttributeError as e:
            logger.error(f"Exchange '{exchange_name}' not found in freqtrade.exchanges: {e}")
            # Pass and raise ImportError instead
            raise ImportError(f"Exchange '{exchange_name}' not found")

        try:
            exchange = ex_class(**kwargs)
            if exchange:
                logger.info(f"Using resolved exchange '{exchange_name}'...")
                return exchange
            else:
                logger.info(f"Exchange '{exchange_name}' could not be instantiated...")
        except Exception as e:
            logger.warning(f"Failed to instantiate {exchange_name} exchange class: {e}")
            logger.debug(f"Instantiation error details: {e}", exc_info=True)
            # Pass and raise ImportError to trigger fallback
            pass

        logger.info(f"Failed to load Exchange '{exchange_name}'...")

        raise ImportError(
            f"Impossible to load Exchange '{exchange_name}'. This class does not exist "
            "or contains Python code errors."
        )

    @classmethod
    def search_all_objects(
        cls, config: Config, enum_failed: bool, recursive: bool = False
    ) -> list[dict[str, Any]]:
        """
        Searches for valid objects
        :param config: Config object
        :param enum_failed: If True, will return None for modules which fail.
            Otherwise, failing modules are skipped.
        :param recursive: Recursively walk directory tree searching for strategies
        :return: List of dicts containing 'name', 'class' and 'location' entries
        """
        result = []
        for exchange_name in dir(exchanges):
            exchange = getattr(exchanges, exchange_name)
            if isclass(exchange) and issubclass(exchange, Exchange):
                result.append(
                    {
                        "name": exchange_name,
                        "class": exchange,
                        "location": exchange.__module__,
                        "location_rel: ": exchange.__module__.replace("freqtrade.", ""),
                    }
                )
        return result
