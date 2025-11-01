# foreignexchange.py
import logging

from freqtrade.exchange.exchange import Exchange


logger = logging.getLogger(__name__)


class Foreignexchange(Exchange):
    def __init__(self, config, validate=True, exchange_config=None, load_leverage_tiers=False):
        super().__init__(
            config,
            exchange_config=exchange_config,
            validate=validate,
            load_leverage_tiers=load_leverage_tiers,
        )
        logger.info("Foreignexchange retrieved successfully.")

    @property
    def _ft_has_default(self):
        return {
            "always_require_api_keys": False,
        }

    @property
    def name(self):
        return "foreignexchange"

    def exchange_has(self, method):
        """
        Check if the exchange supports a specific method.
        """
        if method == "fetchTickers" and not hasattr(self, method):
            raise ValueError(
                "Exchange does not support dynamic whitelist in this configuration."
                "Please edit your config and either remove VolumePairList, or switch"
                " to using candles, and restart the bot."
            )
        return hasattr(self, method)
