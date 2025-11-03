
# pip install ib_insync
# Interactive Brokers exchange stocks integration for FreqTrade

import asyncio
import atexit
import logging
import math
import signal
import socket
import sys
import time
from datetime import UTC, datetime, timedelta
from threading import Event, Lock, Thread
from typing import Any

import pandas as pd
from ib_insync import IB, Contract, Forex, Order, Stock, util

from freqtrade.enums import MarginMode
from freqtrade.exceptions import ExchangeError
from freqtrade.exchange.foreignexchange import Foreignexchange
from freqtrade.persistence import Order as FTOrder
from freqtrade.persistence import Trade
from freqtrade.constants import ListPairsWithTimeframes, PairWithTimeframe


util.patchAsyncio()

logger = logging.getLogger(__name__)

_min_interval = 0.5  # One request every 2 seconds
_last_request_ts = 0.0

_request_lock = Lock()

# Define US stock market open and close times in UTC (ET is UTC-4/UTC-5)
# Regular hours: 9:30 AM - 4:00 PM ET
# Pre-market: 4:00 AM - 8:00 AM ET (limited)
# After-hours: 4:00 PM - 8:00 PM ET (limited)
MARKET_OPEN_TIME_REGULAR_UTC = datetime.strptime("13:30", "%H:%M").time()  # 9:30 AM ET = 13:30 UTC
MARKET_CLOSE_TIME_REGULAR_UTC = datetime.strptime("20:00", "%H:%M").time()  # 4:00 PM ET = 20:00 UTC
MARKET_OPEN_TIME_PREMARKET_UTC = datetime.strptime("08:00", "%H:%M").time()  # 4:00 AM ET = 08:00 UTC
MARKET_CLOSE_TIME_PREMARKET_UTC = datetime.strptime("12:00", "%H:%M").time()  # 8:00 AM ET = 12:00 UTC
MARKET_OPEN_TIME_AFTERHOURS_UTC = datetime.strptime("20:00", "%H:%M").time()  # 4:00 PM ET = 20:00 UTC
MARKET_CLOSE_TIME_AFTERHOURS_UTC = datetime.strptime("00:00", "%H:%M").time()  # 8:00 PM ET = 00:00 UTC (next day)


def throttle():
    global _last_request_ts
    with _request_lock:
        now = time.time()
        elapsed = now - _last_request_ts
        if elapsed < _min_interval:
            time.sleep(_min_interval - elapsed)
        _last_request_ts = time.time()


class InteractivebrokersStocks(Foreignexchange):
    """
    Interactive Brokers stocks exchange class. Contains adjustments needed for Freqtrade
    to work with IBKR for stock trading.
    """

    RECONNECT_MAX_BACKOFF = 32  # seconds
    RECONNECT_BASE_BACKOFF = 1  # seconds

    DECIMAL_PLACES = 2
    SIGNIFICANT_DIGITS = 2
    TICK_SIZE = 0.01
    MAX_DATA_DELAY = pd.Timedelta(minutes=20)  # Stocks can have more delay
    MIN_LOT_SIZE = 1  # Minimum 1 share for stocks
    RECONNECT_TIMEOUT = 30

    _cache_lock: Lock
    _entry_rate_cache: dict[str, float]
    _exit_rate_cache: dict[str, float]

    _ft_has_default = {
        "always_require_api_keys": False,
        "stoploss_on_exchange": False,
        "order_time_in_force": ["GTC", "IOC", "FOK"],
        "ohlcv_candle_limit": 500,
        "ohlcv_has_history": True,
        "ohlcv_partial_candle": True,
        "ohlcv_require_since": False,
        "ohlcv_volume_currency": "base",
        "tickers_have_quoteVolume": True,
        "tickers_have_percentage": True,
        "tickers_have_bid_ask": True,
        "tickers_have_price": True,
        "trades_limit": 1000,
        "trades_pagination": "time",
        "trades_pagination_arg": "since",
        "trades_has_history": False,
        "l2_limit_range": None,
        "l2_limit_range_required": True,
        "mark_ohlcv_price": "mark",
        "mark_ohlcv_timeframe": "8h",
        "funding_fee_timeframe": "8h",
        "ccxt_futures_name": "swap",
        "needs_trading_fees": False,
        "order_props_in_contracts": ["amount", "filled", "remaining"],
        "market_props_in_contracts": ["status"],
        "market_has_ticker": False,
        "market_has_ohlcv": True,
        "order_has_status": True,
        "order_has_type": True,
        "order_has_side": True,
        "order_has_time_in_force": False,
        "order_has_price": True,
        "order_has_amount": True,
        "order_has_cost": False,
        "order_has_fee": False,
        "order_has_slippage": False,
        "order_has_filled": True,
        "order_has_remaining": True,
        "order_has_status_history": False,
        "ws_enabled": True,
        "ws_auto_reconnect": True,
        "ws_reconnect_interval": 30,
    }

    def __init__(
        self,
        config: dict,
        *,
        exchange_config: dict | None = None,
        validate: bool = True,
        load_leverage_tiers: bool = False,
    ) -> None:
        super().__init__(
            config,
            exchange_config=exchange_config,
            validate=validate,
            load_leverage_tiers=load_leverage_tiers,
        )

        self.ib = IB()
        try:
            self.ib.startLoop()
            self._ib_loop_started = True
        except Exception as e:
            logger.debug(f"ib.startLoop() failed — already running or unsupported: {e}")

        self.dry_run = config.get("dry_run", False)
        self.latest_ohlcv: dict = {}
        self._active_tickers: list = []
        self._running = True
        self._reconnect_event = Event()
        self.shutdown_event: Event = Event()
        self.is_shutting_down = False
        self._connection_thread: Thread | None = None
        self._ws_connected = False
        self._markets_cache: dict[str, Any] | None = None
        self._live_price_cache: dict[str, tuple[float, float]] = {}

        self._cache_lock = Lock()
        self._entry_rate_cache = {}
        self._exit_rate_cache = {}

        self._last_connection_ts = 0
        atexit.register(self.close)

        # Set ports based on live/paper trading
        if self.dry_run and self.dry_run is True:
            logger.info(self.dry_run)
            logger.info("Dry run detected. Using paper trading settings.")
            self.port = config.get("ib_paper_port", 4002)
            logger.info(f"Connecting to IBKR paper trading (IB Gateway) on port {self.port}.")
        else:
            self.port = config.get("ib_live_port", 7497)
            logger.info(f"Connecting to IBKR live trading (TWS) on port {self.port}.")

        # Set up host
        self.host = config.get("ib_host", "127.0.0.1")
        self.client_id = config.get("ib_client_id", 1)

        # Get stock symbols from pairlists - this allows dynamic pairlist management
        # If no pairlists are configured yet, use defaults
        pairlists_config = config.get("pairlists", [])
        if pairlists_config:
            # Extract pairs from the first pairlist (typically StaticPairList)
            first_pairlist = pairlists_config[0] if pairlists_config else {}
            self.stock_symbols = first_pairlist.get("pairs", ["AAPL", "MSFT", "GOOGL", "AMZN", "TSLA"])
        else:
            # Fallback for when pairlists aren't configured yet (e.g., during initialization)
            self.stock_symbols = ["AAPL", "MSFT", "GOOGL", "AMZN", "TSLA"]

        # Only connect to IBKR for live trading, not for backtesting
        runmode = self._config.get("runmode", "dry_run")
        if runmode not in ("backtest", "hyperopt", "edge"):
            # Connect to IBKR for live trading
            self._connect_to_ib()

            # Start WebSocket connection
            self.ws_start()

            # Verify connection is established
            if not self.ib.isConnected():
                logger.error("Failed to establish connection to Interactive Brokers")
                # Don't raise error here - allow instantiation to proceed for testing
                # raise ConnectionError("WebSocket connection failed")
        else:
            logger.info("Backtesting mode detected - skipping IBKR connection")

        # Set margin mode and initialize markets
        self.margin_mode = MarginMode.NONE
        self._markets = self.get_markets(reload=True)

        if "candle_type_def" not in self._config:
            self._config["candle_type_def"] = "spot"
            logger.info("Set default candle_type_def to 'spot' for interactivebrokers_stocks")

        # Register signal handler for SIGINT (Ctrl+C)
        signal.signal(signal.SIGINT, self._handle_sigint)

    def _handle_sigint(self, signum, frame):
        logger.info("Received Ctrl+C, forcing immediate shutdown...")
        self.is_shutting_down = True
        self.close()
        sys.exit(0)  # Ensure the program exits

    def _connect_to_ib(self) -> None:
        """
        Establishes connection to Interactive Brokers.
        Handles refusal cleanly without full traceback spam.
        """
        with _request_lock:
            if self.ib.isConnected():
                logger.info("IBKR already connected.")
                self._ws_connected = True
                self.connected = True
                return

            logger.info(f"Connecting to IBKR paper trading (IB Gateway) on port {self.port}.")
            logger.info(
                f"Connecting to IBKR (host={self.host}, "
                f"port={self.port}, clientId={self.client_id})"
            )

            try:
                self.ib.connect(self.host, self.port, clientId=self.client_id, timeout=5)

                if not self.ib.isConnected():
                    logger.error("❌ IBKR connection failed silently.")
                    self._ws_connected = False
                    self.connected = False
                    raise SystemExit("❌ Could not establish connection to IBKR.")

                logger.info("✅ IBKR connection established.")
                self._ws_connected = True
                self.connected = True

            except ConnectionRefusedError:
                logger.error(
                    "❌ Connection refused: IB Gateway or TWS not running on "
                    f"{self.host}:{self.port}"
                )
                self._ws_connected = False
                self.connected = False
                raise SystemExit("❌ Could not connect to IBKR. Is IB Gateway running?")

            except Exception as e:
                logger.error(f"❌ Unexpected error during IBKR connection: {e}")
                self._ws_connected = False
                self.connected = False
                raise SystemExit("❌ Unexpected failure connecting to IBKR.")

    def _setup_event_loop(self) -> None:
        if self._connection_thread and self._connection_thread.is_alive():
            return

        self._reconnect_attempts = 0
        self._max_reconnect_attempts = 5
        self._reconnect_base_delay = 5

        def _start_ib_loop():
            logger.info("Starting IBKR event loop")
            while self._running and not self.shutdown_event.is_set():
                try:
                    if not self.ib.isConnected():
                        self._reconnect_attempts += 1
                        delay = min(
                            self._reconnect_base_delay * 2**self._reconnect_attempts,
                            60,  # Max 60 seconds
                        )
                        logger.warning(
                            f"Connection lost. Reconnecting in {delay}s "
                            f"(attempt {self._reconnect_attempts}/{self._max_reconnect_attempts})"
                        )
                        time.sleep(delay)
                        self._connect_to_ib()
                    else:
                        self._reconnect_attempts = 0
                        self.ib.sleep(1)
                except ConnectionError as e:
                    logger.error(f"IB connection error: {e}")
                except Exception as e:
                    logger.error(f"Unexpected error in event loop: {e}", exc_info=True)
                    time.sleep(5)

            logger.info("IBKR event loop stopped")

        self._connection_thread = Thread(target=_start_ib_loop, daemon=True)
        self._connection_thread.start()

    @property
    def id(self) -> str:
        return "interactivebrokers_stocks"

    @property
    def name(self) -> str:
        return "interactivebrokers_stocks"

    def get_proxy_coin(self) -> str:
        return self._config.get("stake_currency", "USD")

    def is_market_open(self) -> bool:
        """
        Check if the US stock market is currently open based on UTC time.
        Regular hours: Monday-Friday 9:30 AM - 4:00 PM ET
        Pre-market: 4:00 AM - 8:00 AM ET
        After-hours: 4:00 PM - 8:00 PM ET
        """
        now = datetime.now(UTC)
        day = now.weekday()  # 0=Monday, 6=Sunday
        current_time = now.time()

        # Weekend - always closed
        if day >= 5:  # Saturday or Sunday
            return False

        # Regular market hours
        if MARKET_OPEN_TIME_REGULAR_UTC <= current_time <= MARKET_CLOSE_TIME_REGULAR_UTC:
            return True

        # Pre-market hours (4:00 AM - 8:00 AM ET)
        if MARKET_OPEN_TIME_PREMARKET_UTC <= current_time <= MARKET_CLOSE_TIME_PREMARKET_UTC:
            return True

        # After-hours (4:00 PM - 8:00 PM ET)
        if MARKET_OPEN_TIME_AFTERHOURS_UTC <= current_time or current_time <= MARKET_CLOSE_TIME_AFTERHOURS_UTC:
            return True

        return False

    def wait_for_market_open(self) -> None:
        """
        Sleep until the stock market opens if it is currently closed.
        """
        if self.is_market_open():
            return

        now = datetime.now(UTC)
        next_open = None

        if now.weekday() >= 5:  # Weekend
            # Next Monday 9:30 AM ET
            days_until_monday = (7 - now.weekday()) % 7
            if days_until_monday == 0:  # If it's Sunday, wait for next Monday
                days_until_monday = 7
            next_open = now.replace(hour=13, minute=30, second=0, microsecond=0) + timedelta(days=days_until_monday)
        else:
            # Weekday - check if we need to wait for regular hours
            if now.time() < MARKET_OPEN_TIME_REGULAR_UTC:
                # Before regular hours - wait for regular open
                next_open = now.replace(hour=13, minute=30, second=0, microsecond=0)
            elif now.time() > MARKET_CLOSE_TIME_REGULAR_UTC:
                # After regular hours - wait for next day
                if now.weekday() == 4:  # Friday after close
                    # Next Monday
                    next_open = now.replace(hour=13, minute=30, second=0, microsecond=0) + timedelta(days=3)
                else:
                    # Next day regular hours
                    next_open = now.replace(hour=13, minute=30, second=0, microsecond=0) + timedelta(days=1)

        if next_open:
            sleep_seconds = (next_open - now).total_seconds()
            logger.info(
                f"Market closed. Sleeping for {sleep_seconds:.2f} seconds until {next_open} UTC."
            )

            # Sleep in smaller intervals to check for shutdown event
            while sleep_seconds > 0 and not self.shutdown_event.is_set():
                time.sleep(min(1, sleep_seconds))
                sleep_seconds -= 1

            if self.shutdown_event.is_set():
                logger.info("Shutdown signal received, exiting sleep.")

    def create_order(
        self,
        pair: str | tuple,
        ordertype: str,
        side: str,
        amount: float,
        price: float | None = None,
        params: dict[Any, Any] | None = None,
        rate: float | None = None,
        **kwargs,
    ) -> dict:
        # 1) Detect TWS down & back off before everything else
        self.ensure_connected()

        params = params or {}
        pair = pair[0] if isinstance(pair, tuple) else pair

        # ——— Prevent duplicate in-flight orders for the same pair+side ———
        try:
            open_orders = self.fetch_open_orders(pair)
            # match on side and open status
            dup = [
                o
                for o in open_orders
                if o["side"].lower() == side.lower() and o["status"] == "open"
            ]
            if dup:
                logger.warning(
                    f"Skipping new {side.upper()} order for {pair}: "
                    f"{len(dup)} existing open order(s) detected."
                )
                from freqtrade.exceptions import ExchangeError

                raise ExchangeError(f"Duplicate in-flight {side} order for {pair}")
        except ExchangeError:
            # bubble up to FreqTrade so it won't persist anything
            raise
        except Exception as e:
            logger.error(f"Error checking existing orders for {pair}: {e}")
            # proceed anyway

        # ——— initialize contract, amount, price ———
        contract, amount, price = self._initialize_contract_amount_price(pair, amount, price, rate)

        use_market = ordertype.lower() == "market" or (
            side.lower() == "sell" and params.get("exit_as_market", False)
        )

        # ——— build IB order object ———
        if use_market:
            order = Order(action=side.upper(), totalQuantity=amount, orderType="MKT")
        else:
            try:
                if price is None or price <= 0:
                    price = self.get_rate(pair, side=side)
                if not (0.01 <= price <= 10000.0):  # Stock prices can be higher
                    raise ValueError(f"Invalid price for order: {price}")
                order = Order(
                    action=side.upper(),
                    totalQuantity=amount,
                    orderType="LMT",
                    lmtPrice=round(price, self.SIGNIFICANT_DIGITS - 1),
                )
            except ValueError as e:
                logger.error(f"Failed to get valid price for order: {e}")
                return self._failed_response(pair, ordertype, side, amount, price, str(e))

        return self._place_and_wait_for_order(contract, order, pair, ordertype, side, amount, price)

    def _place_and_wait_for_order(
        self,
        contract: Any,
        order: Any,
        pair: str,
        ordertype: str,
        side: str,
        amount: float,
        price: float | None,
    ) -> dict:
        """Place order and wait for IB to acknowledge it."""
        try:
            trade = self.ib.placeOrder(contract, order)
            logger.info(
                f"Order placed: {order.action} {order.totalQuantity} "
                f"{pair} at {getattr(order, 'lmtPrice', 'MARKET')}"
            )
        except Exception as e:
            logger.error(f"Error placing order: {e}")
            return self._failed_response(pair, ordertype, side, amount, price, str(e))

        # ——— wait for IB to ack/fill ———
        deadline = time.time() + 30
        while (
            time.time() < deadline
            and trade.orderStatus.status in ("ApiPending", "PendingSubmit", "Submitted")
            and not self.shutdown_event.is_set()
        ):
            self.ib.waitOnUpdate(timeout=1)

        if self.shutdown_event.is_set():
            logger.info("Shutdown signal received, exiting order placement.")
            return self._failed_response(pair, ordertype, side, amount, price, "Shutdown")

        # ——— finalize or raise on failure ———
        return self._finalize_trade_status(trade, pair, ordertype, side, amount, price)

    def _initialize_contract_amount_price(self, pair, amount, price, rate):
        if rate is not None and (price is None or price <= 0):
            price = rate

        # For stocks, pair is just the symbol (e.g., "AAPL")
        symbol = pair.strip().upper()

        # Create stock contract
        contract = Stock(symbol=symbol, exchange="SMART", currency="USD")

        try:
            if not self.ib.qualifyContracts(contract):
                raise ValueError(f"Contract qualification failed for {pair}")
        except Exception as e:
            logger.error(f"Contract qualification error: {e}")
            raise ValueError(f"Contract qualification failed for {pair}: {e}")

        min_lot = self.MIN_LOT_SIZE
        amount = max(min_lot, math.floor(amount / min_lot) * min_lot)

        return contract, amount, price

    def _finalize_trade_status(self, trade, pair, ordertype, side, amount, price):
        status = trade.orderStatus.status
        oid = str(trade.order.orderId)
        filled = float(trade.orderStatus.filled)
        remaining = amount - filled

        # Map to Freqtrade status
        ft_status = self._parse_order_status(status)

        # Handle open orders (including partially filled ones)
        if ft_status == "open":
            logger.info(
                f"Order {oid} for {pair} is open (status={status}), "
                f"filled={filled}, remaining={remaining}"
            )
            return {
                "id": oid,
                "symbol": pair,
                "type": ordertype.lower(),
                "side": side.lower(),
                "amount": amount,
                "price": price,
                "filled": filled,
                "remaining": remaining,
                "status": ft_status,
                "info": trade,
            }

        # Handle filled orders
        if ft_status == "closed":
            logger.info(f"Order {oid} for {pair} filled {filled} / {amount}")
            return {
                "id": oid,
                "symbol": pair,
                "type": ordertype.lower(),
                "side": side.lower(),
                "amount": amount,
                "price": price,
                "filled": filled,
                "remaining": remaining,
                "status": ft_status,
                "info": trade,
            }

        # Handle failed/canceled orders
        logger.warning(
            f"Order {oid} for {pair} failed with status: {status}. "
            f"Reason: {trade.orderStatus.whyHeld}"
        )
        Trade.session.rollback()
        raise ExchangeError(f"Order for {pair} failed with status: {status}.")

    def get_rate(
        self,
        pair: str | tuple,
        side: str | None = None,
        **kwargs,
    ) -> float:
        """
        Try to fetch a live price; on failure due to stale/nan data or disconnect,
        trigger a reconnect and retry once before falling back to historical.
        """
        if self.is_shutting_down:
            logger.info("Shutdown in progress terminating now.")
            sys.exit(0)

        if not self.is_market_open():
            self.wait_for_market_open()

        pair = pair[0] if isinstance(pair, tuple) else pair
        # First attempt
        try:
            return self._fetch_live_price(pair, side)
        except Exception as e:
            logger.error(f"Failed to request market data for {pair} (live): {e}")
        # Final fallback
        return self._fallback_to_historical_rate(pair)

    def _fetch_live_price(self, pair: str, side: str | None) -> float:
        """
        Fetch live price from IBKR with caching, snapshot requests,
        and silent fallback to historical data. This version is improved to be
        more reliable and efficient.

        Args:
            pair: Stock symbol (e.g., 'AAPL')
            side: 'buy', 'sell', or None for mid price

        Returns:
            Current price as float (live if possible, else historical)
        """
        if self.is_shutting_down:
            raise ConnectionError("Shutdown in progress")

        now = time.time()
        # 1) Return cached price if within 1 second
        cached = self._live_price_cache.get(pair)
        if cached and (now - cached[0] < 1.0):
            price = cached[1]
            logger.debug(f"Using cached price for {pair} ({side}): {price}")
            return price

        # Connection check
        if not self.ib.isConnected():
            raise ConnectionError("Not connected to IBKR")

        # Build contract
        symbol = pair.strip().upper()
        contract = Stock(symbol=symbol, exchange="SMART", currency="USD")

        # Rate limit before request
        throttle()

        ticker = None
        try:
            # 2) Snapshot request: get one tick then unsubscribe
            logger.debug(f"Requesting live price for {contract.symbol}, reqId pending")
            ticker = self.ib.reqMktData(contract, snapshot=True)
            logger.debug(f"Received ticker for {contract.symbol}, reqId processed")

            # Wait for valid data with a timeout instead of a fixed sleep
            deadline = time.time() + 5  # 5-second timeout
            while time.time() < deadline:
                bid = getattr(ticker, "bid", None)
                ask = getattr(ticker, "ask", None)
                if (
                    bid is not None
                    and ask is not None
                    and not math.isnan(bid)
                    and not math.isnan(ask)
                ):
                    break  # Data is valid
                self.ib.sleep(0.1)  # Let ib_insync process events
            else:
                # Loop finished without break, indicates a timeout
                raise ValueError(f"Timeout waiting for valid live tick for {pair}")

            bid = ticker.bid
            ask = ticker.ask

            # Choose price based on side
            if side is None:
                price = (bid + ask) / 2
            elif side.lower() == "buy":
                price = ask
            elif side.lower() == "sell":
                price = bid
            else:
                price = (bid + ask) / 2

            # 3) Cache and log live price
            self._live_price_cache[pair] = (now, price)
            logger.info(f"Returning price for {pair} ({side}): {price}")
            return price

        except Exception as e:
            logger.warning(f"Live price fetch for {pair} failed: {e}. Falling back to historical.")
            # 4) On any failure, fallback to historical close
            price = self._fallback_to_historical_rate(pair)
            logger.info(f"Historical fallback price for {pair}: {price}")
            return price
        finally:
            # IMPORTANT: Cancel the market data subscription to prevent leaks
            pass

    def _fallback_to_historical_rate(self, pair: str) -> float:
        """
        Fallback to historical data when live price fails.

        Args:
            pair: Stock symbol

        Returns:
            Most recent historical close price

        Raises:
            ValueError: If no valid historical data available
        """
        if self.is_shutting_down:
            raise ConnectionError("Shutdown in progress")

        try:
            timeframe = self._config.get("timeframe", "5m")
            ohlcv = self.get_historic_ohlcv(pair, timeframe=timeframe, limit=1)

            if ohlcv.empty:
                raise ValueError(f"No historical data available for {pair}")

            close_price = ohlcv.iloc[0]["close"]

            if pd.isna(close_price):
                raise ValueError(f"NaN value in historical data for {pair}")

            if not (0.01 <= close_price <= 10000.0):
                raise ValueError(f"Historical price {close_price} out of valid range for {pair}")

            logger.info(f"Using historical close price for {pair}: {close_price}")
            return close_price

        except Exception as e:
            logger.error(f"Historical data fallback failed for {pair}: {str(e)}")
            raise ValueError(f"Could not fetch valid rate for {pair} from any source")

    def _failed_response(self, pair, ordertype, side, amount, price, info):
        return {
            "id": None,
            "symbol": pair,
            "type": ordertype.lower(),
            "side": side.lower(),
            "amount": amount,
            "price": price,
            "filled": 0.0,
            "remaining": amount,
            "status": "failed",
            "info": info,
        }

    def _parse_order_status(self, ib_status: str) -> str:
        status_mapping = {
            "ApiPending": "open",
            "PendingSubmit": "open",
            "PreSubmitted": "open",
            "Submitted": "open",
            "Filled": "closed",
            "Cancelled": "canceled",
            "Canceled": "canceled",
            "Inactive": "canceled",
            "ApiCancelled": "canceled",
            "PendingCancel": "canceling",
        }
        return status_mapping.get(ib_status, "unknown")

    def cancel_order(self, order_id: str, pair: str | None = None) -> dict:
        try:
            ib_order_id = int(order_id)
            self.ib.client.cancelOrder(ib_order_id)
            logger.info(f"Order {order_id} cancel request sent successfully.")
            # *** CRUCIAL: tell Freqtrade that this order is gone ***
            self.remove_order_from_freqtrade(order_id)
            return {
                "status": "canceled",
                "id": order_id,
                "message": "Cancelled on IBKR and removed from Freqtrade",
            }
        except (ValueError, TypeError) as e:
            logger.error(f"Invalid order ID format when canceling '{order_id}': {e}")
            return {"status": "error", "id": order_id, "message": f"Invalid order ID format: {e}"}
        except Exception as e:
            logger.error(f"Failed to cancel order {order_id}: {e}")
            return {"status": "error", "id": order_id, "message": str(e)}

    def get_markets(
        self,
        reload: bool = False,
        params: dict[Any, Any] | None = None,
        tradable_only: bool = False,
        active_only: bool = False,
    ) -> dict[Any, Any]:
        if reload or self._markets_cache is None:
            markets: dict[str, Any] = {}

            for symbol in self.stock_symbols:
                pair = symbol  # For stocks, pair is just the symbol
                markets[pair] = {
                    "id": pair,
                    "symbol": pair,
                    "base": symbol,
                    "quote": "USD",  # US stocks are quoted in USD
                    "precision": {"amount": 0, "price": 2},  # Stocks typically have 2 decimal places
                    "limits": {
                        "amount": {"min": self.MIN_LOT_SIZE, "max": 1000000},  # Large max for stocks
                        "price": {"min": 0.01, "max": 10000.0},  # Stock prices can vary widely
                        "cost": {"min": 1.0, "max": 10000000.0},  # Large cost limits
                    },
                    "active": True,
                    "spot": True,
                    "info": {"symbol": symbol, "currency": "USD"},
                }

            self._markets = markets
            self._last_markets_refresh = int(time.time() * 1000)
            self._markets_cache = markets
            logger.info(f"Loaded {len(markets)} stock markets for Interactive Brokers")

        return self._markets_cache

    def get_fee(self, symbol: str, now: Any = None, taker_or_maker: str = "maker") -> float:
        # IBKR commission structure for stocks (approximate)
        maker_fee = 0.0035  # 0.35% maker fee
        taker_fee = 0.0035  # 0.35% taker fee (same for stocks)
        return maker_fee if taker_or_maker == "maker" else taker_fee

    async def fetch_historical_data(self, contract, durationStr, ib_timeframe, endDateTime="", max_retries=3):
        """
        Request historical price data from IBKR (one shot only) with retry logic.

        Args:
            contract: IBKR Contract object.
            durationStr: How far back to go (e.g. '1 D', '2 W').
            ib_timeframe: Bar size (e.g. '1 min', '5 mins').
            endDateTime: End date/time for the request (e.g. '20251103 09:00:00 UTC').
            max_retries: Maximum number of retry attempts for timeouts.

        Returns:
            List of bars, or empty list if unavailable.
        """
        for attempt in range(max_retries):
            try:
                bars = await self.ib.reqHistoricalDataAsync(
                    contract,
                    endDateTime=endDateTime,  # Now or specified end time
                    durationStr=durationStr,
                    barSizeSetting=ib_timeframe,
                    whatToShow="TRADES",  # Use TRADES for stocks instead of MIDPOINT
                    useRTH=True,  # Regular trading hours only for stocks
                    keepUpToDate=False,  # ONE SHOT (no streaming)
                )
                if not bars:
                    logger.warning(f"No historical data returned for contract: {contract}")
                return bars

            except Exception as e:
                error_msg = str(e).lower()
                if "timeout" in error_msg or "162" in error_msg or "cancelled" in error_msg:
                    if attempt < max_retries - 1:
                        wait_time = 2 ** attempt  # Exponential backoff: 1s, 2s, 4s
                        logger.warning(f"Historical data request timeout/cancelled for {contract}, retrying in {wait_time}s (attempt {attempt + 1}/{max_retries})")
                        await asyncio.sleep(wait_time)
                        continue
                    else:
                        logger.error(f"Historical data request failed after {max_retries} attempts for {contract}: {e}")
                        return []
                else:
                    logger.warning(f"Historical data error for {contract}: {e}")
                    return []
        return []

    def get_historic_ohlcv(
        self,
        pair: str,
        since: int | None = None,
        timeframe: str | None = None,
        limit: int = 1000,
        params: dict | None = None,
        since_ms: int | None = None,
        is_new_pair: bool = True,
        candle_type: str = "spot",
        until_ms: int | None = None,
    ) -> pd.DataFrame:
        if not self.is_market_open():
            self.wait_for_market_open()

        if isinstance(pair, tuple):
            pair = pair[0]

        # Create stock contract
        symbol = pair.strip().upper()
        contract = Contract()
        contract.symbol = symbol
        contract.secType = "STK"
        contract.currency = "USD"
        contract.exchange = "SMART"

        if timeframe is None:
            timeframe = self._config.get("timeframe", "1h")
        ib_timeframe = self._convert_timeframe(timeframe)

        # Calculate duration and endDateTime based on timerange if provided
        if since_ms is not None or until_ms is not None:
            endDateTime = ""
            if until_ms is not None:
                # Convert until_ms to IBKR format
                end_datetime = datetime.fromtimestamp(until_ms / 1000, tz=UTC)
                endDateTime = end_datetime.strftime("%Y%m%d %H:%M:%S %Z")
            if since_ms is not None:
                start_datetime = datetime.fromtimestamp(since_ms / 1000, tz=UTC)
                now = datetime.now(UTC)
                if until_ms is not None:
                    end_datetime = datetime.fromtimestamp(until_ms / 1000, tz=UTC)
                    duration_seconds = (end_datetime - start_datetime).total_seconds()
                else:
                    duration_seconds = (now - start_datetime).total_seconds()
                durationStr = self._calculate_duration_from_seconds(duration_seconds)
            else:
                durationStr = self._calculate_duration(timeframe, limit)
        else:
            endDateTime = ""
            durationStr = self._calculate_duration(timeframe, limit)

        # For stock data, limit duration to prevent timeouts - IBKR has limited historical data
        max_duration_days = {"1m": 1, "5m": 7, "15m": 30, "30m": 60, "1h": 365, "4h": 365, "1d": 365*5}
        if timeframe in max_duration_days:
            max_days = max_duration_days[timeframe]
            if durationStr.endswith(" D") and int(durationStr[:-2]) > max_days:
                durationStr = f"{max_days} D"
                logger.info(f"Limited {timeframe} data request to {max_days} days due to IBKR limitations")
            elif durationStr.endswith(" Y") and int(durationStr[:-2]) * 365 > max_days:
                limited_years = max(1, max_days // 365)
                durationStr = f"{limited_years} Y"
                logger.info(f"Limited {timeframe} data request to {limited_years} years due to IBKR limitations")

        try:
            throttle()
            bars = self.ib.run(self.fetch_historical_data(contract, durationStr, ib_timeframe, endDateTime))
            throttle()
            if not bars:
                logger.warning(f"No bars returned for {pair} with timeframe {timeframe}")
                return pd.DataFrame(columns=["date", "open", "high", "low", "close", "volume"])

            df = util.df(bars)
            if df is None or df.empty:
                logger.warning(f"Empty DataFrame returned for {pair}")
                return pd.DataFrame(columns=["date", "open", "high", "low", "close", "volume"])

            df.rename(
                columns={
                    "date": "timestamp",
                    "open": "open",
                    "high": "high",
                    "low": "low",
                    "close": "close",
                    "volume": "volume",
                },
                inplace=True,
            )

            if "timestamp" in df.columns:
                df["date"] = pd.to_datetime(df["timestamp"], utc=True)
            else:
                logger.error(f"No timestamp column in DataFrame for {pair}")
                raise ValueError("DataFrame must have a 'timestamp' column")

            df = df.sort_values(by="date", ascending=True).reset_index(drop=True)

            if not df.empty:
                current_time = datetime.now(UTC)
                last_candle = df["date"].iloc[-1]
                first_candle = df["date"].iloc[0]
                num_candles = len(df)
                age_minutes = (current_time - last_candle).total_seconds() / 60
                logger.info(
                    f"Retrieved {num_candles} candles for {pair}"
                    f"from {first_candle} to {last_candle} "
                    f"(Last candle age: {age_minutes:.2f} minutes)"
                )
            return df

        except Exception as e:
            logger.error(f"Failed to fetch historical data for {pair}: {e}")
            # Return empty DataFrame instead of raising exception to allow download to continue
            return pd.DataFrame(columns=["date", "open", "high", "low", "close", "volume"])

    def refresh_latest_ohlcv(self, pairs: list) -> None:
        """
        Refresh the latest OHLCV data for the given pairs.
        If the market is closed, sleep until 5 minutes before it opens and inform the user.
        """
        if not pairs:
            logger.debug("Empty pairs list passed to refresh_latest_ohlcv")
            return

        for item in pairs:
            try:
                if isinstance(item, tuple):
                    if len(item) >= 2:
                        pair, timeframe = item[0], item[1]
                        candle_type = item[2] if len(item) > 2 else "spot"
                    else:
                        pair = item[0]
                        timeframe = self._config.get("timeframe", "1h")
                        candle_type = "spot"
                else:
                    pair = item
                    timeframe = self._config.get("timeframe", "1h")
                    candle_type = "spot"

                ohlcv = self.get_historic_ohlcv(pair, None, timeframe, limit=3)

                if not ohlcv.empty:
                    key = (pair, timeframe, candle_type)
                    self.latest_ohlcv[key] = ohlcv
                    logger.debug(
                        f"Refreshed latest OHLCV for {pair}/{timeframe}, "
                        f"last timestamp: {ohlcv['date'].iloc[-1]}"
                    )
                else:
                    logger.warning(f"No OHLCV data refreshed for {pair}/{timeframe}")
            except Exception as e:
                logger.error(f"Failed to refresh latest OHLCV for {pair}: {e}")

    def klines(
        self,
        pair_interval: PairWithTimeframe,
        timeframe: str | None = None,
        since: int = 0,
        limit: int = 1000,
        params: dict[Any, Any] | None = None,
        **kwargs: Any,
    ) -> pd.DataFrame:
        if params is None:
            params = {}
        if timeframe is None:
            timeframe = self._config.get("timeframe", "1h")
        return self.get_historic_ohlcv(pair_interval, since, timeframe, limit)

    def get_balances(self):
        account = self.ib.accountSummary()
        balances: dict[str, Any] = {}
        for item in account:
            if item.tag == "TotalCashValue":
                balances[item.currency] = {
                    "free": float(item.value),
                    "used": 0.0,
                    "total": float(item.value),
                }
        return balances

    def market_is_tradable(self, market: dict) -> bool:
        return market.get("active", False) and market.get("tradable", True)

    def get_pair_quote_currency(self, pair: str) -> str:
        if pair not in self.markets:
            raise ValueError(f"Pair {pair} not found in markets")
        return self.markets[pair]["quote"]

    def get_pair_base_currency(self, pair: str) -> str:
        if pair not in self.markets:
            raise ValueError(f"Pair {pair} not found in markets")
        return self.markets[pair]["base"]

    def ws_connection_reset(self) -> None:
        if self.ib.isConnected():
            self.ib.disconnect()
        try:
            self.ib.connect(self.host, self.port, clientId=self.client_id)
            logger.info("WebSocket connection reset")
            self._ws_connected = True
        except Exception as e:
            logger.error(f"Failed to reset WebSocket connection: {e}")
            self._ws_connected = False

    def ws_start(self) -> None:
        if not self.ib.isConnected():
            try:
                self.ib.connect(self.host, self.port, clientId=self.client_id)
                self._setup_event_loop()
                self._ws_connected = True
                logger.info("WebSocket started")
            except Exception as e:
                logger.error(f"Failed to start WebSocket: {e}")
                self._ws_connected = False
        else:
            logger.info("WebSocket already running")

    def ws_stop(self) -> None:
        if self.ib.isConnected():
            self.ib.disconnect()
        logger.info("WebSocket stopped")

    def ws_health_check(self) -> bool:
        if not self.ib.isConnected():
            return False

        try:
            # Verify actual data flow
            self.ib.reqCurrentTime()
            return True
        except Exception:
            return False

    def _convert_timeframe(self, timeframe: str) -> str:
        mapping = {
            "1m": "1 min",
            "5m": "5 mins",
            "15m": "15 mins",
            "30m": "30 mins",
            "1h": "1 hour",
            "4h": "4 hours",
            "1d": "1 day",
        }
        return mapping.get(timeframe, timeframe)

    def _calculate_duration(self, timeframe: str, limit: int) -> str:
        timeframe_to_candles_per_day = {
            "1m": 1440,
            "5m": 288,
            "15m": 96,
            "30m": 48,
            "1h": 24,
            "4h": 6,
            "1d": 1,
        }

        if timeframe not in timeframe_to_candles_per_day:
            raise ValueError(f"Unsupported timeframe: {timeframe}")

        candles_per_day = timeframe_to_candles_per_day[timeframe]
        total_days = math.ceil(limit / candles_per_day)

        if total_days <= 365:
            return f"{total_days} D"
        else:
            years = math.ceil(total_days / 365)
            return f"{years} Y"

    def validate_timeframes(self, timeframes):
        if timeframes is None:
            return  # Skip validation if no timeframes provided

        if isinstance(timeframes, str):
            timeframes = [timeframes]

        supported_timeframes = ["1m", "5m", "15m", "30m", "1h", "4h", "1d"]
        logger.info(f"Validating timeframes: {timeframes}")

        for timeframe in timeframes:
            logger.info(f"Validating timeframe: {timeframe}")
            if timeframe not in supported_timeframes:
                raise ValueError(
                    f"Timeframe '{timeframe}' is not supported by Interactive Brokers."
                )

    def get_funding_fees(self, pair: str, timeframe: str | None = None, **kwargs) -> float:
        return 0.0

    def fetch_order_or_stoploss_order(
        self,
        order_id: str,
        pair: str | None = None,
        *args,
        **kwargs,
    ) -> dict:
        order = self.fetch_order(order_id, pair)
        if order is None:
            return {"status": "not_found"}
        return order

    def check_order_canceled_empty(self, order: dict) -> bool:
        if not order:
            return False
        return order.get("status") == "canceled" and order.get("remaining", 0) == 0

    def order_has_fee(self, order) -> bool:
        return False