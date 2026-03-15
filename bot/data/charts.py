# data/charts.py
"""Generate branded chart images for tweets."""
from __future__ import annotations

import logging
import os
import tempfile
from datetime import datetime, timezone

from PIL import Image, ImageDraw, ImageFont

from bot.config import BOT_HANDLE

# matplotlib is optional — only needed for _get_font fallback and candlestick charts.
# If not installed, font falls back to system paths / PIL default, and candlestick returns None.
try:
    import matplotlib
    matplotlib.use("Agg")  # headless backend — must be set before any other matplotlib import
    _HAS_MATPLOTLIB = True
except ImportError:
    _HAS_MATPLOTLIB = False

logger = logging.getLogger(__name__)

# ── Brand constants ──────────────────────────────────────────────────────────
BG_COLOR = "#0D1117"
CARD_BG = "#161B22"
ACCENT_GREEN = "#00D4AA"
ACCENT_RED = "#FF4757"
TEXT_PRIMARY = "#E6EDF3"
TEXT_SECONDARY = "#8B949E"
WATERMARK_COLOR = "#58A6FF"
BORDER_COLOR = "#30363D"

IMG_WIDTH = 1200
IMG_HEIGHT = 675

COLUMN_NAMES = {
    "morning_brief": "The Open",
    "hot_take": "Signal Flare",
    "onchain_signal": "Chain Pulse",
    "us_open": "Wall St. Cross",
    "evening_wrap": "The Close",
    "alpha_thread": "Deep Dive",
}

COIN_SYMBOLS = {
    "bitcoin": "BTC",
    "ethereum": "ETH",
    "solana": "SOL",
    "binancecoin": "BNB",
    "ripple": "XRP",
}

# Coin display order
COIN_ORDER = ["bitcoin", "ethereum", "solana", "binancecoin", "ripple"]


def _get_font(size: int, bold: bool = False) -> ImageFont.FreeTypeFont:
    """Load font — try matplotlib's bundled DejaVu Sans, then system paths, then PIL default."""
    if _HAS_MATPLOTLIB:
        try:
            import matplotlib.font_manager as fm
            family = "DejaVu Sans"
            weight = "bold" if bold else "normal"
            path = fm.findfont(fm.FontProperties(family=family, weight=weight))
            return ImageFont.truetype(path, size)
        except Exception:
            pass
    # Fallback to common system font paths
    for font_path in [
        "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf" if bold else "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
        "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
    ]:
        try:
            return ImageFont.truetype(font_path, size)
        except Exception:
            pass
    return ImageFont.load_default()


def _draw_rounded_rect(draw: ImageDraw.Draw, xy: tuple, radius: int, fill: str):
    """Draw a rounded rectangle."""
    x0, y0, x1, y1 = xy
    draw.rounded_rectangle(xy, radius=radius, fill=fill)


def _format_price(price: float) -> str:
    if price >= 100:
        return f"${price:,.0f}"
    elif price >= 1:
        return f"${price:,.2f}"
    else:
        return f"${price:.4f}"


def _format_change(change: float) -> str:
    sign = "+" if change >= 0 else ""
    return f"{sign}{change:.1f}%"


def generate_market_card(
    prices: dict, fear_greed: dict, tweet_type: str
) -> str | None:
    """Generate a market dashboard card. Returns path to temp PNG or None."""
    try:
        img = Image.new("RGB", (IMG_WIDTH, IMG_HEIGHT), BG_COLOR)
        draw = ImageDraw.Draw(img)

        column_name = COLUMN_NAMES.get(tweet_type, tweet_type)
        date_str = datetime.now(timezone.utc).strftime("%b %d, %Y • %H:%M UTC")

        # Fonts
        font_title = _get_font(36, bold=True)
        font_date = _get_font(18)
        font_symbol = _get_font(28, bold=True)
        font_price = _get_font(32, bold=True)
        font_change = _get_font(20)
        font_label = _get_font(16)
        font_fg_value = _get_font(24, bold=True)
        font_watermark = _get_font(36, bold=True)

        # ── Header ───────────────────────────────────────────────────────
        draw.text((40, 30), column_name, fill=ACCENT_GREEN, font=font_title)
        draw.text((40, 75), date_str, fill=TEXT_SECONDARY, font=font_date)
        # Accent line
        draw.rectangle([(40, 110), (IMG_WIDTH - 40, 112)], fill=ACCENT_GREEN)

        # ── Price grid ───────────────────────────────────────────────────
        y_start = 140
        card_w = 200
        card_h = 120
        gap = 16
        margin_x = 40
        # 5 coins in a row
        for i, coin_id in enumerate(COIN_ORDER):
            data = prices.get(coin_id, {})
            symbol = COIN_SYMBOLS.get(coin_id, coin_id.upper())
            usd = data.get("usd")
            change = data.get("usd_24h_change")

            x = margin_x + i * (card_w + gap)
            y = y_start

            # Card background
            _draw_rounded_rect(draw, (x, y, x + card_w, y + card_h), 12, CARD_BG)

            # Symbol
            draw.text((x + 15, y + 12), symbol, fill=TEXT_SECONDARY, font=font_symbol)

            # Price
            if usd is not None:
                price_str = _format_price(float(usd))
                draw.text((x + 15, y + 48), price_str, fill=TEXT_PRIMARY, font=font_price)
            else:
                draw.text((x + 15, y + 48), "N/A", fill=TEXT_SECONDARY, font=font_price)

            # Change %
            if change is not None:
                change_val = float(change)
                change_str = _format_change(change_val)
                change_color = ACCENT_GREEN if change_val >= 0 else ACCENT_RED
                draw.text((x + 15, y + 90), change_str, fill=change_color, font=font_change)

        # ── Fear & Greed bar ─────────────────────────────────────────────
        fg_value = fear_greed.get("value") if fear_greed else None
        fg_label = fear_greed.get("label", "") if fear_greed else ""

        y_fg = y_start + card_h + 50
        bar_x = 40
        bar_w = IMG_WIDTH - 80
        bar_h = 30

        draw.text((bar_x, y_fg - 5), "Fear & Greed Index", fill=TEXT_SECONDARY, font=font_label)

        bar_y = y_fg + 25
        # Draw gradient bar (simplified: red → yellow → green)
        for px in range(bar_w):
            ratio = px / bar_w
            if ratio < 0.25:
                r, g, b = 255, int(69 + ratio * 4 * 186), 87
            elif ratio < 0.5:
                r2 = (ratio - 0.25) / 0.25
                r, g, b = 255, 255, int(87 - 87 * r2)
            elif ratio < 0.75:
                r3 = (ratio - 0.5) / 0.25
                r, g, b = int(255 - 255 * r3 + 0 * r3), int(255 - 43 * r3), int(0 + 170 * r3)
            else:
                r, g, b = 0, 212, 170
            draw.line([(bar_x + px, bar_y), (bar_x + px, bar_y + bar_h)],
                      fill=(r, g, b))

        if fg_value is not None:
            fg_value = max(0, min(100, int(fg_value)))
            # Marker
            marker_x = bar_x + int(fg_value / 100 * bar_w)
            draw.polygon([(marker_x - 8, bar_y - 10), (marker_x + 8, bar_y - 10),
                          (marker_x, bar_y - 2)], fill=TEXT_PRIMARY)
            draw.ellipse([(marker_x - 4, bar_y + bar_h + 4),
                          (marker_x + 4, bar_y + bar_h + 12)], fill=TEXT_PRIMARY)

            # Value text
            fg_text = f"{fg_value}/100"
            draw.text((bar_x, bar_y + bar_h + 18), fg_text, fill=TEXT_PRIMARY, font=font_fg_value)
            if fg_label:
                draw.text((bar_x + 100, bar_y + bar_h + 22), fg_label,
                          fill=TEXT_SECONDARY, font=font_change)

        # ── Second row: large BTC + ETH highlight ────────────────────────
        y_highlight = bar_y + bar_h + 70
        btc_data = prices.get("bitcoin", {})
        eth_data = prices.get("ethereum", {})

        font_big_price = _get_font(48, bold=True)
        font_big_label = _get_font(20)

        # BTC highlight
        if btc_data.get("usd") is not None:
            _draw_rounded_rect(draw, (40, y_highlight, 580, y_highlight + 100), 16, CARD_BG)
            draw.text((60, y_highlight + 10), "Bitcoin", fill=TEXT_SECONDARY, font=font_big_label)
            btc_price_str = _format_price(float(btc_data["usd"]))
            draw.text((60, y_highlight + 38), btc_price_str, fill=TEXT_PRIMARY, font=font_big_price)
            if btc_data.get("usd_24h_change") is not None:
                ch = float(btc_data["usd_24h_change"])
                ch_str = _format_change(ch)
                # Right-align change text within the card
                ch_bbox = draw.textbbox((0, 0), ch_str, font=font_big_price)
                ch_w = ch_bbox[2] - ch_bbox[0]
                draw.text((560 - ch_w, y_highlight + 50), ch_str,
                          fill=ACCENT_GREEN if ch >= 0 else ACCENT_RED, font=font_big_price)

        # ETH highlight
        if eth_data.get("usd") is not None:
            _draw_rounded_rect(draw, (620, y_highlight, 1160, y_highlight + 100), 16, CARD_BG)
            draw.text((640, y_highlight + 10), "Ethereum", fill=TEXT_SECONDARY, font=font_big_label)
            eth_price_str = _format_price(float(eth_data["usd"]))
            draw.text((640, y_highlight + 38), eth_price_str, fill=TEXT_PRIMARY, font=font_big_price)
            if eth_data.get("usd_24h_change") is not None:
                ch = float(eth_data["usd_24h_change"])
                ch_str = _format_change(ch)
                ch_bbox = draw.textbbox((0, 0), ch_str, font=font_big_price)
                ch_w = ch_bbox[2] - ch_bbox[0]
                draw.text((1140 - ch_w, y_highlight + 50), ch_str,
                          fill=ACCENT_GREEN if ch >= 0 else ACCENT_RED, font=font_big_price)

        # ── Watermark ────────────────────────────────────────────────────
        draw.text((IMG_WIDTH - 360, IMG_HEIGHT - 55), f"@{BOT_HANDLE}",
                  fill=WATERMARK_COLOR, font=font_watermark)

        # ── Save ─────────────────────────────────────────────────────────
        fd, path = tempfile.mkstemp(suffix=".png")
        os.close(fd)
        img.save(path, "PNG")
        logger.info("Generated market card: %s (%s)", column_name, path)
        return path

    except Exception as exc:
        logger.warning("generate_market_card failed: %s", exc)
        if 'path' in locals():
            try:
                os.unlink(path)
            except OSError:
                pass
        return None


def generate_onchain_card(
    onchain_data: dict, derivatives_data: dict | None, tweet_type: str
) -> str | None:
    """Generate an on-chain metrics card. Returns path to temp PNG or None."""
    try:
        img = Image.new("RGB", (IMG_WIDTH, IMG_HEIGHT), BG_COLOR)
        draw = ImageDraw.Draw(img)

        column_name = COLUMN_NAMES.get(tweet_type, tweet_type)
        date_str = datetime.now(timezone.utc).strftime("%b %d, %Y • %H:%M UTC")

        font_title = _get_font(36, bold=True)
        font_date = _get_font(18)
        font_metric_label = _get_font(16)
        font_metric_value = _get_font(32, bold=True)
        font_metric_unit = _get_font(16)
        font_watermark = _get_font(36, bold=True)

        # ── Header ───────────────────────────────────────────────────────
        draw.text((40, 30), column_name, fill=ACCENT_GREEN, font=font_title)
        draw.text((40, 75), date_str, fill=TEXT_SECONDARY, font=font_date)
        draw.rectangle([(40, 110), (IMG_WIDTH - 40, 112)], fill=ACCENT_GREEN)

        # ── Metric cards ─────────────────────────────────────────────────
        metrics = []
        if "transactions_24h" in onchain_data:
            metrics.append(("24h Transactions", f"{int(onchain_data['transactions_24h']):,}", ""))
        if "transaction_volume_btc" in onchain_data:
            vol = float(onchain_data["transaction_volume_btc"])
            metrics.append(("TX Volume", f"{vol:,.0f}", "BTC"))
        if "hashrate" in onchain_data:
            hr = float(onchain_data["hashrate"])
            metrics.append(("Hashrate", f"{hr / 1e18:.1f}", "EH/s"))
        if "mempool_tx" in onchain_data:
            metrics.append(("Mempool", f"{int(onchain_data['mempool_tx']):,}", "pending"))
        if "fastest_fee" in onchain_data:
            metrics.append(("Priority Fee", f"{int(onchain_data['fastest_fee'])}", "sat/vB"))
        if "hour_fee" in onchain_data:
            metrics.append(("Economy Fee", f"{int(onchain_data['hour_fee'])}", "sat/vB"))

        # Derivatives metrics
        if derivatives_data:
            if "avg_rate" in derivatives_data:
                fr = float(derivatives_data["avg_rate"]) * 100  # ratio → percentage
                sign = "+" if fr >= 0 else ""
                metrics.append(("Funding Rate", f"{sign}{fr:.4f}%", ""))
            if "total_oi_usd" in derivatives_data:
                oi = float(derivatives_data["total_oi_usd"])
                metrics.append(("Open Interest", f"${oi / 1e9:.1f}B", "USD"))

        # Layout: 2 rows, 4 columns max
        card_w = 260
        card_h = 140
        gap = 16
        margin_x = 40
        y_start = 140

        for i, (label, value, unit) in enumerate(metrics[:8]):
            col = i % 4
            row = i // 4
            x = margin_x + col * (card_w + gap)
            y = y_start + row * (card_h + gap)

            _draw_rounded_rect(draw, (x, y, x + card_w, y + card_h), 12, CARD_BG)
            draw.text((x + 20, y + 15), label, fill=TEXT_SECONDARY, font=font_metric_label)
            draw.text((x + 20, y + 50), value, fill=TEXT_PRIMARY, font=font_metric_value)
            if unit:
                draw.text((x + 20, y + 95), unit, fill=TEXT_SECONDARY, font=font_metric_unit)

        # ── BTC label ────────────────────────────────────────────────────
        font_btc = _get_font(20, bold=True)
        draw.text((40, IMG_HEIGHT - 70), "Bitcoin Network Metrics",
                  fill=TEXT_SECONDARY, font=font_btc)

        # ── Watermark ────────────────────────────────────────────────────
        draw.text((IMG_WIDTH - 360, IMG_HEIGHT - 55), f"@{BOT_HANDLE}",
                  fill=WATERMARK_COLOR, font=font_watermark)

        fd, path = tempfile.mkstemp(suffix=".png")
        os.close(fd)
        img.save(path, "PNG")
        logger.info("Generated onchain card: %s (%s)", column_name, path)
        return path

    except Exception as exc:
        logger.warning("generate_onchain_card failed: %s", exc)
        if 'path' in locals():
            try:
                os.unlink(path)
            except OSError:
                pass
        return None


def generate_candlestick_chart(
    ohlc_data: list, coin_id: str = "bitcoin", days: int = 7
) -> str | None:
    """Generate a branded candlestick chart from CoinGecko OHLC data.

    Args:
        ohlc_data: list of [timestamp_ms, open, high, low, close]
        coin_id: e.g. "bitcoin"
        days: timeframe label
    Returns:
        Path to temp PNG or None.
    """
    if not _HAS_MATPLOTLIB:
        logger.info("Skipping candlestick chart: matplotlib not installed")
        return None
    try:
        import mplfinance as mpf
        import pandas as pd

        if not ohlc_data or len(ohlc_data) < 3:
            return None

        df = pd.DataFrame(ohlc_data, columns=["timestamp", "Open", "High", "Low", "Close"])
        df["Date"] = pd.to_datetime(df["timestamp"], unit="ms")
        df.set_index("Date", inplace=True)
        df.index = pd.DatetimeIndex(df.index)

        symbol = COIN_SYMBOLS.get(coin_id, coin_id.upper())

        # Brand-colored style
        mc = mpf.make_marketcolors(
            up=ACCENT_GREEN, down=ACCENT_RED,
            edge={"up": ACCENT_GREEN, "down": ACCENT_RED},
            wick={"up": ACCENT_GREEN, "down": ACCENT_RED},
        )
        style = mpf.make_mpf_style(
            base_mpl_style="dark_background",
            marketcolors=mc,
            facecolor=BG_COLOR,
            figcolor=BG_COLOR,
            gridcolor=BORDER_COLOR,
            rc={"axes.labelcolor": TEXT_SECONDARY, "xtick.color": TEXT_SECONDARY, "ytick.color": TEXT_SECONDARY},
        )

        timeframe = f"{days}D" if days > 1 else "1D"
        title = f"  ${symbol}  {timeframe}  @{BOT_HANDLE}"

        fd, path = tempfile.mkstemp(suffix=".png")
        os.close(fd)

        # Add moving averages if enough data
        mav = ()
        if len(df) >= 20:
            mav = (7, 20)
        elif len(df) >= 7:
            mav = (7,)

        plot_kwargs = dict(
            type="candle",
            style=style,
            title=title,
            ylabel="",
            figsize=(12, 6.75),
            savefig=dict(fname=path, dpi=100, bbox_inches="tight", facecolor=BG_COLOR),
        )
        if mav:
            plot_kwargs["mav"] = mav
        mpf.plot(df, **plot_kwargs)
        logger.info("Generated candlestick chart: %s %s (%s)", symbol, timeframe, path)
        return path

    except Exception as exc:
        logger.warning("generate_candlestick_chart failed: %s", exc)
        if 'path' in locals():
            try:
                os.unlink(path)
            except OSError:
                pass
        return None


def generate_weekly_scorecard(weekly_data: list) -> str | None:
    """Generate a weekly scorecard card showing 7d performance for top coins.

    Args:
        weekly_data: list of dicts from CoinGecko /coins/markets with price_change_percentage_7d
    Returns:
        Path to temp PNG or None.
    """
    try:
        if not weekly_data:
            return None

        img = Image.new("RGB", (IMG_WIDTH, IMG_HEIGHT), BG_COLOR)
        draw = ImageDraw.Draw(img)

        date_str = datetime.now(timezone.utc).strftime("%b %d, %Y")

        font_title = _get_font(36, bold=True)
        font_date = _get_font(18)
        font_coin = _get_font(28, bold=True)
        font_price = _get_font(24)
        font_change = _get_font(28, bold=True)
        font_label = _get_font(16)
        font_watermark = _get_font(36, bold=True)

        # Header
        draw.text((40, 30), "Weekly Scorecard", fill=ACCENT_GREEN, font=font_title)
        draw.text((40, 75), date_str, fill=TEXT_SECONDARY, font=font_date)
        draw.rectangle([(40, 110), (IMG_WIDTH - 40, 112)], fill=ACCENT_GREEN)

        # Coin rows
        y = 130
        row_h = 90
        bar_max_w = 300

        # Sort by 7d change to show best/worst
        sorted_data = sorted(
            weekly_data,
            key=lambda c: c.get("price_change_percentage_7d_in_currency", 0) or 0,
            reverse=True,
        )

        max_abs_change = max(
            (abs(c.get("price_change_percentage_7d_in_currency", 0) or 0) for c in sorted_data),
            default=1,
        ) or 1

        for i, coin in enumerate(sorted_data[:5]):
            symbol = coin.get("symbol", "").upper()
            price = coin.get("current_price", 0) or 0
            change = coin.get("price_change_percentage_7d_in_currency", 0) or 0

            row_y = y + i * row_h

            # Card background
            _draw_rounded_rect(draw, (40, row_y, IMG_WIDTH - 40, row_y + row_h - 8), 10, CARD_BG)

            # Symbol
            draw.text((60, row_y + 12), symbol, fill=TEXT_PRIMARY, font=font_coin)

            # Price
            price_str = f"${price:,.0f}" if price >= 100 else f"${price:,.2f}"
            draw.text((60, row_y + 48), price_str, fill=TEXT_SECONDARY, font=font_price)

            # Change bar
            bar_x = 400
            bar_y_center = row_y + row_h // 2
            bar_w = int(abs(change) / max_abs_change * bar_max_w)
            bar_color = ACCENT_GREEN if change >= 0 else ACCENT_RED

            if change >= 0:
                draw.rectangle([(bar_x, bar_y_center - 12), (bar_x + bar_w, bar_y_center + 12)], fill=bar_color)
            else:
                draw.rectangle([(bar_x - bar_w, bar_y_center - 12), (bar_x, bar_y_center + 12)], fill=bar_color)

            # Change text
            sign = "+" if change >= 0 else ""
            change_str = f"{sign}{change:.1f}%"
            ch_bbox = draw.textbbox((0, 0), change_str, font=font_change)
            ch_w = ch_bbox[2] - ch_bbox[0]
            draw.text((IMG_WIDTH - 60 - ch_w, row_y + 25), change_str,
                      fill=bar_color, font=font_change)

        # Best / Worst labels
        if sorted_data:
            draw.text((40, IMG_HEIGHT - 60), f"Best: {sorted_data[0].get('symbol', '').upper()}",
                      fill=ACCENT_GREEN, font=font_label)
            if len(sorted_data) > 1:
                worst = sorted_data[-1]
                draw.text((200, IMG_HEIGHT - 60), f"Worst: {worst.get('symbol', '').upper()}",
                          fill=ACCENT_RED, font=font_label)

        # Watermark
        draw.text((IMG_WIDTH - 360, IMG_HEIGHT - 55), f"@{BOT_HANDLE}",
                  fill=WATERMARK_COLOR, font=font_watermark)

        fd, path = tempfile.mkstemp(suffix=".png")
        os.close(fd)
        img.save(path, "PNG")
        logger.info("Generated weekly scorecard (%s)", path)
        return path

    except Exception as exc:
        logger.warning("generate_weekly_scorecard failed: %s", exc)
        if 'path' in locals():
            try:
                os.unlink(path)
            except OSError:
                pass
        return None
