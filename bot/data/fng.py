# data/fng.py
"""Crypto Fear & Greed Index — data fetch + gauge image generation."""
from __future__ import annotations

import math
import os
import tempfile
import logging
import requests

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.font_manager as fm
import matplotlib.patches as patches
import matplotlib.patheffects as pe
import numpy as np

from bot.config import BOT_HANDLE

logger = logging.getLogger(__name__)

FNG_API = "https://api.alternative.me/fng/?limit=1"

SEGMENTS = [
    (0,   25, "#EA3943", "Extreme Fear"),
    (25,  45, "#EA8C00", "Fear"),
    (45,  55, "#F5D100", "Neutral"),
    (55,  75, "#93D900", "Greed"),
    (75, 100, "#16C784", "Extreme Greed"),
]

BG = "#0d1117"
CARD_BG = "#151A22"

# Best available font — macOS paths + Linux fallback
_FONT_PATH = None
for _p in ["/System/Library/Fonts/SFNS.ttf",
           "/System/Library/Fonts/HelveticaNeue.ttc",
           "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
           "/usr/share/fonts/truetype/liberation/LiberationSans-Bold.ttf"]:
    if not os.path.isfile(_p):
        continue
    try:
        fm.FontProperties(fname=_p)
        _FONT_PATH = _p
        break
    except Exception:
        continue


def _font(size, weight="bold"):
    if _FONT_PATH:
        return fm.FontProperties(fname=_FONT_PATH, size=size, weight=weight)
    return fm.FontProperties(family="sans-serif", size=size, weight=weight)


def get_label_and_color(value):
    for lo, hi, color, label in SEGMENTS:
        if lo <= value <= hi:
            return label, color
    return (SEGMENTS[0][3], SEGMENTS[0][2]) if value < 0 else (SEGMENTS[-1][3], SEGMENTS[-1][2])


def fetch_fng() -> dict | None:
    """Fetch current Fear & Greed Index. Returns {value, label, timestamp} or None."""
    try:
        resp = requests.get(FNG_API, timeout=10)
        resp.raise_for_status()
        data = resp.json().get("data", [])
        if not data:
            return None
        entry = data[0]
        value = int(entry["value"])
        label, color = get_label_and_color(value)
        return {
            "value": value,
            "label": label,
            "color": color,
            "api_label": entry.get("value_classification", label),
            "timestamp": int(entry.get("timestamp", 0)),
        }
    except Exception as exc:
        logger.warning("Failed to fetch Fear & Greed Index: %s", exc)
        return None


def generate_gauge_image(value, date_str, output_path=None):
    """Generate a CMC-quality gauge PNG. Returns file path."""
    label, color = get_label_and_color(value)

    if output_path is None:
        tmp = tempfile.NamedTemporaryFile(suffix=".png", delete=False)
        output_path = tmp.name
        tmp.close()

    fig, ax = plt.subplots(figsize=(8, 6.4), facecolor=BG)
    ax.set_facecolor(BG)
    ax.set_xlim(-1.55, 1.55)
    ax.set_ylim(-0.9, 1.65)
    ax.set_aspect("equal")
    ax.axis("off")

    # Card shadow + background
    shadow = patches.FancyBboxPatch(
        (-1.42, -0.75), 2.84, 2.25,
        boxstyle="round,pad=0.12",
        facecolor="#000000", edgecolor="none", alpha=0.3, zorder=0,
    )
    ax.add_patch(shadow)
    card = patches.FancyBboxPatch(
        (-1.4, -0.72), 2.8, 2.22,
        boxstyle="round,pad=0.12",
        facecolor=CARD_BG, edgecolor="#1c2129", linewidth=1, zorder=1,
    )
    ax.add_patch(card)

    # Arc segments — thick stroked lines with round caps
    arc_r = 0.88
    line_w = 28
    gap = 2.5

    for lo, hi, seg_color, _ in SEGMENTS:
        a1 = math.radians(180 - (hi / 100) * 180 + gap / 2)
        a2 = math.radians(180 - (lo / 100) * 180 - gap / 2)
        theta = np.linspace(a1, a2, 80)
        x = arc_r * np.cos(theta)
        y = arc_r * np.sin(theta)
        ax.plot(x, y, color=seg_color, linewidth=line_w,
                solid_capstyle="round", zorder=3, alpha=0.95)

    # Position indicator — dark dot with white ring
    angle_rad = math.radians(180 - (value / 100) * 180)
    ix = arc_r * math.cos(angle_rad)
    iy = arc_r * math.sin(angle_rad)

    ax.plot(ix, iy - 0.01, "o", color="black", markersize=19, alpha=0.4, zorder=4)
    ax.plot(ix, iy, "o", color="#0d1117", markersize=17, zorder=5)
    ax.plot(ix, iy, "o", color="none", markersize=17,
            markeredgecolor="white", markeredgewidth=2.2, zorder=6)

    # Value number
    ax.text(0, 0.35, str(value),
            color="white", fontproperties=_font(100, "bold"),
            ha="center", va="center", zorder=8,
            path_effects=[pe.withStroke(linewidth=6, foreground=CARD_BG)])

    # Status label
    ax.text(0, -0.02, label,
            color=color, fontproperties=_font(26, "bold"),
            ha="center", va="center", zorder=8)

    # Title
    ax.text(0, 1.35, "Crypto Fear & Greed Index",
            color="#6e7681", fontproperties=_font(13, "medium"),
            ha="center", va="center", zorder=8)

    # Date
    ax.text(0, -0.35, date_str,
            color="#484f58", fontproperties=_font(11, "normal"),
            ha="center", va="center", zorder=8)

    # Brand
    ax.text(0, -0.6, f"@{BOT_HANDLE}",
            color="#2d333b", fontproperties=_font(13, "bold"),
            ha="center", va="center", zorder=8)

    fig.savefig(output_path, dpi=180, facecolor=BG, bbox_inches="tight", pad_inches=0.15)
    plt.close(fig)
    return output_path
