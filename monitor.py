#!/usr/bin/env python3
"""IBEX serial stream monitor — Flask + pyserial MJPEG viewer."""

from __future__ import annotations

import io
import logging
import multiprocessing as mp
import os
import queue
import struct
import sys
import threading
import time
from collections import deque
from dataclasses import dataclass
from typing import Any

import cv2
import numpy as np
import serial
from flask import Flask, Response, jsonify, render_template, request, send_from_directory
from PIL import Image, ImageDraw
from serial.tools import list_ports

logging.getLogger("werkzeug").setLevel(logging.ERROR)

HEADER = bytes([0xAA, 0x55, 0xAA, 0x55, 0xAA, 0x55])
HEADER_SIZE = len(HEADER)
META_SIZE = 5
PACKET_PREFIX = HEADER_SIZE + META_SIZE
BAUD_RATE = 115_200
IMG_W = 640
IMG_H = 640
BBOX_SIZE = 16
PROFILE_SIZE = 16
PAD_SIZE = 6
RGB888_SIZE = IMG_W * IMG_H * 3
RGB565_SIZE = IMG_W * IMG_H * 2
RAW8_SIZE = IMG_W * IMG_H
MAX_BBOX_COUNT = 64
IBEX_BAYER_FORMATS = {3, 4, 5, 6}
WB_GAIN_MIN = 0.5
WB_GAIN_MAX = 2.5
AWB_DOWNSAMPLE = 8
AWB_CLIP_LOW = 8
AWB_CLIP_HIGH = 247
AWB_GREY_CHROMA_MAX = 24
AWB_MIN_VALID_PIXELS = 16
BYTE_QUEUE_MAX = 64
MAX_MAIN_BUFFER = 8 * 1024 * 1024
READ_CHUNK_DEFAULT = 128 * 1024
READ_CHUNK_MIN = 1024 * 64
READ_CHUNK_MAX = 1024 * 4096
SERIAL_READ_TIMEOUT = 0.001
MAX_DETECTION_HISTORY_SEC = 30 * 60
ROLLING_AVG_WINDOW_SEC = 1.0
DETECTION_PLOT_INTERVAL_SEC = .2

FORMAT_NAMES = {
    0: "RGB888",
    1: "RGB888",
    2: "RGB565",
    3: "RAW8-RGGB",
    4: "RAW8-BGGR",
    5: "RAW8-GBRG",
    6: "RAW8-GRBG",
}

VALID_IMG_SIZES = (RGB888_SIZE, RGB565_SIZE, RAW8_SIZE)

BAYER_CV2_CODES = {
    3: cv2.COLOR_BAYER_RG2BGR,
    4: cv2.COLOR_BAYER_BG2BGR,
    5: cv2.COLOR_BAYER_GB2BGR,
    6: cv2.COLOR_BAYER_GR2BGR,
}


def _demosaic_with_wb(bayer: np.ndarray, fmt: int, wb_r: float, wb_b: float) -> np.ndarray:
    code = BAYER_CV2_CODES.get(fmt)
    if code is None:
        raise ValueError(f"unsupported Bayer format: {fmt}")
    rgb = cv2.cvtColor(bayer, code).astype(np.float32)
    rgb[:, :, 0] = np.minimum(255.0, rgb[:, :, 0] * wb_r)
    rgb[:, :, 2] = np.minimum(255.0, rgb[:, :, 2] * wb_b)
    return rgb


def compute_awb_adjustment(
    data: bytes, fmt: int, wb_r: float, wb_b: float
) -> tuple[float, float]:
    """Return per-channel gain multipliers from demosaiced grey midtones (1.0 = neutral)."""
    if fmt not in IBEX_BAYER_FORMATS:
        return 1.0, 1.0

    raw = np.frombuffer(data, dtype=np.uint8)
    rows = min(len(raw) // IMG_W, IMG_H)
    if rows <= 0:
        return 1.0, 1.0

    bayer = raw[: rows * IMG_W].reshape(rows, IMG_W)
    try:
        rgb = _demosaic_with_wb(bayer, fmt, wb_r, wb_b)
    except ValueError:
        return 1.0, 1.0

    small = rgb[::AWB_DOWNSAMPLE, ::AWB_DOWNSAMPLE]
    chroma = small.max(axis=2) - small.min(axis=2)
    clip_valid = (
        (small[:, :, 0] > AWB_CLIP_LOW)
        & (small[:, :, 0] < AWB_CLIP_HIGH)
        & (small[:, :, 1] > AWB_CLIP_LOW)
        & (small[:, :, 1] < AWB_CLIP_HIGH)
        & (small[:, :, 2] > AWB_CLIP_LOW)
        & (small[:, :, 2] < AWB_CLIP_HIGH)
    )
    valid = clip_valid & (chroma <= AWB_GREY_CHROMA_MAX)
    if int(valid.sum()) < AWB_MIN_VALID_PIXELS:
        valid = clip_valid
    if int(valid.sum()) < AWB_MIN_VALID_PIXELS:
        return 1.0, 1.0

    means = small[valid].mean(axis=0)
    mean_display_r, mean_g, mean_display_b = means[0], means[1], means[2]
    if mean_display_r < 1e-3 or mean_display_b < 1e-3:
        return 1.0, 1.0

    return float(mean_g / mean_display_r), float(mean_g / mean_display_b)

MP_CTX = mp.get_context("spawn")

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
RESOURCES_TEMPLATES_DIR = os.path.join(BASE_DIR, "resources", "templates")

app = Flask(__name__)


@dataclass
class Frame:
    fmt: int
    expected_size: int
    raw_image: bytes
    footer: bytes | None


@dataclass
class DetectionSample:
    t: float
    class_counts: dict[int, int]


class StreamState:
    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.stop_event = threading.Event()
        self.read_process: mp.Process | None = None
        self.display_thread: threading.Thread | None = None
        self.byte_queue: mp.Queue | None = None
        self.reader_stop_event: mp.synchronize.Event | None = None
        self.chunk_size_value: mp.sharedctypes.Synchronized | None = None
        self.latest_jpeg: bytes | None = None
        self.frame_version = 0
        self.connected = False
        self.error: str | None = None
        self.fmt = "--"
        self.fps = 0
        self.buf_kb = 0
        self.byte_queue_len = 0
        self.status_text = "Status: Disconnected"
        self._frame_count = 0
        self._last_fps_time = time.monotonic()
        self.wb_r = 1.7
        self.wb_b = 1.55
        self.wb_awb = True
        self.read_chunk_size = READ_CHUNK_DEFAULT
        self.detection_history: deque[DetectionSample] = deque()

    def reset_stats(self) -> None:
        self.fmt = "--"
        self.fps = 0
        self.buf_kb = 0
        self.byte_queue_len = 0
        self.status_text = "Status: Disconnected"
        self.error = None
        self._frame_count = 0
        self._last_fps_time = time.monotonic()
        self.read_chunk_size = READ_CHUNK_DEFAULT
        self.detection_history.clear()
        if self.chunk_size_value is not None:
            self.chunk_size_value.value = READ_CHUNK_DEFAULT


state = StreamState()


def put_bytes_on_queue(byte_queue: mp.Queue, chunk: bytes) -> None:
    while True:
        try:
            byte_queue.put_nowait(chunk)
            break
        except queue.Full:
            try:
                byte_queue.get_nowait()
            except queue.Empty:
                break


def serial_reader_worker(
    port: str,
    byte_queue: mp.Queue,
    stop_event: mp.synchronize.Event,
    chunk_size: mp.sharedctypes.Synchronized,
) -> None:
    ser: serial.Serial | None = None
    try:
        ser = serial.Serial(port, baudrate=BAUD_RATE, timeout=SERIAL_READ_TIMEOUT)
    except serial.SerialException as exc:
        put_bytes_on_queue(byte_queue, ("error", str(exc)))
        put_bytes_on_queue(byte_queue, None)
        return

    try:
        while not stop_event.is_set():
            waiting = ser.in_waiting
            size = max(READ_CHUNK_MIN, min(READ_CHUNK_MAX, waiting))
            chunk = ser.read(size)
            if chunk:
                put_bytes_on_queue(byte_queue, chunk)
    except serial.SerialException as exc:
        put_bytes_on_queue(byte_queue, ("error", str(exc)))
    finally:
        if ser is not None:
            try:
                ser.close()
            except serial.SerialException:
                pass
        put_bytes_on_queue(byte_queue, None)


def header_matches_at(buf: bytes | bytearray, idx: int) -> bool:
    if idx < 0 or idx + HEADER_SIZE > len(buf):
        return False
    return bytes(buf[idx : idx + HEADER_SIZE]) == HEADER


def find_header_pattern(buf: bytes | bytearray, start: int = 0) -> int:
    limit = len(buf) - HEADER_SIZE
    for i in range(start, limit + 1):
        if header_matches_at(buf, i):
            return i
    return -1


def trim_buffer_for_resync(buffer: bytearray) -> None:
    keep = HEADER_SIZE - 1
    del buffer[: len(buffer) - keep]


def parse_meta(buf: bytes | bytearray, pos: int) -> tuple[int, int] | None:
    if pos + PACKET_PREFIX > len(buf):
        return None
    if not header_matches_at(buf, pos):
        return None
    fmt = buf[pos + HEADER_SIZE]
    if fmt not in FORMAT_NAMES:
        return None
    expected_size = struct.unpack_from("<I", buf, pos + HEADER_SIZE + 1)[0]
    if expected_size not in VALID_IMG_SIZES:
        return None
    return fmt, expected_size


def sync_buffer(buffer: bytearray) -> bool:
    """Align buffer so the current frame header is at offset 0."""
    while True:
        header_idx = find_header_pattern(buffer)
        if header_idx == -1:
            trim_buffer_for_resync(buffer)
            return False
        if header_idx > 0:
            del buffer[:header_idx]
        if len(buffer) < PACKET_PREFIX:
            return False
        if parse_meta(buffer, 0) is not None:
            return True
        del buffer[0:1]


def find_next_valid_header(buf: bytes | bytearray, start: int) -> int:
    limit = len(buf) - PACKET_PREFIX
    if start > limit:
        return -1
    pos = start
    search_end = limit + HEADER_SIZE
    while True:
        pos = buf.find(HEADER, pos, search_end)
        if pos < 0:
            return -1
        if parse_meta(buf, pos) is not None:
            return pos
        pos += 1


def iter_valid_header_positions(buf: bytes | bytearray, start: int = 0) -> list[int]:
    positions: list[int] = []
    limit = len(buf) - PACKET_PREFIX
    if start > limit:
        return positions
    pos = start
    search_end = limit + HEADER_SIZE
    while True:
        pos = buf.find(HEADER, pos, search_end)
        if pos < 0:
            break
        if parse_meta(buf, pos) is not None:
            positions.append(pos)
        pos += 1
    return positions


def skip_to_newest_complete_frame(buffer: bytearray) -> bool:
    """When backed up, discard older complete frames. Returns True if behind."""
    if not sync_buffer(buffer):
        return False
    meta = parse_meta(buffer, 0)
    if meta is None:
        return False
    _, expected_size = meta
    if len(buffer) <= 2 * expected_size:
        return False

    for start in reversed(iter_valid_header_positions(buffer)):
        if find_next_valid_header(buffer, start + PACKET_PREFIX) >= 0:
            if start > 0:
                del buffer[:start]
                sync_buffer(buffer)
            return True
    return True


def footer_byte_len(bbox_count: int) -> int:
    return 1 + bbox_count * BBOX_SIZE + PROFILE_SIZE + PAD_SIZE


def try_parse_footer(footer: bytes) -> bytes | None:
    if not footer:
        return None
    bbox_count = footer[0]
    if bbox_count > MAX_BBOX_COUNT:
        return None
    expected = footer_byte_len(bbox_count)
    if len(footer) == expected:
        return footer
    # Bboxes then next header, no profile/padding
    bbox_only = 1 + bbox_count * BBOX_SIZE
    if len(footer) >= bbox_only:
        return footer[:bbox_only]
    return None


def try_emit_frame(buffer: bytearray) -> Frame | None:
    if not sync_buffer(buffer):
        return None

    meta = parse_meta(buffer, 0)
    if meta is None:
        return None
    fmt, expected_size = meta

    next_pos = find_next_valid_header(buffer, PACKET_PREFIX)
    if next_pos < 0:
        return None

    image_start = PACKET_PREFIX
    full_image_end = image_start + expected_size
    if next_pos <= full_image_end:
        raw_image = bytes(buffer[image_start:next_pos])
        parsed_footer = None
    else:
        raw_image = bytes(buffer[image_start:full_image_end])
        parsed_footer = try_parse_footer(bytes(buffer[full_image_end:next_pos]))
        # if parsed_footer is None:
        #     return None

    del buffer[:next_pos]
    return Frame(fmt=fmt, expected_size=expected_size, raw_image=raw_image, footer=parsed_footer)


def demosaic_bayer_rows(
    data: bytes, fmt: int, wb_r: float, wb_b: float, rows: int
) -> np.ndarray:
    raw = np.frombuffer(data, dtype=np.uint8)
    rows = min(rows, IMG_H, len(raw) // IMG_W)
    if rows <= 0:
        return np.zeros((0, IMG_W, 3), dtype=np.uint8)

    bayer = raw[: rows * IMG_W].reshape(rows, IMG_W)
    code = BAYER_CV2_CODES.get(fmt)
    if code is None:
        return np.zeros((rows, IMG_W, 3), dtype=np.uint8)

    rgb = _demosaic_with_wb(bayer, fmt, wb_r, wb_b)
    return rgb.astype(np.uint8)


def demosaic_bayer(data: bytes, fmt: int, wb_r: float, wb_b: float) -> np.ndarray:
    return demosaic_bayer_rows(data, fmt, wb_r, wb_b, IMG_H)


def decode_partial_image(
    data: bytes, fmt: int, wb_r: float, wb_b: float
) -> np.ndarray | None:
    if fmt in IBEX_BAYER_FORMATS:
        rows = min(len(data) // IMG_W, IMG_H)
        if rows <= 0:
            return None
        return demosaic_bayer_rows(data[: rows * IMG_W], fmt, wb_r, wb_b, rows)

    if fmt in (0, 1):
        row_bytes = IMG_W * 3
        rows = min(len(data) // row_bytes, IMG_H)
        if rows <= 0:
            return None
        return np.frombuffer(data[: rows * row_bytes], dtype=np.uint8).reshape(rows, IMG_W, 3)

    if fmt == 2:
        row_bytes = IMG_W * 2
        rows = min(len(data) // row_bytes, IMG_H)
        if rows <= 0:
            return None
        pixels = np.frombuffer(data[: rows * row_bytes], dtype="<u2").reshape(rows, IMG_W)
        r = ((pixels >> 11) & 0x1F) << 3
        g = ((pixels >> 5) & 0x3F) << 2
        b = (pixels & 0x1F) << 3
        return np.stack([r, g, b], axis=-1).astype(np.uint8)

    img = decode_image(data, fmt, len(data), wb_r, wb_b)
    if img is None:
        return None
    return np.array(img)


def composite_invalid_frame(
    partial_rgb: np.ndarray, prev_img: Image.Image | None
) -> Image.Image:
    if prev_img is None:
        out = np.zeros((IMG_H, IMG_W, 3), dtype=np.uint8)
    else:
        out = np.array(prev_img)

    copy_rows = min(partial_rgb.shape[0], IMG_H)
    if copy_rows > 0:
        out[:copy_rows] = partial_rgb[:copy_rows]
    return Image.fromarray(out, mode="RGB")


def decode_image(data: bytes, fmt: int, img_size: int, wb_r: float, wb_b: float) -> Image.Image | None:
    if fmt == 0 and img_size != RGB888_SIZE:
        try:
            return Image.open(io.BytesIO(data)).convert("RGB")
        except OSError:
            return None

    if fmt in (0, 1):
        arr = np.frombuffer(data, dtype=np.uint8).reshape(IMG_H, IMG_W, 3)
        return Image.fromarray(arr, mode="RGB")

    if fmt == 2:
        pixels = np.frombuffer(data, dtype="<u2").reshape(IMG_H, IMG_W)
        r = ((pixels >> 11) & 0x1F) << 3
        g = ((pixels >> 5) & 0x3F) << 2
        b = (pixels & 0x1F) << 3
        arr = np.stack([r, g, b], axis=-1).astype(np.uint8)
        return Image.fromarray(arr, mode="RGB")

    if fmt in IBEX_BAYER_FORMATS:
        arr = demosaic_bayer(data, fmt, wb_r, wb_b)
        return Image.fromarray(arr, mode="RGB")

    return None


def decode_frame(
    frame: Frame,
    last_image: Image.Image | None,
    wb_r: float,
    wb_b: float,
) -> Image.Image | None:
    image_data = frame.raw_image
    if len(image_data) < frame.expected_size:
        partial = decode_partial_image(image_data, frame.fmt, wb_r, wb_b)
        if partial is not None:
            return composite_invalid_frame(partial, last_image)
        if last_image is not None:
            return last_image.copy()
        return None
    return decode_image(image_data, frame.fmt, frame.expected_size, wb_r, wb_b)


def parse_profile(footer: bytes) -> dict[str, int] | None:
    bbox_count = footer[0]
    if bbox_count > MAX_BBOX_COUNT:
        return None
    profile_start = 1 + bbox_count * BBOX_SIZE
    profile_end = profile_start + PROFILE_SIZE
    if len(footer) < profile_end:
        return None
    return {
        "t_pre": struct.unpack_from("<I", footer, profile_start)[0],
        "t_inf": struct.unpack_from("<I", footer, profile_start + 4)[0],
        "t_post": struct.unpack_from("<I", footer, profile_start + 8)[0],
        "t_usb": struct.unpack_from("<I", footer, profile_start + 12)[0],
    }


def color_for_class(class_id: int) -> tuple[int, int, int]:
    seed = class_id & 0xFFFFFFFF
    seed = (seed * 1664525 + 16) & 0xFFFFFFFF
    r = seed % 256
    seed = (seed * 1664525 + 16) & 0xFFFFFFFF
    g = seed % 256
    seed = (seed * 1664525 + 16) & 0xFFFFFFFF
    b = seed % 256
    return (r, g, b)


def color_hex_for_class(class_id: int) -> str:
    r, g, b = color_for_class(class_id)
    return f"#{r:02x}{g:02x}{b:02x}"


def class_counts_from_bboxes(
    bboxes: list[tuple[int, int, int, int, int, float]],
) -> dict[int, int]:
    counts: dict[int, int] = {}
    for *_, class_id, _conf in bboxes:
        counts[class_id] = counts.get(class_id, 0) + 1
    return counts


def record_detection_sample(class_counts: dict[int, int]) -> None:
    now = time.monotonic()
    with state.lock:
        state.detection_history.append(DetectionSample(now, class_counts))
        cutoff = now - MAX_DETECTION_HISTORY_SEC
        while state.detection_history and state.detection_history[0].t < cutoff:
            state.detection_history.popleft()


def build_rolling_points_for_class(
    samples: list[DetectionSample],
    class_id: int,
    t0: float,
) -> list[list[float]]:
    if not samples:
        return []

    points: list[list[float]] = []
    window_start = 0
    window_sum = 0
    window_n = 0
    next_emit_t = samples[0].t

    for sample in samples:
        c = sample.class_counts.get(class_id, 0)
        window_sum += c
        window_n += 1
        while (
            window_start < len(samples)
            and samples[window_start].t < sample.t - ROLLING_AVG_WINDOW_SEC
        ):
            window_sum -= samples[window_start].class_counts.get(class_id, 0)
            window_n -= 1
            window_start += 1

        if sample.t + 1e-9 >= next_emit_t:
            avg = window_sum / window_n if window_n else 0.0
            points.append([sample.t - t0, avg])
            next_emit_t = sample.t + DETECTION_PLOT_INTERVAL_SEC

    last_x = samples[-1].t - t0
    if not points or points[-1][0] < last_x - 1e-9:
        avg = window_sum / window_n if window_n else 0.0
        points.append([last_x, avg])

    return points


def build_detection_series(samples: list[DetectionSample]) -> list[dict[str, Any]]:
    if not samples:
        return []
    t0 = samples[0].t
    class_ids: set[int] = set()
    for sample in samples:
        class_ids.update(sample.class_counts.keys())
    series: list[dict[str, Any]] = []
    for class_id in sorted(class_ids):
        points = build_rolling_points_for_class(samples, class_id, t0)
        series.append(
            {
                "class_id": class_id,
                "color": color_hex_for_class(class_id),
                "points": points,
            }
        )
    return series


def parse_bboxes(footer: bytes) -> list[tuple[int, int, int, int, int, float]]:
    if not footer:
        return []
    bbox_count = footer[0]
    if bbox_count == 0 or bbox_count > MAX_BBOX_COUNT:
        return []
    bboxes: list[tuple[int, int, int, int, int, float]] = []
    off = 1
    for _ in range(bbox_count):
        if off + BBOX_SIZE > len(footer):
            break
        x1, y1, x2, y2 = struct.unpack_from("<HHHH", footer, off)
        class_id = struct.unpack_from("<I", footer, off + 8)[0]
        conf = struct.unpack_from("<f", footer, off + 12)[0]
        bboxes.append((x1, y1, x2, y2, class_id, conf))
        off += BBOX_SIZE
    return bboxes


def normalize_bbox(
    x1: int, y1: int, x2: int, y2: int, img_w: int, img_h: int
) -> tuple[int, int, int, int] | None:
    left, right = min(x1, x2), max(x1, x2)
    top, bottom = min(y1, y2), max(y1, y2)
    left = max(0, min(left, img_w))
    right = max(0, min(right, img_w))
    top = max(0, min(top, img_h))
    bottom = max(0, min(bottom, img_h))
    if right <= left or bottom <= top:
        return None
    return left, top, right, bottom


def prepare_bboxes(
    footer: bytes | None, img_w: int, img_h: int
) -> list[tuple[int, int, int, int, int, float]]:
    drawable: list[tuple[int, int, int, int, int, float]] = []
    for x1, y1, x2, y2, class_id, conf in parse_bboxes(footer or b""):
        norm = normalize_bbox(x1, y1, x2, y2, img_w, img_h)
        if norm is None:
            continue
        drawable.append((*norm, class_id, conf))
    return drawable


def draw_bboxes(
    img: Image.Image, bboxes: list[tuple[int, int, int, int, int, float]]
) -> Image.Image:
    if not bboxes:
        return img
    draw = ImageDraw.Draw(img)
    for x1, y1, x2, y2, class_id, _conf in bboxes:
        draw.rectangle([x1, y1, x2, y2], outline=color_for_class(class_id), width=2)
    return img


def encode_jpeg(img: Image.Image) -> bytes:
    if img.size != (IMG_W, IMG_H):
        canvas = Image.new("RGB", (IMG_W, IMG_H))
        canvas.paste(img, (0, 0))
        img = canvas
    out = io.BytesIO()
    img.save(out, format="JPEG", quality=85)
    return out.getvalue()


def update_fps_counter(t_inf: float | int | None = None) -> None:
    state._frame_count += 1
    now = time.monotonic()
    if now - state._last_fps_time >= 1.0:
        state.fps = state._frame_count
        state._frame_count = 0
        state._last_fps_time = now
    if t_inf is not None:
        state.fps = int(1000. / t_inf)

def build_status_text(
    fmt: int,
    expected_size: int,
    received_size: int,
    profile: dict[str, int] | None,
    buf_kb: int,
    byte_queue_len: int,
    bboxes: list[tuple[int, int, int, int, int, float]] | None = None,
) -> str:
    fmt_name = FORMAT_NAMES.get(fmt, str(fmt))
    partial = " (partial)" if received_size < expected_size else ""
    lines = [
        "Status: Connected",
        f"fmt={fmt_name}  img={received_size}/{expected_size}B{partial}",
        f"buf/queue: {buf_kb}KB / {byte_queue_len}",
    ]
    if profile:
        lines.append(
            "Profiling (ms): "
            f"Pre={profile['t_pre']} Inf={profile['t_inf']} "
            f"Post={profile['t_post']} USB={profile['t_usb']}"
        )
    else:
        lines.append("Profiling (ms): Pre= Inf= Post= USB=")
    for x1, y1, x2, y2, class_id, conf in bboxes or []:
        lines.append(f"  #{class_id} {conf:.0f}%  xyxy=({x1},{y1},{x2},{y2})")
    return "\n".join(lines)


def _byte_queue_size(byte_queue: mp.Queue | None) -> int:
    if byte_queue is None:
        return 0
    try:
        return byte_queue.qsize()
    except (NotImplementedError, OSError):
        return 0


def drain_byte_queue(byte_queue: mp.Queue | None) -> None:
    if byte_queue is None:
        return
    while True:
        try:
            byte_queue.get_nowait()
        except queue.Empty:
            break


def _handle_reader_exit(item: Any) -> bool:
    """Returns True if the display loop should stop."""
    if item is None:
        with state.lock:
            state.connected = False
            if state.error is None:
                state.status_text = "Status: Disconnected"
        return True
    if isinstance(item, tuple) and len(item) == 2 and item[0] == "error":
        with state.lock:
            state.connected = False
            state.error = item[1]
            state.status_text = f"Error: {item[1]}"
        return True
    return False


def display_loop(byte_queue: mp.Queue) -> None:
    buffer = bytearray()
    last_image: Image.Image | None = None
    pending_awb_adj_r = 1.0
    pending_awb_adj_b = 1.0

    while not state.stop_event.is_set():
        while True:
            try:
                item = byte_queue.get_nowait()
            except queue.Empty:
                break
            if _handle_reader_exit(item):
                return
            buffer.extend(item)

        with state.lock:
            state.buf_kb = len(buffer) // 1024
            state.byte_queue_len = _byte_queue_size(byte_queue)

        decoded_any = False
        behind = skip_to_newest_complete_frame(buffer)
        while True:
            frame = try_emit_frame(buffer)
            if frame is None:
                break

            with state.lock:
                wb_awb = state.wb_awb
                wb_r = state.wb_r
                wb_b = state.wb_b
                if wb_awb and frame.fmt in IBEX_BAYER_FORMATS:
                    wb_r = max(
                        WB_GAIN_MIN,
                        min(WB_GAIN_MAX, wb_r * pending_awb_adj_r),
                    )
                    wb_b = max(
                        WB_GAIN_MIN,
                        min(WB_GAIN_MAX, wb_b * pending_awb_adj_b),
                    )
                    state.wb_r = wb_r
                    state.wb_b = wb_b

            img = decode_frame(frame, last_image, wb_r, wb_b)
            if img is None:
                if behind:
                    break
                continue

            if wb_awb and frame.fmt in IBEX_BAYER_FORMATS:
                pending_awb_adj_r, pending_awb_adj_b = compute_awb_adjustment(
                    frame.raw_image, frame.fmt, wb_r, wb_b
                )
            else:
                pending_awb_adj_r = 1.0
                pending_awb_adj_b = 1.0

            last_image = img.copy()
            drawable_bboxes = prepare_bboxes(frame.footer, img.width, img.height)
            display_img = draw_bboxes(img, drawable_bboxes)
            profile = parse_profile(frame.footer) if frame.footer else None
            class_counts = class_counts_from_bboxes(drawable_bboxes)

            jpg_image = encode_jpeg(display_img)

            with state.lock:
                state.latest_jpeg = jpg_image
                state.frame_version += 1
                state.fmt = FORMAT_NAMES.get(frame.fmt, str(frame.fmt))
                state.byte_queue_len = _byte_queue_size(byte_queue)
                update_fps_counter(getattr(profile, 't_inf', None))
                state.status_text = build_status_text(
                    frame.fmt,
                    frame.expected_size,
                    len(frame.raw_image),
                    profile,
                    state.buf_kb,
                    state.byte_queue_len,
                    drawable_bboxes,
                )
            record_detection_sample(class_counts)
            decoded_any = True
            if behind:
                break

        if not decoded_any:
            time.sleep(0.05)


def _stop_reader_process() -> None:
    if state.reader_stop_event is not None:
        state.reader_stop_event.set()
    if state.read_process is not None and state.read_process.is_alive():
        state.read_process.join(timeout=2.0)
        if state.read_process.is_alive():
            state.read_process.terminate()
            state.read_process.join(timeout=1.0)


def start_stream(port_name: str) -> None:
    with state.lock:
        if state.read_process is not None and state.read_process.is_alive():
            raise RuntimeError("Stream already running")

        state.stop_event.clear()
        state.reset_stats()
        state.latest_jpeg = None
        state.frame_version = 0
        state.buf_kb = 0
        state.error = None
        state.connected = True
        state.status_text = "Status: Connected"

        state.byte_queue = MP_CTX.Queue(maxsize=BYTE_QUEUE_MAX)
        state.reader_stop_event = MP_CTX.Event()
        state.chunk_size_value = MP_CTX.Value("i", state.read_chunk_size)
        state.read_process = MP_CTX.Process(
            target=serial_reader_worker,
            args=(
                port_name,
                state.byte_queue,
                state.reader_stop_event,
                state.chunk_size_value,
            ),
            name="ibex-serial",
            daemon=True,
        )

    byte_queue = state.byte_queue
    assert byte_queue is not None
    state.read_process.start()
    state.display_thread = threading.Thread(
        target=display_loop,
        args=(byte_queue,),
        daemon=True,
        name="ibex-display",
    )
    state.display_thread.start()


def _join_display_thread() -> None:
    if state.display_thread and state.display_thread.is_alive():
        state.display_thread.join(timeout=2.0)
    state.display_thread = None


def stop_stream() -> None:
    _stop_reader_process()
    state.stop_event.set()
    _join_display_thread()
    drain_byte_queue(state.byte_queue)
    with state.lock:
        state.read_process = None
        state.byte_queue = None
        state.reader_stop_event = None
        state.chunk_size_value = None
        state.connected = False
        state.latest_jpeg = None
        state.buf_kb = 0
        state.byte_queue_len = 0
        state.reset_stats()


@app.route("/")
def index() -> str:
    return render_template("monitor.html")


@app.route("/resources/templates/<path:filename>")
def resources_templates(filename: str):
    return send_from_directory(RESOURCES_TEMPLATES_DIR, filename)


@app.get("/api/ports")
def api_ports():
    ports = [
        {"device": p.device, "description": p.description or p.device}
        for p in list_ports.comports()
    ]
    return jsonify(ports)


@app.post("/api/start")
def api_start():
    body = request.get_json(silent=True) or {}
    port = body.get("port")
    if not port:
        return jsonify({"error": "port required"}), 400

    try:
        start_stream(port)
    except RuntimeError as exc:
        return jsonify({"error": str(exc)}), 409
    except Exception as exc:  # noqa: BLE001
        return jsonify({"error": str(exc)}), 500

    return jsonify({"ok": True})


@app.post("/api/stop")
def api_stop():
    stop_stream()
    return jsonify({"ok": True})


@app.post("/api/wb")
def api_wb():
    body = request.get_json(silent=True) or {}
    if "awb" in body:
        wb_awb = bool(body["awb"])
    else:
        wb_awb = None

    try:
        wb_r = float(body.get("r", state.wb_r))
        wb_b = float(body.get("b", state.wb_b))
    except (TypeError, ValueError):
        return jsonify({"error": "r and b must be numbers"}), 400

    wb_r = max(WB_GAIN_MIN, min(WB_GAIN_MAX, wb_r))
    wb_b = max(WB_GAIN_MIN, min(WB_GAIN_MAX, wb_b))

    with state.lock:
        if wb_awb is not None:
            state.wb_awb = wb_awb
        if wb_awb is not True:
            state.wb_r = wb_r
            state.wb_b = wb_b
        wb_awb = state.wb_awb
        wb_r = state.wb_r
        wb_b = state.wb_b

    return jsonify({"ok": True, "r": wb_r, "b": wb_b, "awb": wb_awb})


@app.get("/api/detections")
def api_detections():
    minutes = request.args.get("minutes", default=1, type=float)
    if minutes is None:
        minutes = 1.0
    minutes = max(1.0, min(30.0, minutes))
    window_sec = minutes * 60.0
    cutoff = time.monotonic() - window_sec

    with state.lock:
        samples = [s for s in state.detection_history if s.t >= cutoff]

    return jsonify(
        {
            "window_sec": window_sec,
            "series": build_detection_series(samples),
        }
    )


@app.get("/api/stats")
def api_stats():
    with state.lock:
        return jsonify(
            {
                "connected": state.connected,
                "fmt": state.fmt,
                "fps": state.fps,
                "buf_kb": state.buf_kb,
                "byte_queue_len": state.byte_queue_len,
                "status_text": state.status_text,
                "error": state.error,
                "wb_r": state.wb_r,
                "wb_b": state.wb_b,
                "wb_awb": state.wb_awb,
                "is_raw": state.fmt.startswith("RAW8"),
            }
        )


def mjpeg_generator():
    last_version = -1
    while True:
        with state.lock:
            version = state.frame_version
            frame = state.latest_jpeg

        if frame and version != last_version:
            last_version = version
            yield (
                b"--frame\r\n"
                b"Content-Type: image/jpeg\r\n\r\n" + frame + b"\r\n"
            )
        else:
            time.sleep(0.05)


@app.route("/video_feed")
def video_feed():
    return Response(
        mjpeg_generator(),
        mimetype="multipart/x-mixed-replace; boundary=frame",
    )


PORT = 8765  # avoid macOS AirPlay Receiver on :5000

if __name__ == "__main__":
    mp.freeze_support()
    url = f"http://127.0.0.1:{PORT}/"
    print(f"Open in any browser: {url}")
    app.run(host="127.0.0.1", port=PORT, threaded=True)
