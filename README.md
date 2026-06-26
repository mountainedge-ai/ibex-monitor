# IBEX Monitor

Web-based viewer for the IBEX serial image stream. Connects to an IBEX device over USB serial, decodes incoming frames, and displays a live MJPEG feed in the browser with format stats, buffer monitoring, and white-balance controls for RAW Bayer frames.

## Prerequisites

- **Python 3.11+**
- **[uv](https://docs.astral.sh/uv/)** — installs dependencies and manages the virtual environment
- **IBEX hardware** connected via USB serial
- **Serial port access** on your machine:
  - **macOS** — the device usually appears as `/dev/cu.usbserial-*` or `/dev/cu.usbmodem*`
  - **Linux** — often `/dev/ttyUSB0` or `/dev/ttyACM0`; your user may need membership in the `dialout` group
  - **Windows** — appears as `COM3`, `COM4`, etc.

Frames are **640×640** pixels.

## Installation

Clone the repository, then install dependencies with uv:

```bash
git clone <repo-url>
cd ibex-monitor
uv sync
```

## Running

Start the monitor:

```bash
uv run monitor.py
```

Open the URL printed in the terminal (default: [http://127.0.0.1:8765/](http://127.0.0.1:8765/)).

Port **8765** is used instead of Flask’s default 5000 to avoid conflicting with macOS AirPlay Receiver.

## Usage

1. Click **START STREAM**.
2. Select the serial port when prompted (if only one port is found, it connects automatically).
3. The live feed appears in the viewer. Stats show:
   - **FMT** — pixel format (e.g. `RGB888`, `RAW8-RGGB`)
   - **FPS** — decoded frame rate
   - **BUF** — receive buffer size / queue depth
4. Click **STOP STREAM** to disconnect.

### White balance (RAW formats only)

When the stream is a RAW8 Bayer format (`RAW8-RGGB`, `RAW8-BGGR`, `RAW8-GBRG`, or `RAW8-GRBG`), a white-balance panel appears beside the image. Gains are applied after demosaic (R and B channels). **AWB** is on by default: each frame measures grey midtones on an 8× downsampled demosaic and applies the correction multiplier on the next frame. Uncheck AWB or drag the picker to adjust R and B gain manually (0.5–2.5; G is fixed at 1.0).

### Supported pixel formats

| Format ID | Name        |
|-----------|-------------|
| 0, 1      | RGB888      |
| 2         | RGB565      |
| 3         | RAW8-RGGB   |
| 4         | RAW8-BGGR   |
| 5         | RAW8-GBRG   |
| 6         | RAW8-GRBG   |

Bounding-box metadata in the stream footer is drawn as overlays when present.

## API

The web UI uses these endpoints if you want to integrate programmatically:

| Method | Path          | Description                          |
|--------|---------------|--------------------------------------|
| GET    | `/`           | Monitor UI                           |
| GET    | `/video_feed` | MJPEG stream                         |
| GET    | `/api/ports`  | List available serial ports          |
| POST   | `/api/start`  | Start stream — body: `{"port": "..."}` |
| POST   | `/api/stop`   | Stop stream                          |
| GET    | `/api/stats`  | Connection status, FPS, format, etc. |
| POST   | `/api/wb`     | Set white balance — body: `{"r": 1.0, "b": 1.0, "awb": true}` |

## Troubleshooting

- **No serial ports listed** — check the USB cable and that the device driver is installed. On Linux, verify permissions (`ls -l /dev/ttyUSB*`).
- **Connection error on start** — another program may have the port open; close other serial terminals or flasher tools.
- **Low FPS or growing buffer** — the host may not be reading fast enough; ensure nothing else is consuming CPU on the serial reader process.
- **Frames torn apart or video sync issues** — OS serial buffer overflowing; data arrives faster than we can read. Try closing other applications to reduce system load.