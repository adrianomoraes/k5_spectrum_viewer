# K5Viewer: Advanced Viewer & Recorder for Quansheng K5-Family Radios (permanent BETA)

`K5Viewer` is a powerful Python-based utility, forked from the `k5viewer` found in the [armel/uv-k5-firmware-custom](https://github.com/armel/uv-k5-firmware-custom) repository ([direct link](https://github.com/armel/uv-k5-firmware-custom/tree/main/k5viewer)). It mirrors the radio's 128x64 screen in real-time, provides a detailed live waterfall display, and automatically records full spectrum sessions to a local database.

[![A link to the K5Viewer demo video](assets/screenshot.png)](https://youtu.be/BLnvxmcPg08)

The main upgrade in this K5 viewer fork is a custom, much more robust Anchor-Based OCR Engine along with the waterfall view and session recording capabilities. It still uses the original method of pulling screen frames from the COM port with a programming cable, but now with added features.


> **Important Note:** K5Viewer is **NOT a remote control tool**. It is a passive *screenreader* and data logger. It cannot transmit, change frequencies, or modify settings on your radio. Its sole purpose is to read, display, and record the data from the radio's screen.

**Important:** Due to the incremental way the firmware sends frames through the COM port, you must start the program while the radio is at its initial screen.
**Troubleshooting:** If the program starts but no radio screen appears, try going back to the initial screen (exiting the spectrum analyzer view) and turning the radio off and on again.

## ✨ Key Features

* **Live Radio Screen Mirroring:** View your K5's screen in a scalable Pygame window.
* **Advanced Spectrum Waterfall:** A smooth, live waterfall display with multiple color gradients, zoom, and calibration.
* **Automatic Session Recording:** Automatically detects when the spectrum analyzer is active and records all data (spectrum, frequencies, modulation) to an `sqlite3` database.
* **Powerful Replay System:** Browse, search, filter, and replay all past recordings.
* **Variable-Speed Playback:** Replay sessions at 1x, 2x, 4x, 8x, or 16x speed.
* **Energy-Map Seek Bar:** Instantly see high-energy moments in a recording's timeline and click to seek.
* **Points of Interest (POI):** Click on the waterfall (live or replay) to save a frequency, timestamp, and description for future reference.

## 🛠️ Requirements

* Python 3.8+
* A Quansheng UV-K5 (or compatible) radio and its USB programming cable.
* A few Python libraries.

To install the libraries, create a file named `requirements.txt` in the project folder with this content:

```
pyserial
pygame
```

Then, run this command in your terminal:

```bash
pip install -r requirements.txt
```

## 🚀 How to Run

1.  Connect your radio to your computer using its programming cable.
2.  Find your radio's COM port. You can use the built-in utility for this:
    ```bash
    # On Windows
    python k5_spectrum_13.py --list-ports

    # On macOS / Linux
    python3 k5_spectrum_13.py --list-ports
    ```
3.  Run the main application:
    ```bash
    python k5_spectrum_13.py
    ```
4.  The application will open. Click the correct COM port from the connection menu.
5.  Turn on your radio's spectrum analyzer to begin viewing and auto-recording. The database file `viewer_recordings.db` will be created automatically.

## ⌨️ Hotkeys & Controls

### General
* **Q:** Quit the application.
* **Up/Down Arrows:** Increase/Decrease the pixel size of the radio screen.
* **G / O / B / W:** Change radio screen color (Grey, Orange, Blue, White).

### Waterfall (Live & Replay)
* **Z / X:** Zoom in/out of the frequency range (centered on the mouse).
* **Left/Right Arrows:** Adjust waterfall pixel offset for calibration.
* **Click:** Click on the waterfall to view info and save a Point of Interest (POI).
* **Ctrl + Z / Ctrl + X:** Decrease/Increase the waterfall's time history (fewer/more lines).

### Replay Mode
* **Spacebar:** Toggle Play/Pause.
* **Shift + Left/Right:** Seek frame by frame.
* **Ctrl + Shift + Left/Right:** Seek by 100 frames.
* **Click Seek Bar:** Jump to any part of the recording.

## 🤝 Contributing

Pull Requests are welcome! If you have improvements, especially for OCR robustness, new radio features, or bug fixes, feel free to open an issue or PR.

## 📜 License

This project is licensed under the **[PLEASE ADD LICENSE - e.g., MIT, GPLv3]** License.

*(**Note:** Please check the license of the original project you forked from and ensure it is compatible. The MIT License is a common and permissive choice if you are unsure.)*
