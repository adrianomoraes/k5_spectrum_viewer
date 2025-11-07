#!/usr/bin/env python3

import typing
import os
import sys
import time
import datetime
import argparse
from collections import deque
import threading
import queue
import json
import sqlite3

os.environ["PYGAME_HIDE_SUPPORT_PROMPT"] = "hide"

import pygame
import serial
from serial.tools import list_ports

# Version
VERSION = '1.7.5' # Updated version for recording rate fixes

# --- Recording Configuration ---
# Set RECORDING_INTERVAL:
# 0     = Record every frame received from the radio (respects radio's latency)
# > 0 = Record approx. one frame every RECORDING_INTERVAL seconds (e.g., 1.0 = ~1fps, 0.5 = ~2fps)
RECORDING_INTERVAL = 1 # Default: Record every frame

# --- Database Setup ---
DB_FILE = 'viewer_recordings.db'

def init_db():
    """Initializes the database and creates/migrates tables."""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS sessions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            identifier TEXT NOT NULL UNIQUE,
            start_time DATETIME NOT NULL
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS recordings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            session_id INTEGER NOT NULL,
            timestamp REAL NOT NULL,
            spectrum_data TEXT,
            center_freq TEXT,
            start_freq TEXT,
            end_freq TEXT,
            impedance_low TEXT,
            impedance_high TEXT,
            bars TEXT,
            step TEXT,
            modulation TEXT,
            bandwidth TEXT,
            FOREIGN KEY (session_id) REFERENCES sessions (id)
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS points_of_interest (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            recording_id INTEGER NOT NULL,
            frequency_mhz REAL NOT NULL,
            description TEXT,
            absolute_timestamp DATETIME NOT NULL,
            FOREIGN KEY (recording_id) REFERENCES recordings (id)
        )
    """)
    
    # --- START OF NEW FEATURE ---
    # Database Migration: Add the 'spectrum_sum' column if it doesn't exist
    try:
        cursor.execute("ALTER TABLE recordings ADD COLUMN spectrum_sum INTEGER")
        print("[DB Migration] Added 'spectrum_sum' column to recordings.")
        
        # Now, populate it for all existing data (one-time cost)
        print("[DB Migration] Populating 'spectrum_sum' for old data... This may take a moment.")
        cursor.execute("SELECT id, spectrum_data FROM recordings WHERE spectrum_sum IS NULL")
        rows_to_update = cursor.fetchall()
        
        updates = []
        for row_id, spectrum_json in rows_to_update:
            try:
                spectrum_list = json.loads(spectrum_json)
                spectrum_sum = sum(spectrum_list)
                updates.append((spectrum_sum, row_id))
            except (json.JSONDecodeError, TypeError):
                updates.append((0, row_id)) # Set sum to 0 if data is bad

        if updates:
            cursor.executemany("UPDATE recordings SET spectrum_sum = ? WHERE id = ?", updates)
            print(f"[DB Migration] Updated {len(updates)} old records.")
            
    except sqlite3.OperationalError as e:
        if "duplicate column name" in str(e):
            pass # Column already exists, which is fine
        else:
            raise e # Re-raise other errors
    # --- END OF NEW FEATURE ---
            
    conn.commit()
    conn.close()

def get_sessions(search_term=None, date_term=None, energy_min=None):
    """
    Returns a list of tuples from the database:
    (identifier, start_time, frame_count, duration_seconds, poi_count, poi_descriptions)
    
    Filters by text search, start date, and minimum energy sum.
    """
    try:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()

        base_query = """
            SELECT
                s.identifier,
                s.start_time,
                COUNT(DISTINCT r.id) as frame_count,
                MAX(r.timestamp) as duration,
                COUNT(DISTINCT poi.id) as poi_count,
                GROUP_CONCAT(DISTINCT poi.description) as poi_descriptions,
                MAX(r.spectrum_sum) as max_session_energy
            FROM sessions s
            LEFT JOIN recordings r ON s.id = r.session_id
            LEFT JOIN points_of_interest poi ON r.id = poi.recording_id
        """
        
        where_clauses = []
        having_clauses = []
        params = []
        
        if search_term:
            like_term = f"%{search_term}%"
            # This complex WHERE/subquery filters on session fields OR on 
            # sessions that have matching POI descriptions.
            where_clauses.append("""
                (s.identifier LIKE ? OR s.start_time LIKE ? OR s.id IN (
                    SELECT s_inner.id
                    FROM sessions s_inner
                    JOIN recordings r_inner ON s_inner.id = r_inner.session_id
                    JOIN points_of_interest poi_inner ON r_inner.id = poi_inner.recording_id
                    WHERE poi_inner.description LIKE ?
                ))
            """)
            params.extend([like_term, like_term, like_term])

        # --- START OF NEW FEATURE ---
        if date_term:
            # Assumes YYYY-MM-DD format. SQL will handle it.
            where_clauses.append("s.start_time >= ?")
            params.append(date_term)
            
        if energy_min is not None and energy_min > 0:
            # Use HAVING to filter groups (sessions) based on their max energy
            having_clauses.append("max_session_energy >= ?")
            params.append(energy_min)
        # --- END OF NEW FEATURE ---

        if where_clauses:
            base_query += " WHERE " + " AND ".join(where_clauses)

        base_query += " GROUP BY s.id"

        if having_clauses:
            base_query += " HAVING " + " AND ".join(having_clauses)
            
        base_query += " ORDER BY s.start_time DESC"

        cursor.execute(base_query, params)
        sessions = cursor.fetchall()
        conn.close()
        
        processed_sessions = []
        for s in sessions:
            # Unpack all 7 items (we don't need max_session_energy in the list)
            identifier, start, frame_count, duration, poi_count, poi_descs, _ = s
            processed_sessions.append((
                identifier,
                start,
                frame_count,
                duration if duration is not None else 0,
                poi_count,
                poi_descs if poi_descs is not None else ""
            ))
        return processed_sessions
        
    except sqlite3.Error as e:
        print(f"[DB Error] Failed to get sessions: {e}")
        return []

def get_session_frame_count(session_identifier):
    """Returns the total number of frames for a given session identifier."""
    try:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(recordings.id) FROM recordings JOIN sessions ON recordings.session_id = sessions.id WHERE sessions.identifier = ?", (session_identifier,))
        count = cursor.fetchone()[0]
        conn.close()
        return count
    except (sqlite3.Error, IndexError):
        return 0

def get_session_data_chunk(session_identifier, offset, limit):
    """Returns a chunk of frames for a given session identifier."""
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row # Return rows as dictionaries
    cursor = conn.cursor()
    cursor.execute("PRAGMA journal_mode=WAL;")
    cursor.execute("""
        SELECT r.*
        FROM recordings as r
        JOIN sessions as s ON r.session_id = s.id
        WHERE s.identifier = ?
        ORDER BY r.timestamp ASC
        LIMIT ? OFFSET ?
    """, (session_identifier, limit, offset))
    data = [dict(row) for row in cursor.fetchall()]
    conn.close()
    return data
# ---

# Serial configuration
DEFAULT_PORT = 'COM5'
        
BAUDRATE = 38400
#BAUDRATE = 9600
TIMEOUT = 0.5

# Screen configuration
WIDTH, HEIGHT = 128, 64
FRAME_SIZE = 1024

# Protocol
HEADER = b'\xAA\x55'
TYPE_SCREENSHOT = b'\x01'
TYPE_DIFF = b'\x02'

# Target zones
DETECT_X_START, DETECT_X_END = 0, 128
DETECT_Y_START, DETECT_Y_END = 48, 48
SPECTRUM_X_START, SPECTRUM_X_END = 0, 127
SPECTRUM_Y_START, SPECTRUM_Y_END = 20, 48

# OCR Definitions
SMALL_FONT_MAP = {
    '0': (0x1C, 0x11, 0x0F), '1': (0x02, 0x1F, 0x00),
    '2': (0x19, 0x15, 0x12), '3': (0x11, 0x1D, 0x0A),
    '4': (0x07, 0x04, 0x1F), '5': (0x17, 0x15, 0x09),
    '6': (0x1E, 0x15, 0x1D), '7': (0x19, 0x05, 0x03),
    '8': (0x1F, 0x15, 0x1F), '9': (0x17, 0x15, 0x0F),
    '.': (0x00, 0x10, 0x00),
    '-': (0x04, 0x04, 0x04),
    '/': (0x18, 0x04, 0x03),
    'F': (0x1F, 0x05, 0x05),
    'A': (0x1E, 0x05, 0x1E),
    'M': (0x1F, 0x0C, 0x1F),
    'U': (0x0F, 0x10, 0x1F),
    'S': (0x12, 0x15, 0x09),
    'B': (0x1F, 0x15, 0x0A),
}
SMALL_FONT_DIMS = {'width': 3, 'height': 5, 'spacing': 1}

LARGE_FONT_MAP = {
    '0': (0x3E, 0x41, 0x41, 0x41, 0x41, 0x3E),
    '1': (0x00, 0x40, 0x64, 0x7F, 0x40, 0x40),
    '2': (0x62, 0x51, 0x51, 0x49, 0x49, 0x46),
    '3': (0x22, 0x41, 0x49, 0x49, 0x49, 0x36),
    '4': (0x18, 0x14, 0x0B, 0x39, 0x7F, 0x02),
    '5': (0x27, 0x45, 0x65, 0x65, 0x65, 0x39),
    '6': (0x3E, 0x4B, 0x49, 0x49, 0x49, 0x32),
    '7': (0x11, 0x11, 0x79, 0x05, 0x03, 0x01),
    '8': (0x36, 0x49, 0x49, 0x49, 0x49, 0x36),
    '9': (0x46, 0x49, 0x49, 0x49, 0x29, 0x1E),
    '.': (0x00, 0x00, 0x60, 0x60, 0x00, 0x00)
}
LARGE_FONT_DIMS = {'width': 6, 'height': 7, 'spacing': 1}

CENTER_FREQ_RECT = pygame.Rect(35, 8, 70, 7)
START_FREQ_RECT = pygame.Rect(0, 57, 40, 5)
END_FREQ_RECT = pygame.Rect(93, 57, 40, 5)
SPECTRUM_AREA_RECT = pygame.Rect(SPECTRUM_X_START, SPECTRUM_Y_START, SPECTRUM_X_END - SPECTRUM_X_START, SPECTRUM_Y_END - SPECTRUM_Y_START)

ZONE_A_RECT = pygame.Rect(0, 1, 35, 5)
ZONE_B_RECT = pygame.Rect(0, 9, 15, 5)
ZONE_C_RECT = pygame.Rect(0, 15, 30, 5)
ZONE_D_RECT = pygame.Rect(115, 9, 12, 5)
ZONE_E_RECT = pygame.Rect(97, 15, 30, 5)

COLOR_SETS = { "g": ("Grey", (0,0,0), (202,202,202)), "o": ("Orange", (0,0,0), (255,193,37)), "b": ("Blue", (0,0,0), (28,134,228)), "w": ("White", (0,0,0), (255,255,255)), }
DEFAULT_COLOR = "g"
PRESET_GRADIENTS = {
    'Heatmap': [(0.0, (0,0,100)), (0.5, (255,0,0)), (1.0, (255,255,255))],
    'Grayscale': [(0.0, (0,0,0)), (1.0, (255,255,255))],
    'Rainbow': [(0.0,(0,0,255)), (0.25,(0,255,255)), (0.5,(0,255,0)), (0.75,(255,255,0)), (1.0,(255,0,0))]
}

class Slider:
    def __init__(self, x, y, w, h, min_val, max_val, initial_val):
        self.rect = pygame.Rect(x, y, w, h); self.min_val, self.max_val = min_val, max_val; self.val = initial_val
        self.dragging = False; self.handle_rect = pygame.Rect(0, 0, 10, h + 10); self.update_handle_pos()
    def update_handle_pos(self):
        if self.max_val == self.min_val: ratio = 0
        else: ratio = (self.val - self.min_val) / (self.max_val - self.min_val)
        self.handle_rect.centerx = self.rect.left + ratio * self.rect.width
        self.handle_rect.centery = self.rect.centery
    def handle_event(self, event):
        if event.type == pygame.MOUSEBUTTONDOWN and (self.handle_rect.collidepoint(event.pos) or self.rect.collidepoint(event.pos)):
            if event.button == 1:
                self.dragging = True
                self.handle_rect.centerx = max(self.rect.left, min(event.pos[0], self.rect.right))
                self.update_value_from_pos()
        elif event.type == pygame.MOUSEBUTTONUP:
            if event.button == 1:
                self.dragging = False
        elif event.type == pygame.MOUSEMOTION and self.dragging:
            self.handle_rect.centerx = max(self.rect.left, min(event.pos[0], self.rect.right))
            self.update_value_from_pos()
    def update_value_from_pos(self):
        if self.rect.width == 0: ratio = 0
        else: ratio = (self.handle_rect.centerx - self.rect.left) / self.rect.width
        self.val = self.min_val + ratio * (self.max_val - self.min_val)
    def draw(self, screen, label, font, is_percent=True, actual_value=None, progress_ratio=None, energy_map=None, max_energy=None, color_config=None):
        # Draw the background of the slider
        pygame.draw.rect(screen, (80, 80, 80), self.rect)
        
        # --- START OF NEW FEATURE ---
        # Draw the energy bar *under* the progress/handle
        if energy_map and max_energy is not None and color_config:
            # Call our new helper function
            draw_energy_bar(screen, self.rect, energy_map, max_energy, color_config)
        # --- END OF NEW FEATURE ---
        
        # Draw the blue progress bar
        if progress_ratio is not None:
            progress_width = self.rect.width * progress_ratio
            progress_rect = pygame.Rect(self.rect.left, self.rect.top, progress_width, self.rect.height)
            # Draw with some transparency so the energy bar shows through
            progress_surface = pygame.Surface((progress_rect.width, progress_rect.height), pygame.SRCALPHA)
            progress_surface.fill((28, 134, 228, 100)) # 100 alpha = ~40% opacity
            screen.blit(progress_surface, progress_rect.topleft)

        # Draw the handle
        pygame.draw.rect(screen, (200, 200, 200), self.handle_rect)

        if is_percent:
            display_text = f"{label}: {int(self.val * 100)}%"
        else:
            if actual_value is None:
                actual_value = int(self.val)
            display_text = f"{label}{actual_value}"
        label_surf = font.render(display_text, True, (200, 200, 200))
        screen.blit(label_surf, (self.rect.left, self.rect.y - 15))

def draw_ocr_preview(screen: pygame.Surface, fb: bytearray, area: pygame.Rect, draw_pos: tuple, label: str, font: pygame.font.Font):
    label_surf = font.render(label, True, (200, 200, 200))
    screen.blit(label_surf, (draw_pos[0], draw_pos[1] - 20))
    
    preview_scale = 10
    preview_rect = pygame.Rect(draw_pos[0], draw_pos[1], area.width * preview_scale, area.height * preview_scale)
    pygame.draw.rect(screen, (40, 40, 40), preview_rect)
    pygame.draw.rect(screen, (255, 80, 80), preview_rect, 1)

    for y in range(area.height):
        for x in range(area.width):
            px, py = area.left + x, area.top + y
            if px >= WIDTH or py >= HEIGHT: continue
            
            bit_idx = py * WIDTH + px
            byte_idx, bit_pos = divmod(bit_idx, 8)
            
            if byte_idx < len(fb) and (fb[byte_idx] >> bit_pos) & 0x01:
                pygame.draw.rect(screen, (200, 200, 200), (draw_pos[0] + x * preview_scale, draw_pos[1] + y * preview_scale, preview_scale, preview_scale))

def draw_area_highlight_preview(screen: pygame.Surface, area_to_highlight: pygame.Rect, draw_pos: tuple, label: str, font: pygame.font.Font):
    label_surf = font.render(label, True, (200, 200, 200))
    screen.blit(label_surf, (draw_pos[0], draw_pos[1] - 20))

    preview_scale = 2
    preview_rect = pygame.Rect(draw_pos[0], draw_pos[1], WIDTH * preview_scale, HEIGHT * preview_scale)
    pygame.draw.rect(screen, (40, 40, 40), preview_rect)
    
    highlight_rect = pygame.Rect(
        draw_pos[0] + area_to_highlight.left * preview_scale,
        draw_pos[1] + area_to_highlight.top * preview_scale,
        area_to_highlight.width * preview_scale,
        area_to_highlight.height * preview_scale
    )
    pygame.draw.rect(screen, (80, 255, 80), highlight_rect, 1)

def lerp(a, b, t): return a * (1 - t) + b * t

def get_gradient_color(value: int, max_value: int, config: dict) -> tuple[int, int, int]:
    if max_value == 0: return (0, 0, 0)
    ratio = min(value / max_value, 1.0)
    range_start, range_end = config['range_start'], config['range_end']
    if range_start > range_end: range_start, range_end = range_end, range_start
    remapped_ratio = lerp(range_start, range_end, ratio)
    preset_name = list(PRESET_GRADIENTS.keys())[config['current_preset_index']]
    gradient = PRESET_GRADIENTS[preset_name]
    for i in range(len(gradient) - 1):
        p1_ratio, p1_color = gradient[i]; p2_ratio, p2_color = gradient[i+1]
        if p1_ratio <= remapped_ratio <= p2_ratio:
            segment_ratio = (remapped_ratio - p1_ratio) / (p2_ratio - p1_ratio) if (p2_ratio - p1_ratio) != 0 else 0
            return (int(lerp(p1_color[0], p2_color[0], segment_ratio)), int(lerp(p1_color[1], p2_color[1], segment_ratio)), int(lerp(p1_color[2], p2_color[2], segment_ratio)))
    return gradient[-1][1]

def get_spectrum_data(fb: bytearray) -> list[int]:
    heights = []
    for x in range(SPECTRUM_X_START, SPECTRUM_X_END + 1):
        column_height = 0
        for y in range(SPECTRUM_Y_START, SPECTRUM_Y_END + 1):
            bit_idx = y * WIDTH + x; byte_idx = bit_idx // 8; bit_pos = bit_idx % 8
            if byte_idx < len(fb) and (fb[byte_idx] >> bit_pos) & 0x01:
                column_height += 1
        
        if x % 2 == 0 and column_height > 0:
            column_height -= 1
            
        heights.append(column_height)
    return heights

def draw_waterfall(screen: pygame.Surface, data: deque, area: pygame.Rect, color_config: dict, 
                   full_freq_range: tuple, view_freq_range: tuple, calibration_pixel_offset: int):
    if not data or not full_freq_range or not view_freq_range: return
    
    full_start_f, full_end_f = full_freq_range
    view_start_f, view_end_f = view_freq_range
    full_span = full_end_f - full_start_f

    if full_span <= 0: return

    start_ratio = (view_start_f - full_start_f) / full_span
    end_ratio = (view_end_f - full_start_f) / full_span

    line_height = area.height / data.maxlen 
    max_bar_height = SPECTRUM_Y_END - SPECTRUM_Y_START
    
    screen.set_clip(area)
    
    for y_idx, line_data in enumerate(data):
        num_total_points = len(line_data)
        if num_total_points == 0: continue

        start_idx = max(0, int(start_ratio * num_total_points))
        end_idx = min(num_total_points, int(end_ratio * num_total_points))
        
        visible_data = line_data[start_idx:end_idx]
        num_visible_points = len(visible_data)
        
        if num_visible_points == 0: continue
        
        line_y = area.top + (y_idx * line_height)
        pixel_width = area.width / num_visible_points

        for x_idx, value in enumerate(visible_data):
            color = get_gradient_color(value, max_bar_height, color_config)
            rect_x = area.left + calibration_pixel_offset + (x_idx * pixel_width)
            pygame.draw.rect(screen, color, (rect_x, line_y, pixel_width + 1, line_height + 1))
            
    screen.set_clip(None)
    
def draw_energy_bar(screen: pygame.Surface, rect: pygame.Rect, energy_map: list, max_energy: int, color_config: dict):
    """Draws a colored bar representing the energy map of the session."""
    if not energy_map or max_energy == 0 or rect.width == 0:
        return # Nothing to draw
        
    num_frames = len(energy_map)
    frames_per_pixel = num_frames / rect.width
    
    # We can optimize by drawing one line per pixel
    for x_pixel in range(rect.width):
        # Find the corresponding frame index
        # We sample the frame at the *start* of this pixel's "zone"
        map_index = int(x_pixel * frames_per_pixel)
        
        # Ensure index is valid
        if 0 <= map_index < num_frames:
            energy_value = energy_map[map_index]
            
            # Get color based on energy (using the same logic as the waterfall)
            color = get_gradient_color(energy_value, max_energy, color_config)
            
            # Draw a 1-pixel wide vertical line
            line_x = rect.left + x_pixel
            pygame.draw.line(screen, color, (line_x, rect.top), (line_x, rect.bottom))


def draw_frequency_scale(screen: pygame.Surface, start_f: float, center_f_str: str, end_f: float, area: pygame.Rect, tiny_font: pygame.font.Font, small_font: pygame.font.Font, large_font: pygame.font.Font, bold_font: pygame.font.Font, num_divisions: int, waterfall_area_bottom: int):
    if num_divisions < 1 or (end_f - start_f) == 0: return
    span = end_f - start_f
    tick_color = (100, 100, 100)
    label_color = (180, 180, 180)
    
    center_text_surf = large_font.render(f"{center_f_str} MHz", True, (220, 220, 220))
    center_text_rect = center_text_surf.get_rect(centerx=area.centerx, top=area.top)
    screen.blit(center_text_surf, center_text_rect)
    
    if num_divisions == 29:
        for i in range(29):
            ratio = i / 28.0
            x_pos = area.left + (ratio * area.width)
            current_freq = start_f + (ratio * span)
            
            is_main_tick = (i % 2 == 0)
            
            if is_main_tick:
                is_edge = (i == 0 or i == 28)
                font_to_use = bold_font if is_edge else small_font
                y_offset = 40 if is_edge else 25
                label_surf = font_to_use.render(f"{current_freq:.3f}", True, label_color)
            else: 
                font_to_use = tiny_font
                y_offset = 12
                label_surf = font_to_use.render(f"{current_freq:.3f}", True, label_color)
                
            label_rect = label_surf.get_rect(centerx=x_pos, bottom=area.bottom - y_offset)
            if label_rect.left < area.left: label_rect.left = area.left
            if label_rect.right > area.right: label_rect.right = area.right
            screen.blit(label_surf, label_rect)
            pygame.draw.line(screen, tick_color, (x_pos, label_rect.bottom + 2), (x_pos, waterfall_area_bottom))
        return

    if num_divisions > 1:
        labeled_ratios = [i / (num_divisions - 1) for i in range(num_divisions)]
        for i, ratio in enumerate(labeled_ratios):
            x_pos = area.left + (ratio * area.width)
            current_freq = start_f + (ratio * span)
            
            is_edge = (i == 0 or i == len(labeled_ratios) - 1)
            font_to_use = bold_font if is_edge else small_font
            y_offset = 40 if is_edge else 25 
            
            label_surf = font_to_use.render(f"{current_freq:.3f}", True, label_color)
            label_rect = label_surf.get_rect(centerx=x_pos, bottom=area.bottom - y_offset)
            
            if label_rect.left < area.left: label_rect.left = area.left
            if label_rect.right > area.right: label_rect.right = area.right
            
            screen.blit(label_surf, label_rect)
            pygame.draw.line(screen, tick_color, (x_pos, label_rect.bottom + 2), (x_pos, waterfall_area_bottom))

        for i in range(num_divisions - 1):
            ratio_mid = (labeled_ratios[i] + labeled_ratios[i+1]) / 2
            x_pos_mid = area.left + (ratio_mid * area.width)
            pygame.draw.line(screen, tick_color, (x_pos_mid, area.bottom - 15), (x_pos_mid, waterfall_area_bottom))

def draw_time_scale(screen: pygame.Surface, area: pygame.Rect, font: pygame.font.Font, max_seconds: int):
    tick_color = (100, 100, 100)
    label_color = (180, 180, 180)
    
    for seconds in range(0, max_seconds + 1, 10):
        if max_seconds == 0: ratio = 0
        else: ratio = seconds / max_seconds
        y_pos = area.top + (ratio * area.height)
        
        label_surf = font.render(str(seconds), True, label_color)
        label_rect = label_surf.get_rect(right=area.right - 5, centery=y_pos)
        screen.blit(label_surf, label_rect)
        
        pygame.draw.line(screen, tick_color, (area.right - 2, y_pos), (area.right + 2, y_pos))

def is_spectrum_analyzer_active(fb: bytearray) -> bool:
    for y in range(DETECT_Y_START, DETECT_Y_END + 1):
        for x in range(DETECT_X_START, DETECT_X_END): # Note: range is exclusive of the end value
            if x >= WIDTH or y >= HEIGHT: continue
            bit_idx = y * WIDTH + x; byte_idx = bit_idx // 8; bit_pos = bit_idx % 8
            if not ((fb[byte_idx] >> bit_pos) & 0x01): return False
    return True

def ocr_area(fb: bytearray, area: pygame.Rect, font_map: dict, font_dims: dict) -> str:
    recognized_text = ""
    char_w, char_h, char_s = font_dims['width'], font_dims['height'], font_dims['spacing']
    
    x_pos = area.left
    while x_pos <= area.right - char_w:
        char_pattern_list = []
        is_blank = True
        for x_offset in range(char_w):
            col_val = 0
            for y_offset in range(char_h):
                px, py = x_pos + x_offset, area.top + y_offset
                if not (0 <= px < WIDTH and 0 <= py < HEIGHT): continue
                
                bit_idx = py * WIDTH + px
                byte_idx, bit_pos = divmod(bit_idx, 8)
                
                if byte_idx < len(fb) and (fb[byte_idx] >> bit_pos) & 0x01:
                    col_val |= (1 << y_offset)
                    is_blank = False
            char_pattern_list.append(col_val)
        
        if is_blank:
            recognized_text += ' '
            x_pos += char_w + char_s
            continue
            
        char_pattern = tuple(char_pattern_list)
        
        best_match_char = '?'
        min_mismatched_pixels = float('inf')

        for char, font_pattern in font_map.items():
            if len(font_pattern) != len(char_pattern): continue
            
            mismatched_pixels = 0
            for i in range(len(char_pattern)):
                mismatched_pixels += bin(char_pattern[i] ^ font_pattern[i]).count('1')
            
            if mismatched_pixels < min_mismatched_pixels:
                min_mismatched_pixels = mismatched_pixels
                best_match_char = char

        max_allowed_mismatch = (char_w * char_h) * 0.35 
        if min_mismatched_pixels <= max_allowed_mismatch:
            recognized_text += best_match_char
        else:
            recognized_text += '?'
            
        x_pos += char_w + char_s

    return recognized_text.strip()

def ocr_area_rtl(fb: bytearray, area: pygame.Rect, font_map: dict, font_dims: dict) -> str:
    recognized_chars = []
    char_w, char_h, char_s = font_dims['width'], font_dims['height'], font_dims['spacing']
    
    x_pos = area.right - char_w
    while x_pos >= area.left:
        char_pattern_list = []
        is_blank = True
        for x_offset in range(char_w):
            col_val = 0
            for y_offset in range(char_h):
                px, py = x_pos + x_offset, area.top + y_offset
                if not (0 <= px < WIDTH and 0 <= py < HEIGHT): continue
                bit_idx = py * WIDTH + px
                byte_idx, bit_pos = divmod(bit_idx, 8)
                if byte_idx < len(fb) and (fb[byte_idx] >> bit_pos) & 0x01:
                    col_val |= (1 << y_offset)
                    is_blank = False
            char_pattern_list.append(col_val)
        
        if is_blank:
            recognized_chars.append(' ')
            x_pos -= (char_w + char_s)
            continue
            
        char_pattern = tuple(char_pattern_list)
        best_match_char = '?'
        min_mismatched_pixels = float('inf')

        for char, font_pattern in font_map.items():
            if len(font_pattern) != len(char_pattern): continue
            mismatched_pixels = 0
            for i in range(len(char_pattern)):
                mismatched_pixels += bin(char_pattern[i] ^ font_pattern[i]).count('1')
            if mismatched_pixels < min_mismatched_pixels:
                min_mismatched_pixels = mismatched_pixels
                best_match_char = char

        max_allowed_mismatch = (char_w * char_h) * 0.35 
        if min_mismatched_pixels <= max_allowed_mismatch:
            recognized_chars.append(best_match_char)
        else:
            recognized_chars.append('?')
            
        x_pos -= (char_w + char_s)

    return "".join(reversed(recognized_chars)).strip()

def _read_char_pattern_at(fb: bytearray, x_pos: int, y_pos: int, char_w: int, char_h: int) -> typing.Union[tuple, None]:
    """Reads a single character pattern from a specific (x, y) coordinate."""
    char_pattern_list = []
    is_blank = True
    for x_offset in range(char_w):
        col_val = 0
        for y_offset in range(char_h):
            px, py = x_pos + x_offset, y_pos + y_offset
            if not (0 <= px < WIDTH and 0 <= py < HEIGHT): continue
            
            bit_idx = py * WIDTH + px
            byte_idx, bit_pos = divmod(bit_idx, 8)
            
            if byte_idx < len(fb) and (fb[byte_idx] >> bit_pos) & 0x01:
                col_val |= (1 << y_offset)
                is_blank = False
        char_pattern_list.append(col_val)
    
    if is_blank:
        return None # Use None to represent a blank space
    return tuple(char_pattern_list)

def _match_pattern_to_font(char_pattern: typing.Union[tuple, None], font_map: dict, char_w: int, char_h: int) -> str:
    """Finds the best-matching character for a given pattern tuple."""
    if char_pattern is None:
        return ' ' # It's a blank
        
    best_match_char = '?'
    min_mismatched_pixels = float('inf')
    # Use the same mismatch tolerance as the original function
    max_allowed_mismatch = (char_w * char_h) * 0.35 

    for char, font_pattern in font_map.items():
        if len(font_pattern) != len(char_pattern): continue
        
        mismatched_pixels = 0
        for i in range(len(char_pattern)):
            mismatched_pixels += bin(char_pattern[i] ^ font_pattern[i]).count('1')
        
        if mismatched_pixels < min_mismatched_pixels:
            min_mismatched_pixels = mismatched_pixels
            best_match_char = char
    
    if min_mismatched_pixels <= max_allowed_mismatch:
        return best_match_char
    else:
        return '?'

def ocr_area_anchor_scan(fb: bytearray, area: pygame.Rect, font_map: dict, font_dims: dict, anchor_char='.') -> str:
    """
    Performs OCR by finding a reliable 'anchor' character (like '.') 
    and then scanning left and right from it.
    """
    char_w, char_h, char_s = font_dims['width'], font_dims['height'], font_dims['spacing']
    step_size = char_w + char_s
    anchor_pattern = font_map.get(anchor_char)

    if not anchor_pattern:
        # If we can't find the anchor in the font map, we can't use this method.
        return ocr_area_centered(fb, area, font_map, font_dims)

    # 1. Find the anchor (the dot)
    best_anchor_x = -1
    min_anchor_mismatch = float('inf')
    max_allowed_mismatch = (char_w * char_h) * 0.35 # Mismatch tolerance for the anchor

    # Slide pixel-by-pixel to find the best match for the anchor
    for x in range(area.left, area.right - char_w + 1):
        scanned_pattern = _read_char_pattern_at(fb, x, area.top, char_w, char_h)
        
        if scanned_pattern is None: # Skip blank areas
            continue

        # Calculate mismatch vs. the anchor pattern
        mismatched_pixels = 0
        for i in range(len(scanned_pattern)):
            mismatched_pixels += bin(scanned_pattern[i] ^ anchor_pattern[i]).count('1')
        
        if mismatched_pixels < min_anchor_mismatch:
            min_anchor_mismatch = mismatched_pixels
            best_anchor_x = x

    # 2. Check if we found a good-enough anchor
    if best_anchor_x == -1 or min_anchor_mismatch > max_allowed_mismatch:
        # We didn't find the dot. The string might not have one (e.g., "SCANNING").
        # Fallback to the centered scan.
        return ocr_area_centered(fb, area, font_map, font_dims)
    
    # 3. We found the dot! Store it and scan left/right.
    # We use a dictionary to store chars by their x_pos to keep them in order.
    found_chars = {best_anchor_x: anchor_char}
    
    # Scan left
    current_x = best_anchor_x - step_size
    while current_x >= area.left:
        pattern = _read_char_pattern_at(fb, current_x, area.top, char_w, char_h)
        char = _match_pattern_to_font(pattern, font_map, char_w, char_h)
        
        if char == ' ': # Stop if we hit a leading blank
             break
        found_chars[current_x] = char
        current_x -= step_size

    # Scan right
    current_x = best_anchor_x + step_size
    while current_x <= area.right - char_w:
        pattern = _read_char_pattern_at(fb, current_x, area.top, char_w, char_h)
        char = _match_pattern_to_font(pattern, font_map, char_w, char_h)

        if char == ' ': # Stop if we hit a trailing blank
            break
        found_chars[current_x] = char
        current_x += step_size

    # 4. Assemble the final string
    # Sort the found characters by their x-coordinate to get the correct order
    sorted_x_positions = sorted(found_chars.keys())
    final_text = "".join(found_chars[x] for x in sorted_x_positions)
    
    return final_text.strip()

def ocr_area_centered(fb: bytearray, area: pygame.Rect, font_map: dict, font_dims: dict) -> str:
    """
    Performs OCR on an area where the text may be centered.
    It tries every possible 1-pixel grid offset and returns the best-scoring match.
    """
    char_w, char_h, char_s = font_dims['width'], font_dims['height'], font_dims['spacing']
    step_size = char_w + char_s
    
    best_text = ""
    best_score = -1 # Use -1 to ensure even a blank line (score 0) is a valid result

    # Try every possible offset (0 through 6)
    for offset in range(step_size):
        recognized_text = ""
        current_score = 0
        
        # Start scan at area.left + our current offset
        x_pos = area.left + offset
        
        while x_pos <= area.right - char_w:
            # --- This logic is identical to ocr_area ---
            char_pattern_list = []
            is_blank = True
            for x_offset in range(char_w):
                col_val = 0
                for y_offset in range(char_h):
                    px, py = x_pos + x_offset, area.top + y_offset
                    if not (0 <= px < WIDTH and 0 <= py < HEIGHT): continue
                    
                    bit_idx = py * WIDTH + px
                    byte_idx, bit_pos = divmod(bit_idx, 8)
                    
                    if byte_idx < len(fb) and (fb[byte_idx] >> bit_pos) & 0x01:
                        col_val |= (1 << y_offset)
                        is_blank = False
                char_pattern_list.append(col_val)
            
            if is_blank:
                recognized_text += ' '
                x_pos += step_size
                continue
                
            char_pattern = tuple(char_pattern_list)
            
            best_match_char = '?'
            min_mismatched_pixels = float('inf')

            for char, font_pattern in font_map.items():
                if len(font_pattern) != len(char_pattern): continue
                
                mismatched_pixels = 0
                for i in range(len(char_pattern)):
                    mismatched_pixels += bin(char_pattern[i] ^ font_pattern[i]).count('1')
                
                if mismatched_pixels < min_mismatched_pixels:
                    min_mismatched_pixels = mismatched_pixels
                    best_match_char = char

            max_allowed_mismatch = (char_w * char_h) * 0.35
            if min_mismatched_pixels <= max_allowed_mismatch:
                recognized_text += best_match_char
                current_score += 1 # Add to score for a good character
            else:
                recognized_text += '?'
            # --- End of identical logic ---
            
            x_pos += step_size # Move to next grid position

        # Now that this offset scan is complete, check its score
        if current_score > best_score:
            best_score = current_score
            best_text = recognized_text
            
            # Small optimization: If the score is high (e.g., > 5 chars),
            # it's almost certainly the right one. We could break early.
            # But letting it run 7 times is more robust and still very fast.
            
    return best_text.strip()


def send_keepalive(ser: serial.Serial):
    try: ser.write(b'\x55\xAA\x00\x00')
    except serial.SerialException: pass

def serial_reader_thread(ser: serial.Serial, data_queue: queue.Queue, stop_event: threading.Event):
    thread_local_framebuffer = bytearray([0] * FRAME_SIZE)
    while not stop_event.is_set():
        try:
            new_frame = _read_frame_from_serial_port(ser, thread_local_framebuffer)
            if new_frame:
                thread_local_framebuffer = new_frame
                data_queue.put(thread_local_framebuffer)
            send_keepalive(ser)
        except (serial.SerialException, OSError):
            print("[!] Serial connection lost.")
            break

# Add this function in the global scope, near serial_reader_thread

def db_writer_thread_func(db_queue: queue.Queue, stop_event: threading.Event):
    """A dedicated thread to handle all database writes in batches."""
    conn = sqlite3.connect(DB_FILE)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous = NORMAL;") 
    cursor = conn.cursor()
    
    # --- START OF NEW FEATURE ---
    # Update the INSERT statement
    insert_sql = """
        INSERT INTO recordings (
            session_id, timestamp, spectrum_data, center_freq, start_freq, 
            end_freq, impedance_low, impedance_high, bars, step, 
            modulation, bandwidth, spectrum_sum
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    # --- END OF NEW FEATURE ---
    
    while not stop_event.is_set():
        try:
            data_batch = [db_queue.get(timeout=0.5)] 
            while not db_queue.empty():
                data_batch.append(db_queue.get_nowait())
            
            if data_batch:
                conn.execute("BEGIN;")
                cursor.executemany(insert_sql, data_batch) # Use the new SQL
                conn.commit()
                
        except queue.Empty:
            continue 
        except Exception as e:
            print(f"[DB Writer Error] {e}")
            try: conn.rollback()
            except: pass
    
    # Final flush on stop
    try:
        data_batch = []
        while not db_queue.empty():
            data_batch.append(db_queue.get_nowait())
        if data_batch:
            print(f"[DB Writer] Saving final {len(data_batch)} items before exit...")
            conn.execute("BEGIN;")
            cursor.executemany(insert_sql, data_batch) # Use the new SQL
            conn.commit()
    except Exception as e:
        print(f"[DB Writer Error] Final flush failed: {e}")
    finally:
        conn.close()
        print("[DB Writer] Thread stopped.")

def _read_frame_from_serial_port(ser: serial.Serial, current_fb: bytearray):
    while True:
        try:
            b = ser.read(1)
            if not b: return None
            if b == HEADER[0:1]:
                b2 = ser.read(1)
                if not b2: return None
                if b2 == HEADER[1:2]:
                    t = ser.read(1)
                    size_bytes = ser.read(2)
                    if not t or not size_bytes: return None
                    
                    size = int.from_bytes(size_bytes, 'big')
                    if t == TYPE_SCREENSHOT and size == FRAME_SIZE:
                        payload = ser.read(size)
                        if len(payload) == size: return bytearray(payload)
                    elif t == TYPE_DIFF and size > 0 and size % 9 == 0:
                        payload = ser.read(size)
                        if len(payload) == size: return apply_diff(current_fb.copy(), payload)
        except serial.SerialException:
            raise

def apply_diff(framebuffer: bytearray, diff_payload: bytes) -> bytearray:
    i = 0
    while i + 9 <= len(diff_payload):
        block_index = diff_payload[i]; i += 1
        if block_index >= 128: break
        framebuffer[block_index * 8 : block_index * 8 + 8] = diff_payload[i : i + 8]
        i += 8
    return framebuffer

def draw_frame(screen: pygame.Surface, fb: bytearray, bg_color: pygame.Color, fg_color: pygame.Color, area: pygame.Rect, pixel_size: int = 4, pixel_lcd: int = 0):
    screen.fill(bg_color, area)
    for y in range(HEIGHT):
        for x in range(WIDTH):
            bit_idx = y * WIDTH + x; byte_idx = bit_idx // 8; bit_pos = bit_idx % 8
            if (fb[byte_idx] >> bit_pos) & 0x01:
                px = area.left + x * (pixel_size - 1); py = area.top + y * pixel_size
                pygame.draw.rect(screen, fg_color, (px, py, pixel_size - 1 - pixel_lcd, pixel_size - pixel_lcd))

def cmd_list_ports(args: argparse.Namespace):
    ports = list_ports.comports()
    print("Available ports:")
    for port in ports:
        if port.vid is None: continue
        desc = " - ".join(filter(None, (port.product, port.manufacturer)))
        print(f"- {desc if desc else 'Unknown'}: {port.device}")


class K5ViewerApp:
    def __init__(self, args):
        self.args = args
        self.running = True
        
        # --- Pygame and Window Setup ---
        pygame.init()
        self.pixel_size, self.pixel_lcd = 5, 0
        self.base_width = WIDTH * (self.pixel_size - 1)
        self.TOOLBAR_HEIGHT = 40
        self.PREVIEW_AREA_HEIGHT = 250
        self.TIME_SCALE_WIDTH = 30
        self.show_preview_area = False
        
        win_width, win_height = self._get_window_size()
        self.screen = pygame.display.set_mode((win_width, win_height), pygame.RESIZABLE)
        self.base_title = f"Quansheng K5Viewer v{VERSION}"
        pygame.display.set_caption(self.base_title)
        
        self.clock = pygame.time.Clock()
        self.fg_color, self.bg_color = COLOR_SETS[DEFAULT_COLOR][1:]
        
        try:
            self.font = pygame.font.SysFont('Arial', 14)
            self.small_font = pygame.font.SysFont('Arial', 12)
            self.tiny_font = pygame.font.SysFont('Arial', 10)
            self.scale_large_font = pygame.font.SysFont('Arial', 16, bold=True)
            self.scale_bold_font = pygame.font.SysFont('Arial', 12, bold=True)
        except:
            self.font = pygame.font.Font(None, 18)
            self.small_font = pygame.font.Font(None, 16)
            self.tiny_font = pygame.font.Font(None, 14)
            self.scale_large_font = pygame.font.Font(None, 20)
            self.scale_bold_font = pygame.font.Font(None, 16)

        # --- State Management ---
        self.app_state = 'CONNECTION_MENU'
        self.main_framebuffer = bytearray([0] * FRAME_SIZE)
        
        # --- Serial Communication State ---
        self.ser = None
        self.data_queue = None
        self.stop_event = None
        self.reader_thread = None
        self.com_ports = []
        self.com_port_buttons = []
        self.connection_error_msg = ""
        self.last_frame_time = None
        self.current_frame_latency = 0.0
        
        # --- Recording State ---
        self.is_recording = False
        #self.db_conn = None
        #self.db_cursor = None
        self.db_write_queue = None  # <-- ADD THIS
        self.db_stop_event = None   # <-- ADD THIS
        self.db_writer_thread = None # <-- ADD THIS
        self.session_id_pk = None
        self.recording_start_time = 0
        self.spectrum_mode_active = False
        self.was_spectrum_mode_active = False
        self.last_recording_log_time = 0 # For rate limiting
        self.recording_interval = RECORDING_INTERVAL # Use global setting
        
        # --- Replay State ---
        self.replay_sessions = []
        self.replay_session_buttons = []
        self.replay_menu_scroll_offset = 0
        self.replay_search_text = ""
        self.replay_search_box_rect = pygame.Rect(20, self.TOOLBAR_HEIGHT + 20, 400, 30)
        self.replay_search_active = False # True if user is typing in the box
        self.btn_replay_search = pygame.Rect(0, 0, 80, 30)

        self.replay_date_search_text = ""
        self.replay_date_search_box_rect = pygame.Rect(0, 0, 150, 30)
        self.replay_date_search_active = False
      
        self.replay_global_max_energy = 0 # We'll populate this later
        self.slider_replay_energy = Slider(0, 0, 200, 8, 0, 1, 0)
    
        self.saved_waterfall_data = None
        self.REPLAY_BUFFER_SIZE = 2000
        self.REPLAY_FETCH_CHUNK_SIZE = 1000
        self.current_session_identifier = None
        self.total_frames_in_session = 0
        self.replay_buffer = deque()
        self.replay_buffer_start_index = 0
        self.replay_frame_index = 0
        self.replay_start_time = 0
        self.replay_session_start_dt = None
        self.replay_is_paused = False
        self.time_when_paused = 0
        self.replay_speed_options = [1, 2, 4, 8, 16]
        self.replay_speed_multiplier = 1
        self.replay_energy_map = []
        self.replay_max_energy = 0
        
        # --- Waterfall and Spectrum State ---
        self.waterfall_len = 60
        self.empty_scan = [0] * (SPECTRUM_X_END - SPECTRUM_X_START + 1)
        self.waterfall_data = deque([self.empty_scan] * self.waterfall_len, maxlen=self.waterfall_len)
        self.preset_names = list(PRESET_GRADIENTS.keys())
        self.color_config = {'current_preset_index': 0, 'range_start': 0.0, 'range_end': 1.0}
        self.full_freq_range = None
        self.view_freq_range = None
        self.calibration_pixel_offset = 0
        self.auto_align_triggered = False
        self.impedance_low_text, self.impedance_high_text = "N/A", "N/A"
        self.bars_text, self.step_text, self.mod_text, self.bw_text = "N/A", "N/A", "N/A", "N/A"
        self.center_freq_text, self.start_freq_text, self.end_freq_text = "N/A", "N/A", "N/A"
        self.clicked_freq_info = None
        self.waterfall_markers = []
        self.pending_marker = None
        self.replay_pois = []
        self.hovered_poi_info = None
        
        # --- Modal State ---
        self.modal_active = False
        self.modal_input_text = ""
        self.modal_data_to_save = None
        self.modal_rect = pygame.Rect(0, 0, 400, 150)
        self.modal_save_btn = pygame.Rect(0, 0, 80, 30)
        self.modal_cancel_btn = pygame.Rect(0, 0, 80, 30)

        # --- Rate Limiting for Waterfall UI ---
        self.last_waterfall_update_time = 0
        self.waterfall_update_interval = 1.0 / 10.0 # Target 10 UI updates per second for waterfall
        self.waterfall_updates_since_start = 0

        # --- UI Elements ---
        self._init_ui_elements()

    def _init_ui_elements(self):
        # Toolbar Buttons
        self.btn_preview_toggle = pygame.Rect(10, 5, 120, 30)
        self.btn_replay_toggle = pygame.Rect(140, 5, 80, 30)
        self.btn_disconnect = pygame.Rect(230, 5, 100, 30)
        self.btn_radio_menu = pygame.Rect(140, 5, 80, 30)

        # Replay Controls
        self.slider_replay_speed = Slider(320, 25, 100, 8, 0, len(self.replay_speed_options) - 1, 0)
        self.slider_seek = Slider(0, 0, 100, 8, 0, 0, 0)
        self.btn_play_pause = pygame.Rect(0, 0, 30, 20)

        # Side Panel Controls
        self.side_panel_ui_offset = self.base_width + self.TIME_SCALE_WIDTH
        self.btn_scheme_cycle = pygame.Rect(0, 0, 90, 30)
        self.slider_low = Slider(0, 25, 80, 8, -0.25, 1.0, 0.0)
        self.slider_high = Slider(0, 25, 80, 8, 0.0, 1.25, 1.0)
        self.division_options = [3, 5, 7, 9, 11, 13, 15, 29]
        initial_div_index = self.division_options.index(5)
        self.slider_divs = Slider(0, 25, 80, 8, 0, len(self.division_options) - 1, initial_div_index)
        self.btn_auto_align = pygame.Rect(0, 0, 90, 30)
        
        # Areas for drawing
        self.waterfall_area = pygame.Rect(0, 0, 0, 0)
        self.info_panel_rect = pygame.Rect(0, 0, 0, 0)

        self._set_side_panel_ui_positions()

    def _get_window_size(self):
        w = self.base_width * 2 + self.TIME_SCALE_WIDTH
        h = (HEIGHT * self.pixel_size) + self.TOOLBAR_HEIGHT
        if self.show_preview_area:
            h += self.PREVIEW_AREA_HEIGHT
        return w, h
    
    def _execute_replay_search(self):
        """Runs the DB query with all current search filters."""
        
        # --- START OF MODIFICATION ---
        date_val = self.replay_date_search_text.strip()
        energy_val = int(self.slider_replay_energy.val)
        
        print(f"[Search] Searching for text: '{self.replay_search_text}', after_date: '{date_val}', min_energy: {energy_val}")
        
        self.replay_sessions = get_sessions(
            self.replay_search_text,
            date_val if date_val else None,
            energy_val if energy_val > 0 else None
        )
        # --- END OF MODIFICATION ---
        
        self.replay_menu_scroll_offset = 0 # Reset scroll
        self.replay_search_active = False # Deactivate text boxes
        self.replay_date_search_active = False
        
    def _get_global_max_energy(self):
        """Finds the single highest spectrum_sum across all recordings."""
        try:
            conn = sqlite3.connect(DB_FILE)
            cursor = conn.cursor()
            cursor.execute("SELECT MAX(spectrum_sum) FROM recordings")
            result = cursor.fetchone()
            conn.close()
            if result and result[0] is not None:
                return int(result[0])
        except sqlite3.Error as e:
            print(f"[DB Error] Could not get global max energy: {e}")
        return 1 # Return a default

    def _set_side_panel_ui_positions(self):
        padding = 10
        current_x = self.side_panel_ui_offset - 20

        self.btn_scheme_cycle.topleft = (current_x, 5)
        current_x = self.btn_scheme_cycle.right + padding

        self.slider_low.rect.x = current_x
        self.slider_low.update_handle_pos()
        current_x = self.slider_low.rect.right + padding

        self.slider_high.rect.x = current_x
        self.slider_high.update_handle_pos()
        current_x = self.slider_high.rect.right + padding

        self.slider_divs.rect.x = current_x
        self.slider_divs.update_handle_pos()
        current_x = self.slider_divs.rect.right + padding
        
        self.btn_auto_align.topleft = (current_x, 5)

    def _resize_window(self):
        self.base_width = WIDTH * (self.pixel_size - 1)
        win_width, win_height = self._get_window_size()
        self.screen = pygame.display.set_mode((win_width, win_height), pygame.RESIZABLE)
        self.side_panel_ui_offset = self.base_width + self.TIME_SCALE_WIDTH
        self._set_side_panel_ui_positions()

    def _cleanup(self):
        # Stop the DB writer thread first (if it's running)
        if self.is_recording and self.db_stop_event:
            self.is_recording = False
            print("[Cleanup] Stopping writer thread...")
            self.db_stop_event.set()
            
            # Wait for the thread to finish flushing its queue and exit
            if self.db_writer_thread:
                # Wait up to 2 seconds for a clean shutdown
                self.db_writer_thread.join(timeout=2.0) 
            print("[Cleanup] Writer thread stopped.")
        
        # Stop the serial reader thread
        if self.ser:
            if self.stop_event:
                self.stop_event.set()
            if self.reader_thread:
                self.reader_thread.join(timeout=1)
            self.ser.close()

    def run(self):
        while self.running:
            self._handle_events()
            self._update()
            self._render()
            self.clock.tick(60)
        self._cleanup()

    def _handle_events(self):
        was_dragging_before_event = self.slider_seek.dragging

        for event in pygame.event.get():
            if event.type == pygame.QUIT:
                self.running = False
                return

            # --- Handle Modal events first if active ---
            if self.modal_active:
                self._handle_modal_events(event)
                continue

            # --- Handle Sliders ---
            self.slider_low.handle_event(event)
            self.slider_high.handle_event(event)
            self.slider_divs.handle_event(event)
            self.slider_replay_speed.handle_event(event)
            self.slider_seek.handle_event(event)

            # --- Handle State-Specific Events ---
            if self.app_state == 'CONNECTION_MENU':
                self._handle_events_connection(event)
            elif self.app_state == 'REPLAY_MENU':
                self._handle_events_replay_menu(event)
            elif self.app_state == 'LIVE':
                self._handle_events_live(event, was_dragging_before_event)
            elif self.app_state == 'REPLAYING':
                self._handle_events_replaying(event, was_dragging_before_event)
    
    def _handle_events_connection(self, event):
        if event.type != pygame.MOUSEBUTTONDOWN or event.button != 1: return

        if self.btn_replay_toggle.collidepoint(event.pos):
            self.app_state = 'REPLAY_MENU'
            self.replay_search_text = ""
            self.replay_date_search_text = ""
            self.replay_global_max_energy = self._get_global_max_energy()
            self.slider_replay_energy.max_val = self.replay_global_max_energy
            self.slider_replay_energy.val = 0
            self.replay_sessions = get_sessions() # Load all sessions initially
            
            self.replay_menu_scroll_offset = 0
            return

        for i, btn in enumerate(self.com_port_buttons):
            if btn.collidepoint(event.pos):
                try:
                    device_path = self.com_ports[i].device
                    if device_path.startswith("/dev/cu."):
                        tty_path = device_path.replace("/dev/cu.", "/dev/tty.", 1)
                        print(f"[Mac Fix] Attempting to use {tty_path} instead of {device_path}")
                        device_path = tty_path
    
                        self.ser = serial.Serial(device_path, BAUDRATE, timeout=TIMEOUT)
                        self.data_queue = queue.Queue()
                        self.stop_event = threading.Event()
                        self.reader_thread = threading.Thread(target=serial_reader_thread, args=(self.ser, self.data_queue, self.stop_event), daemon=True)
                        self.reader_thread.start()
                        self.app_state = 'LIVE'
                        self.connection_error_msg = ""
                except serial.SerialException as e:
                    self.connection_error_msg = f"Failed to connect: {e}"
    
    def _handle_events_replay_menu(self, event):
        # --- Pass event to the energy slider first ---
        self.slider_replay_energy.handle_event(event)

        if event.type == pygame.MOUSEBUTTONDOWN:
            if event.button == 1: # Left click
                
                # --- Check for click on the text search box ---
                if self.replay_search_box_rect.collidepoint(event.pos):
                    self.replay_search_active = True
                    self.replay_date_search_active = False
                # --- Check for click on the new date search box ---
                elif self.replay_date_search_box_rect.collidepoint(event.pos):
                    self.replay_search_active = False
                    self.replay_date_search_active = True
                else:
                    self.replay_search_active = False
                    self.replay_date_search_active = False
                    
                # --- Check for button clicks ---
                if self.btn_replay_search.collidepoint(event.pos):
                    self._execute_replay_search()
                    
                elif self.btn_radio_menu.collidepoint(event.pos):
                    self._return_to_live_or_connection()
                else:
                    # Check for session list clicks
                    for i, (session_identifier, start_time, *_) in enumerate(self.replay_sessions):
                        if i < len(self.replay_session_buttons) and self.replay_session_buttons[i].collidepoint(event.pos):
                            self._start_replay_session(session_identifier, start_time)
                            break
            
            elif event.button == 4: # Scroll Up
                self.replay_menu_scroll_offset = max(0, self.replay_menu_scroll_offset - 35)
            elif event.button == 5: # Scroll Down
                self.replay_menu_scroll_offset += 35

        # --- Handle keyboard input for the ACTIVE search box ---
        if event.type == pygame.KEYDOWN:
            if self.replay_search_active:
                # Typing in the main text search box
                if event.key == pygame.K_ESCAPE:
                    self.replay_search_active = False
                elif event.key == pygame.K_RETURN or event.key == pygame.K_KP_ENTER:
                    self._execute_replay_search()
                elif event.key == pygame.K_BACKSPACE:
                    self.replay_search_text = self.replay_search_text[:-1]
                else:
                    if event.unicode and event.unicode.isprintable():
                        self.replay_search_text += event.unicode
            
            elif self.replay_date_search_active:
                # Typing in the new date search box
                if event.key == pygame.K_ESCAPE:
                    self.replay_date_search_active = False
                elif event.key == pygame.K_RETURN or event.key == pygame.K_KP_ENTER:
                    self._execute_replay_search()
                elif event.key == pygame.K_BACKSPACE:
                    self.replay_date_search_text = self.replay_date_search_text[:-1]
                else:
                    if event.unicode and event.unicode.isprintable():
                        # Simple validation for date-like characters
                        if event.unicode in "0123456789-":
                            self.replay_date_search_text += event.unicode


    def _handle_events_live(self, event, was_dragging_before_event):
        self._handle_common_main_view_events(event, was_dragging_before_event)
        
        if event.type == pygame.MOUSEBUTTONDOWN and event.button == 1:
            if self.btn_replay_toggle.collidepoint(event.pos):
                # Don't save waterfall_data here, let _return handle refresh
                self.app_state = 'REPLAY_MENU'
                
                self.replay_search_text = ""
                self.replay_date_search_text = ""
                self.replay_global_max_energy = self._get_global_max_energy()
                self.slider_replay_energy.max_val = self.replay_global_max_energy
                self.slider_replay_energy.val = 0
                self.replay_sessions = get_sessions() # Load all sessions initially

                self.replay_menu_scroll_offset = 0
                # Don't reset latency timers here, they continue in background
            elif self.btn_disconnect.collidepoint(event.pos):
                self._disconnect_serial()

    def _handle_events_replaying(self, event, was_dragging_before_event):
        self._handle_common_main_view_events(event, was_dragging_before_event)
        
        if event.type == pygame.MOUSEBUTTONDOWN and event.button == 1:
            if self.btn_play_pause.collidepoint(event.pos):
                self._toggle_play_pause()
            elif self.btn_radio_menu.collidepoint(event.pos):
                self._return_to_live_or_connection()

    def _handle_common_main_view_events(self, event, was_dragging_before_event):
        if event.type == pygame.MOUSEBUTTONUP:
            if was_dragging_before_event and not self.slider_seek.dragging and event.button == 1 and self.app_state == 'REPLAYING':
                self._seek_replay_to(int(round(self.slider_seek.val)))

        if event.type == pygame.MOUSEBUTTONDOWN:
            if self.clicked_freq_info:
                # Check for click on the save button in the info box
                _, _, data_for_saving, save_btn = self.clicked_freq_info
                if save_btn.collidepoint(event.pos):
                    self.modal_data_to_save = data_for_saving
                    self.modal_input_text = ""
                    self.modal_active = True
                    return # Stop further processing of this click

            self.clicked_freq_info = None # Clear on any other click

            if event.button == 1:
                if self.btn_scheme_cycle.collidepoint(event.pos):
                    self.color_config['current_preset_index'] = (self.color_config['current_preset_index'] + 1) % len(self.preset_names)
                elif self.btn_preview_toggle.collidepoint(event.pos):
                    self.show_preview_area = not self.show_preview_area
                    self._resize_window()
                elif self.btn_auto_align.collidepoint(event.pos):
                    self.auto_align_triggered = True
                elif self.waterfall_area.collidepoint(event.pos) and (self.app_state == 'LIVE' or self.app_state == 'REPLAYING'):
                     # Only allow waterfall clicks in relevant modes
                    self._handle_waterfall_click(event.pos)
        
        if event.type == pygame.KEYDOWN:
             if not self.modal_active: # Only handle shortcuts if modal isn't active
                 self._handle_keyboard_shortcuts(event)


    def _handle_modal_events(self, event):
        if event.type == pygame.MOUSEBUTTONDOWN and event.button == 1:
            if self.modal_save_btn.collidepoint(event.pos):
                self._save_point_of_interest()
                self.modal_active = False
            elif self.modal_cancel_btn.collidepoint(event.pos):
                self.modal_active = False
                self.pending_marker = None
            # Don't clear modal if clicking inside the input box (optional enhancement)
        
        if event.type == pygame.KEYDOWN:
            if event.key == pygame.K_ESCAPE:
                self.modal_active = False
                self.pending_marker = None
            elif event.key == pygame.K_RETURN or event.key == pygame.K_KP_ENTER:
                self._save_point_of_interest()
                self.modal_active = False
            elif event.key == pygame.K_BACKSPACE:
                self.modal_input_text = self.modal_input_text[:-1]
            else:
                # Basic input handling, might need more checks (e.g., length limit)
                self.modal_input_text += event.unicode


    def _update(self):
        # Process any waiting serial data first
        self._process_serial_data() 
        
        # Manage recording state changes (start/stop) based on spectrum activity
        if self.ser: # Only manage recording if connected
            self._manage_auto_recording()
            
        # Update markers regardless of state (they might be visible in replay or live)
        self._update_waterfall_markers()
        
        # Update replay timeline only if in replay mode
        if self.app_state == 'REPLAYING':
            self._update_replay_timeline()
            
    def _process_serial_data(self):
        if not self.data_queue: return
        try:
            while not self.data_queue.empty(): # Process all frames in the queue
                new_frame = self.data_queue.get_nowait()
                if new_frame:
                    current_time = time.monotonic()
                    if self.last_frame_time is not None:
                        self.current_frame_latency = current_time - self.last_frame_time
                    self.last_frame_time = current_time

                    self.main_framebuffer = new_frame # Update latest framebuffer for UI
                    
                    # Check spectrum status from the new frame
                    self.spectrum_mode_active = is_spectrum_analyzer_active(new_frame)
                    
                    if self.spectrum_mode_active:
                        # 1. Perform OCR and store data locally
                        local_ocr_data = {}
                        local_ocr_data['center_freq'] = ocr_area_anchor_scan(new_frame, CENTER_FREQ_RECT, LARGE_FONT_MAP, LARGE_FONT_DIMS)
                        local_ocr_data['start_freq'] = ocr_area(new_frame, START_FREQ_RECT, SMALL_FONT_MAP, SMALL_FONT_DIMS)
                        local_ocr_data['end_freq'] = ocr_area(new_frame, END_FREQ_RECT, SMALL_FONT_MAP, SMALL_FONT_DIMS)

                        impedance_full = ocr_area(new_frame, ZONE_A_RECT, SMALL_FONT_MAP, SMALL_FONT_DIMS)
                        if '/' in impedance_full:
                            parts = impedance_full.split('/')
                            local_ocr_data['impedance_low'] = parts[0].strip() if len(parts) > 0 and parts[0].strip() else "N/A"
                            local_ocr_data['impedance_high'] = parts[1].strip() if len(parts) > 1 and parts[1].strip() else "N/A"
                        else:
                            local_ocr_data['impedance_low'], local_ocr_data['impedance_high'] = "N/A", "N/A"

                        bars_raw = ocr_area(new_frame, ZONE_B_RECT, SMALL_FONT_MAP, SMALL_FONT_DIMS).strip()
                        local_ocr_data['bars'] = f"{bars_raw[:-1]}x" if bars_raw and len(bars_raw) > 1 and bars_raw[-1].isdigit() else f"{bars_raw}x" if bars_raw else "N/A"
                        
                        step_raw = ocr_area(new_frame, ZONE_C_RECT, SMALL_FONT_MAP, SMALL_FONT_DIMS).strip()
                        local_ocr_data['step'] = f"{step_raw[:-1]}k" if step_raw and len(step_raw) > 1 and step_raw[-1].lower() == 'k' else f"{step_raw}k" if step_raw else "N/A"

                        local_ocr_data['modulation'] = ocr_area_rtl(new_frame, ZONE_D_RECT, SMALL_FONT_MAP, SMALL_FONT_DIMS)
                        local_ocr_data['bandwidth'] = ocr_area_rtl(new_frame, ZONE_E_RECT, SMALL_FONT_MAP, SMALL_FONT_DIMS)
                        
                        # Calculate spectrum data and sum
                        spectrum_data = get_spectrum_data(new_frame)
                        local_ocr_data['spectrum_sum'] = sum(spectrum_data)

                        # 2. Handle Recording (always happens in background)
                        if self.is_recording:
                            should_log = False
                            if self.recording_interval == 0:
                                should_log = True
                            elif self.recording_interval > 0 and current_time - self.last_recording_log_time >= self.recording_interval:
                                should_log = True
                                self.last_recording_log_time = current_time

                            if should_log:
                                # We pass the local_ocr_data directly to the logger
                                self._log_frame_to_db(spectrum_data, local_ocr_data) 
                        
                        # 3. Handle UI Update (ONLY if in LIVE mode)
                        if self.app_state == 'LIVE':
                            self.center_freq_text = local_ocr_data['center_freq']
                            self.start_freq_text = local_ocr_data['start_freq']
                            self.end_freq_text = local_ocr_data['end_freq']
                            self.impedance_low_text = local_ocr_data['impedance_low']
                            self.impedance_high_text = local_ocr_data['impedance_high']
                            self.bars_text = local_ocr_data['bars']
                            self.step_text = local_ocr_data['step']
                            self.mod_text = local_ocr_data['modulation']
                            self.bw_text = local_ocr_data['bandwidth']
                    
                    else: # Spectrum is not active
                        # If in LIVE mode, clear the UI text
                        if self.app_state == 'LIVE':
                            self.center_freq_text, self.start_freq_text, self.end_freq_text = "N/A", "N/A", "N/A"
                            self.impedance_low_text, self.impedance_high_text = "N/A", "N/A"
                            self.bars_text, self.step_text, self.mod_text, self.bw_text = "N/A", "N/A", "N/A", "N/A"

        except queue.Empty:
            pass # No more frames for now
        except Exception as e:
            print(f"[Error] Exception processing serial data: {e}")


    def _update_ocr_variables(self):
        """Extracts OCR data and updates instance variables for immediate use."""
        # This function assumes self.main_framebuffer holds the relevant frame
        # In the new logic, _manage_auto_recording calls this, ensuring it uses
        # the latest frame processed by _process_serial_data.
        self.center_freq_text = ocr_area_anchor_scan(self.main_framebuffer, CENTER_FREQ_RECT, LARGE_FONT_MAP, LARGE_FONT_DIMS)
        self.start_freq_text = ocr_area(self.main_framebuffer, START_FREQ_RECT, SMALL_FONT_MAP, SMALL_FONT_DIMS)
        self.end_freq_text = ocr_area(self.main_framebuffer, END_FREQ_RECT, SMALL_FONT_MAP, SMALL_FONT_DIMS)
        
        impedance_full = ocr_area(self.main_framebuffer, ZONE_A_RECT, SMALL_FONT_MAP, SMALL_FONT_DIMS)
        if '/' in impedance_full:
            parts = impedance_full.split('/')
            if len(parts) == 2:
                self.impedance_low_text, self.impedance_high_text = parts[0].strip(), parts[1].strip()
            else: # Handle cases like '50/' or '/75' if they occur
                 self.impedance_low_text = parts[0].strip() if parts[0].strip() else "N/A"
                 self.impedance_high_text = parts[1].strip() if len(parts) > 1 and parts[1].strip() else "N/A"
        else:
             # If no '/', maybe it's just one value? Or invalid. Treat as N/A for now.
            self.impedance_low_text, self.impedance_high_text = "N/A", "N/A"


        bars_raw = ocr_area(self.main_framebuffer, ZONE_B_RECT, SMALL_FONT_MAP, SMALL_FONT_DIMS).strip()
        self.bars_text = f"{bars_raw[:-1]}x" if bars_raw and len(bars_raw) > 1 and bars_raw[-1].isdigit() else f"{bars_raw}x" if bars_raw else "N/A" # Basic check if last char is digit
        
        step_raw = ocr_area(self.main_framebuffer, ZONE_C_RECT, SMALL_FONT_MAP, SMALL_FONT_DIMS).strip()
        # Add more robust check for 'k' or units if needed
        self.step_text = f"{step_raw[:-1]}k" if step_raw and len(step_raw) > 1 and step_raw[-1].lower() == 'k' else f"{step_raw}k" if step_raw else "N/A"


        self.mod_text = ocr_area_rtl(self.main_framebuffer, ZONE_D_RECT, SMALL_FONT_MAP, SMALL_FONT_DIMS)
        self.bw_text = ocr_area_rtl(self.main_framebuffer, ZONE_E_RECT, SMALL_FONT_MAP, SMALL_FONT_DIMS)

    def _manage_auto_recording(self):
        # Determine if spectrum is active based on the *latest* framebuffer
        self.spectrum_mode_active = is_spectrum_analyzer_active(self.main_framebuffer)
        
        # --- MODIFICATION ---
        # The call to _update_ocr_variables() has been REMOVED from here.
        # This stops the live system from fighting with the replay UI.
        # --- END MODIFICATION ---
        
        # Transition TO recording state
        if self.spectrum_mode_active and not self.was_spectrum_mode_active:
            if not self.is_recording: # Prevent starting if already recording somehow
                self.is_recording = True # Set this early to prevent re-entry
                self.waterfall_updates_since_start = 0 
                session_identifier = f"rec_{datetime.datetime.now():%Y%m%d_%H%M%S}"
                start_time_iso = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                try:
                    # 1. Insert the session synchronously (one-time fast operation)
                    conn = sqlite3.connect(DB_FILE)
                    conn.execute("PRAGMA journal_mode=WAL;") 
                    cursor = conn.cursor()
                    cursor.execute("INSERT INTO sessions (identifier, start_time) VALUES (?, ?)", (session_identifier, start_time_iso))
                    self.session_id_pk = cursor.lastrowid
                    conn.commit()
                    conn.close()

                    # 2. Start the background writer thread
                    self.db_write_queue = queue.Queue()
                    self.db_stop_event = threading.Event()
                    self.db_writer_thread = threading.Thread(
                        target=db_writer_thread_func, 
                        args=(self.db_write_queue, self.db_stop_event), 
                        daemon=True
                    )
                    self.db_writer_thread.start()

                    self.recording_start_time = time.monotonic()
                    self.last_recording_log_time = self.recording_start_time
                    print(f"[REC] Auto-recording started. Session ID: {session_identifier}")

                except sqlite3.Error as e:
                    print(f"[DB Error] Failed to start recording session: {e}")
                    self.is_recording = False # Ensure state is correct on failure
                    if self.db_stop_event: self.db_stop_event.set() # Stop thread if it started
                    self.db_write_queue, self.db_stop_event, self.db_writer_thread = None, None, None
                    self.session_id_pk = None


        # Transition FROM recording state
        elif not self.spectrum_mode_active and self.was_spectrum_mode_active:
            if self.is_recording: # Prevent stopping if not recording
                self.is_recording = False
                if self.db_stop_event:
                    print("[REC] Auto-recording stopping... signaling writer thread.")
                    self.db_stop_event.set() 
                
                # Clear thread-related variables
                self.db_write_queue, self.db_stop_event, self.db_writer_thread = None, None, None
                self.session_id_pk = None 
                print("[REC] Auto-recording stopped.")

        
        # Update the state for the next cycle
        self.was_spectrum_mode_active = self.spectrum_mode_active
        

    def _log_frame_to_db(self, spectrum_data_list, ocr_data):
        """Queues a single frame with pre-calculated spectrum and OCR data."""
        if not self.is_recording or not self.db_write_queue:
            return 

        timestamp = time.monotonic() - self.recording_start_time
        
        # --- START OF NEW FEATURE ---
        # Add the spectrum_sum to the tuple
        data_tuple = (
            self.session_id_pk, 
            timestamp, 
            json.dumps(spectrum_data_list), 
            ocr_data.get('center_freq', '?'), 
            ocr_data.get('start_freq', '?'), 
            ocr_data.get('end_freq', '?'),
            ocr_data.get('impedance_low', '?'), 
            ocr_data.get('impedance_high', '?'), 
            ocr_data.get('bars', '?'), 
            ocr_data.get('step', '?'), 
            ocr_data.get('modulation', '?'), 
            ocr_data.get('bandwidth', '?'),
            ocr_data.get('spectrum_sum', 0) # Add the new sum here
        )
        # --- END OF NEW FEATURE ---
        
        try:
            self.db_write_queue.put_nowait(data_tuple)
        except queue.Full:
            print("[Warn] DB writer queue is full! Data is being lost. Check disk speed.")
        except Exception as e:
            print(f"[Error] Failed to queue frame for recording: {e}")


    def _update_replay_timeline(self):
        if self.replay_is_paused or not self.replay_buffer: return

        # Fetch more data if we're nearing the end of the buffer
        fetch_threshold = self.replay_buffer_start_index + len(self.replay_buffer) - (self.REPLAY_FETCH_CHUNK_SIZE // 2)
        is_near_end_of_buffer = self.replay_frame_index >= fetch_threshold
        not_at_end_of_session = (self.replay_buffer_start_index + len(self.replay_buffer)) < self.total_frames_in_session

        if is_near_end_of_buffer and not_at_end_of_session:
            # print(f"[REPLAY] Fetching next chunk...") # Reduce console noise
            next_chunk = get_session_data_chunk(self.current_session_identifier, self.replay_buffer_start_index + len(self.replay_buffer), self.REPLAY_FETCH_CHUNK_SIZE)
            self.replay_buffer.extend(next_chunk)
            while len(self.replay_buffer) > self.REPLAY_BUFFER_SIZE:
                self.replay_buffer.popleft()
                self.replay_buffer_start_index += 1

        # Check for speed change
        speed_index = min(round(self.slider_replay_speed.val), len(self.replay_speed_options) - 1)
        new_speed_multiplier = self.replay_speed_options[speed_index]

        current_frame_data = self._get_current_replay_frame()
        if not current_frame_data: return

        if new_speed_multiplier != self.replay_speed_multiplier:
            self.replay_start_time = time.monotonic() - (current_frame_data['timestamp'] / new_speed_multiplier)
            self.replay_speed_multiplier = new_speed_multiplier

        # Advance frame based on time
        if not self.slider_seek.dragging:
            elapsed_time = time.monotonic() - self.replay_start_time
             # Get the timestamp of the *next* frame to determine when to advance
            next_frame_data = self._get_replay_frame_by_index(self.replay_frame_index + 1)
            advance_time = float('inf') # Default to never advancing if it's the last frame
            if next_frame_data:
                 advance_time = next_frame_data['timestamp'] / self.replay_speed_multiplier

            # If enough time has passed to show the next frame
            if elapsed_time >= advance_time:
                if self.replay_frame_index < self.total_frames_in_session - 1:
                    self.replay_frame_index += 1
                    self.slider_seek.val = self.replay_frame_index
                    # Get the *new* current frame data to display
                    new_current_frame_data = self._get_current_replay_frame()
                    if new_current_frame_data:
                         self._update_display_from_replay_frame(new_current_frame_data)
                         self.waterfall_data.appendleft(json.loads(new_current_frame_data['spectrum_data']))
                         self.waterfall_updates_since_start += 1 # Sync UI counter
                    else:
                         # Should not happen if index check is correct, but handle gracefully
                         self.replay_is_paused = True

                else: # We were already at the last frame
                    self.replay_is_paused = True
            # else: Keep showing the current frame


    def _update_waterfall_markers(self):
        if not self.waterfall_markers: return
        
        if self.app_state == 'LIVE':
            # In live mode, markers age based on waterfall updates
            for marker in self.waterfall_markers:
                marker['y_offset_updates'] = self.waterfall_updates_since_start - marker['initial_update_count']
        elif self.app_state == 'REPLAYING':
            # In replay, markers are tied to the frame index
            for marker in self.waterfall_markers:
                marker['y_offset_updates'] = self.replay_frame_index - marker['initial_update_count']
        
        # Remove markers that have scrolled off-screen
        self.waterfall_markers = [m for m in self.waterfall_markers if m['initial_y_idx'] + m['y_offset_updates'] < self.waterfall_len]

    def _render(self):
        self.screen.fill((20, 20, 20))

        if self.app_state == 'CONNECTION_MENU':
            self._render_connection_menu()
        elif self.app_state == 'REPLAY_MENU':
            self._render_replay_menu()
        else: # LIVE or REPLAYING
            self._render_main_view()
        
        if self.modal_active:
            self._render_modal()
            
        pygame.display.flip()

def _render_connection_menu(self):
        pygame.display.set_caption(f"{self.base_title} – Connection Manager")
        title_surf = self.scale_large_font.render("Select a COM Port to Connect", True, (220, 220, 220))
        self.screen.blit(title_surf, (20, self.TOOLBAR_HEIGHT + 20))
        
        all_ports = list_ports.comports()
        self.com_ports = []
        self.com_port_buttons = []
        y_pos = self.TOOLBAR_HEIGHT + 60
        win_width, _ = self.screen.get_size()

        # Check the OS. 'darwin' is macOS. 'win32' is Windows.
        is_macos = (sys.platform == 'darwin')

        for port in all_ports:
            
            # Apply the strict filter ONLY on macOS. 
            # Windows needs to see all ports, even those without VID/PID.
            if is_macos and (port.vid is None or port.pid is None): 
                continue
            
            # Now the logic is correct for both:
            # - On Mac, only filtered ports are added.
            # - On Windows, all ports are added.
            self.com_ports.append(port)
            
            btn_rect = pygame.Rect(20, y_pos, win_width - 40, 30)
            self.com_port_buttons.append(btn_rect)
            port_text = f"{port.device}: {port.description}"
            port_surf = self.font.render(port_text, True, (200, 200, 200))
            self.screen.blit(port_surf, (btn_rect.x + 10, btn_rect.y + 7))
            y_pos += 35

        
        if self.connection_error_msg:
            error_surf = self.small_font.render(self.connection_error_msg, True, (255, 100, 100))
            self.screen.blit(error_surf, (20, y_pos + 10))

        toolbar_rect = pygame.Rect(0, 0, win_width, self.TOOLBAR_HEIGHT)
        pygame.draw.rect(self.screen, (50, 50, 50), toolbar_rect)
        pygame.draw.rect(self.screen, (100,100,100), self.btn_replay_toggle)
        self.screen.blit(self.font.render("Replay", True, (255,255,255)), (self.btn_replay_toggle.x+15, self.btn_replay_toggle.y+7))
    
    def _render_replay_menu(self):
        pygame.display.set_caption(f"{self.base_title} – Replay Menu")
        win_width, win_height = self.screen.get_size()

        # --- 1. Draw the search UI ---

        # --- Row 1: Text Search ---
        button_width = self.btn_replay_search.width
        padding = 10
        self.btn_replay_search.topright = (win_width - 20, self.TOOLBAR_HEIGHT + 20)
        self.btn_replay_search.height = self.replay_search_box_rect.height # Match height

        self.replay_search_box_rect.topleft = (20, self.TOOLBAR_HEIGHT + 20)
        self.replay_search_box_rect.width = win_width - 40 - button_width - padding

        # Draw the box's black background first
        pygame.draw.rect(self.screen, (20, 20, 20), self.replay_search_box_rect)

        # Define the padded area where text can be drawn
        text_render_rect = self.replay_search_box_rect.inflate(-10, -10)

        # Draw placeholder text (only if no text and not active)
        if not self.replay_search_text and not self.replay_search_active:
            placeholder_surf = self.font.render("Search by ID or POI...", True, (100, 100, 100))
            self.screen.blit(placeholder_surf, text_render_rect.topleft)

        # Render the actual search text in WHITE
        text_surf = self.font.render(self.replay_search_text, True, (255, 255, 255))
        text_width = text_surf.get_width()

        # Set a temporary clipping region to prevent text overflow
        self.screen.set_clip(text_render_rect)

        # Calculate text X position for left alignment with scrolling
        text_x = text_render_rect.left
        overflow = text_width - text_render_rect.width
        if overflow > 0:
            text_x -= overflow # Shift text left if it's too long

        # Blit the text surface at its starting position (potentially shifted left)
        self.screen.blit(text_surf, (text_x, text_render_rect.top))

        # IMPORTANT: Reset the clipping region
        self.screen.set_clip(None)

        # Draw the box border (highlight if active)
        border_color = (200, 200, 255) if self.replay_search_active else (150, 150, 150)
        pygame.draw.rect(self.screen, border_color, self.replay_search_box_rect, 1)

        # Draw cursor for main search box
        if self.replay_search_active and time.time() % 1 > 0.5:
            # Position cursor at the end of the visible text
            cursor_x = text_x + text_width
            # Constrain cursor X to be inside the visible text area
            cursor_x = max(text_render_rect.left, min(cursor_x, text_render_rect.right))
            pygame.draw.line(self.screen, (220, 220, 220), (cursor_x, text_render_rect.top), (cursor_x, text_render_rect.bottom), 1)

        # Draw the search button
        pygame.draw.rect(self.screen, (100, 100, 150), self.btn_replay_search)
        search_text_surf = self.font.render("Search", True, (255, 255, 255))
        self.screen.blit(search_text_surf, search_text_surf.get_rect(center=self.btn_replay_search.center))

        # --- Row 2: Date and Energy Filters ---
        row_2_y = self.replay_search_box_rect.bottom + 10

        # Date Box
        self.replay_date_search_box_rect.topleft = (20, row_2_y)
        pygame.draw.rect(self.screen, (20, 20, 20), self.replay_date_search_box_rect)

        date_text_render_rect = self.replay_date_search_box_rect.inflate(-10, -10)

        if not self.replay_date_search_text and not self.replay_date_search_active:
            placeholder_surf = self.font.render("Date (YYYY-MM-DD)", True, (100, 100, 100))
            self.screen.blit(placeholder_surf, date_text_render_rect.topleft)

        date_text_surf = self.font.render(self.replay_date_search_text, True, (255, 255, 255))
        # Use simple blit with clip area for date box
        self.screen.blit(date_text_surf, date_text_render_rect.topleft, date_text_render_rect.move(-date_text_render_rect.x, -date_text_render_rect.y))

        date_border_color = (200, 200, 255) if self.replay_date_search_active else (150, 150, 150)
        pygame.draw.rect(self.screen, date_border_color, self.replay_date_search_box_rect, 1)

        # Draw cursor for date box
        if self.replay_date_search_active and time.time() % 1 > 0.5:
            cursor_x = date_text_render_rect.left + date_text_surf.get_width()
            cursor_x = min(cursor_x, date_text_render_rect.right) # Clamp to right edge
            pygame.draw.line(self.screen, (220, 220, 220), (cursor_x, date_text_render_rect.top), (cursor_x, date_text_render_rect.bottom), 1)

        # Energy Slider
        slider_x = self.replay_date_search_box_rect.right + padding
        slider_width = self.replay_search_box_rect.right - slider_x # Make slider fill remaining space
        self.slider_replay_energy.rect.topleft = (slider_x, row_2_y + 11) # Align slider bar vertically
        self.slider_replay_energy.rect.width = slider_width
        self.slider_replay_energy.handle_rect.height = self.replay_date_search_box_rect.height # Match handle height to box height

        self.slider_replay_energy.update_handle_pos()
        slider_label = f"Min. Energy Sum >= {int(self.slider_replay_energy.val)}"
        self.slider_replay_energy.draw(self.screen, slider_label, self.small_font, is_percent=False)


        # --- 3. Adjust list position to be below all filters ---
        list_y_start = self.replay_date_search_box_rect.bottom + 10
        y_pos = list_y_start - self.replay_menu_scroll_offset

        self.replay_session_buttons = []

        list_area_height = win_height - list_y_start
        total_list_height = len(self.replay_sessions) * 35
        max_scroll_offset = max(0, total_list_height - list_area_height + 20)
        self.replay_menu_scroll_offset = max(0, min(self.replay_menu_scroll_offset, max_scroll_offset))

        # Unpack all 6 items correctly
        for (session_identifier, start_time_str, frame_count, duration_sec, poi_count, poi_descs) in self.replay_sessions:
            btn_rect = pygame.Rect(20, y_pos, win_width - 40, 30)
            self.replay_session_buttons.append(btn_rect)

            if btn_rect.bottom > self.TOOLBAR_HEIGHT and btn_rect.top < win_height:
                pygame.draw.rect(self.screen, (80, 80, 80), btn_rect)

                duration_fmt = f"{int(duration_sec // 60)}m {int(duration_sec % 60)}s" if duration_sec is not None else "0s"
                info_text = f"{session_identifier} | {start_time_str} | {duration_fmt} | {frame_count} frames"

                if poi_count > 0:
                    max_desc_len = 50 # Limit POI description length in list
                    truncated_descs = (poi_descs[:max_desc_len] + '...') if len(poi_descs) > max_desc_len else poi_descs
                    info_text += f" | POIs: {poi_count} ({truncated_descs})"

                session_surf = self.font.render(info_text, True, (200, 200, 200))
                # Define a clip area based on the button width minus padding
                clip_rect = btn_rect.inflate(-20, 0) # 10px padding on each side
                # Blit using the clip area relative to the button's top-left
                self.screen.blit(session_surf, (btn_rect.x + 10, btn_rect.y + 7), clip_rect.move(-btn_rect.x - 10, -btn_rect.y - 7))

            y_pos += 35

        # --- Toolbar (unchanged) ---
        toolbar_rect = pygame.Rect(0, 0, win_width, self.TOOLBAR_HEIGHT)
        pygame.draw.rect(self.screen, (50, 50, 50), toolbar_rect)

        pygame.draw.rect(self.screen, (80, 150, 80), self.btn_radio_menu)
        self.screen.blit(self.font.render("Radio", True, (255,255,255)), (self.btn_radio_menu.x+15, self.btn_radio_menu.y+7))


    def _render_main_view(self):
        win_width, win_height = self.screen.get_size()
        
        # --- Draw Radio Screen / Left Panel ---
        main_view_area_height = (HEIGHT * self.pixel_size)
        if self.show_preview_area and win_height > self.TOOLBAR_HEIGHT + self.PREVIEW_AREA_HEIGHT:
            main_view_area_height = win_height - self.TOOLBAR_HEIGHT - self.PREVIEW_AREA_HEIGHT
        main_view_area = pygame.Rect(0, self.TOOLBAR_HEIGHT, self.base_width, main_view_area_height)
        
        # In replay mode, don't draw the live framebuffer, show placeholder
        if self.app_state == 'REPLAYING':
            self.screen.fill((0,0,0), main_view_area) # Black background
            replay_surf = self.scale_large_font.render("REPLAY MODE", True, (100, 100, 100))
            replay_rect = replay_surf.get_rect(center=main_view_area.center)
            self.screen.blit(replay_surf, replay_rect)
            # You might want to overlay the current replayed frame's OCR data here
            # for context, but keep it distinct from the live view draw_frame call.
        else: # LIVE state (or connection menu before live starts)
            draw_frame(self.screen, self.main_framebuffer, self.bg_color, self.fg_color, main_view_area, self.pixel_size, self.pixel_lcd)

        
        # --- Draw Waterfall / Right Panel ---
        panel_area = pygame.Rect(self.base_width, self.TOOLBAR_HEIGHT, self.base_width + self.TIME_SCALE_WIDTH, main_view_area_height)
        pygame.draw.rect(self.screen, (30,30,50), panel_area)
        
        # Spectrum panel is drawn in both LIVE and REPLAYING states if active
        if self.spectrum_mode_active or self.app_state == 'REPLAYING': # Show waterfall in replay too
             try: # Add try-except for robustness during state transitions
                 self._render_spectrum_panel(panel_area)
             except Exception as e:
                 print(f"[Error] Rendering spectrum panel failed: {e}")
                 # Optionally draw an error message on the panel
                 error_surf = self.font.render('Error rendering spectrum', True, (255,100,100))
                 self.screen.blit(error_surf, (self.base_width + 20, self.TOOLBAR_HEIGHT + 20))
        elif self.ser: # Connected but spectrum not active
             self.screen.blit(self.font.render('Spectrum Analyzer not active...', True, (200,200,200)), (self.base_width + 20, self.TOOLBAR_HEIGHT + 20))
        else: # Not connected
             self.screen.blit(self.font.render('Not connected...', True, (150,150,150)), (self.base_width + 20, self.TOOLBAR_HEIGHT + 20))


        
        # --- Draw Toolbar ---
        self._render_toolbar()

        # --- Draw Preview Area ---
        if self.show_preview_area:
            self._render_preview_area()
            
        # --- Draw Overlays ---
        # Draw the main click info box
        if self.clicked_freq_info:
            bg_rect, drawable_items, _, save_btn = self.clicked_freq_info
            pygame.draw.rect(self.screen, (20, 20, 20), bg_rect)
            pygame.draw.rect(self.screen, (100, 100, 100), bg_rect, 1)

            for surf, rect in drawable_items:
                self.screen.blit(surf, rect)
            
            # Only show save button if recording is possible (LIVE mode or REPLAYING a valid frame)
            if self.is_recording or (self.app_state == 'REPLAYING' and self.clicked_freq_info[2].get('recording_id') is not None):
                 pygame.draw.rect(self.screen, (80, 150, 80), save_btn)
                 save_surf = self.font.render("+", True, (255, 255, 255))
                 self.screen.blit(save_surf, save_surf.get_rect(center=save_btn.center))

        # Draw the hover info box (only in REPLAYING mode for POIs)
        elif self.app_state == 'REPLAYING' and self.hovered_poi_info:
            bg_rect, drawable_items = self.hovered_poi_info
            pygame.draw.rect(self.screen, (20, 20, 20), bg_rect)
            pygame.draw.rect(self.screen, (100, 100, 100), bg_rect, 1)
            for surf, rect in drawable_items:
                self.screen.blit(surf, rect)

    
    def _render_spectrum_panel(self, panel_area):
        current_time = time.monotonic()

        # --- MODIFICATION START: Sync UI update rate with Recording Rate ---
        # Determine the UI update interval to sync with recording
        # If interval is 0 (every frame), cap UI at 10fps (default) to avoid lag
        # If interval > 0, use that interval for UI updates.
        effective_ui_interval = self.recording_interval
        if self.recording_interval == 0:
            effective_ui_interval = self.waterfall_update_interval # Use the 10fps default cap
        # --- MODIFICATION END ---

        # --- Update Waterfall UI Data (Rate Limited) ---
        if self.app_state == 'LIVE':
            # Use the new effective_ui_interval
            if current_time - self.last_waterfall_update_time >= effective_ui_interval:
                if self.main_framebuffer and self.spectrum_mode_active: # Only update if spectrum active
                    # Use the latest framebuffer processed, recalculate spectrum data for UI
                    spectrum_data_ui = get_spectrum_data(self.main_framebuffer)
                    self.waterfall_data.appendleft(spectrum_data_ui)
                    self.last_waterfall_update_time = current_time # Reset UI timer
                    self.waterfall_updates_since_start += 1
                elif not self.spectrum_mode_active:
                     # If spectrum stops, add empty scans to UI waterfall
                     self.waterfall_data.appendleft(self.empty_scan)
                     self.last_waterfall_update_time = current_time
                     self.waterfall_updates_since_start += 1


        # --- Update frequency ranges (from latest OCR, could be slightly stale in Replay) ---
        try:
            # Use the class members updated by _update_ocr_variables or _update_display_from_replay_frame
            start_f_text = self.start_freq_text
            end_f_text = self.end_freq_text
            center_f_text = self.center_freq_text

            # Attempt to parse
            new_start_f = float(start_f_text.replace('?', '').strip())
            new_end_f = float(end_f_text.replace('?', '').strip())

             # Update full range only if it changes significantly (avoids flicker on minor OCR errors)
            if self.full_freq_range is None or abs(self.full_freq_range[0] - new_start_f) > 0.01 or abs(self.full_freq_range[1] - new_end_f) > 0.01:
                 # If this is the *first* time or range changes, reset view range too
                is_first_update = self.full_freq_range is None
                self.full_freq_range = (new_start_f, new_end_f)
                if is_first_update or self.app_state == 'LIVE': # Reset view on range change in Live mode
                     self.view_freq_range = (new_start_f, new_end_f)
                 # In Replay, don't reset view range automatically on frame change

        except (ValueError, TypeError, AttributeError):
            # --- THIS IS THE FIX ---
            # If we fail to parse (e.g., temporary "N/A" or "?"),
            # DO NOT wipe out the existing good ranges.
            # Just 'pass' and let the graph render with the old ranges
            # until the OCR recovers on the next frame.
            if self.full_freq_range is None:
                # Only set to None if they were never valid in the first place.
                self.full_freq_range = None
                self.view_freq_range = None
            else:
                pass # Keep the last known good ranges
            # --- END FIX ---


        
        # Define drawing areas (dependent on updated frequency ranges potentially)
        graph_left_x = self.side_panel_ui_offset + 10
        graph_width = self.base_width - 20
        scale_area = pygame.Rect(graph_left_x, self.TOOLBAR_HEIGHT + 10, graph_width, 60)
        
        INFO_PANEL_HEIGHT = 25 if self.app_state == 'REPLAYING' else 0
        # Calculate height available for time scale + waterfall
        available_graph_height = panel_area.height - (scale_area.bottom - self.TOOLBAR_HEIGHT) - 10 # Reserve space below scale
        # If replay controls are shown, subtract their height
        if INFO_PANEL_HEIGHT > 0:
            available_graph_height -= (INFO_PANEL_HEIGHT + 5) # +5 for padding

        available_graph_height = max(10, available_graph_height) # Minimum height


        time_scale_area = pygame.Rect(self.base_width, scale_area.bottom, self.TIME_SCALE_WIDTH, available_graph_height)
        self.waterfall_area = pygame.Rect(graph_left_x, scale_area.bottom, graph_width, available_graph_height)
        
        # Position info panel below waterfall
        self.info_panel_rect = pygame.Rect(graph_left_x, self.waterfall_area.bottom + 5, graph_width, INFO_PANEL_HEIGHT)


        # Auto-align logic
        if self.auto_align_triggered: self._execute_auto_align()

        # Set slider values for color mapping
        self.color_config['range_start'], self.color_config['range_end'] = self.slider_low.val, self.slider_high.val
        div_index = min(round(self.slider_divs.val), len(self.division_options) - 1)
        num_divisions = self.division_options[div_index]
        
        # --- Draw main components ---
        pygame.draw.rect(self.screen, (0,0,0), self.waterfall_area) # Black background for waterfall

        # Draw waterfall only if we have valid frequency ranges
        if self.full_freq_range and self.view_freq_range:
            draw_waterfall(self.screen, self.waterfall_data, self.waterfall_area, self.color_config, self.full_freq_range, self.view_freq_range, self.calibration_pixel_offset)
            self._render_waterfall_markers() # Draw temporary markers

            if self.app_state == 'REPLAYING':
                 self._render_and_handle_poi_markers() # Draw POI markers from DB

            # Draw scales
            view_start_f, view_end_f = self.view_freq_range
            # Adjust time scale max based on current waterfall length
            draw_time_scale(self.screen, time_scale_area, self.small_font, self.waterfall_len) # Pass actual length
            draw_frequency_scale(self.screen, view_start_f, self.center_freq_text, view_end_f, scale_area, self.tiny_font, self.small_font, self.scale_large_font, self.scale_bold_font, num_divisions, self.waterfall_area.bottom)
            self._render_spectrum_overlays(scale_area) # Info text, center marker, mouse hover
        else:
             # Draw placeholder if ranges are invalid
            no_range_surf = self.font.render('Waiting for frequency data...', True, (180,180,180))
            self.screen.blit(no_range_surf, no_range_surf.get_rect(center=self.waterfall_area.center))


        # Draw replay controls below waterfall if in replay mode
        if self.app_state == 'REPLAYING':
            self._render_replay_controls()
    

    def _render_spectrum_overlays(self, scale_area):
        # Info text (uses latest values from class attributes)
        info_color = (180, 180, 180)
        left_info = f"dB: {self.impedance_low_text}/{self.impedance_high_text} | Bars: {self.bars_text} | Step: {self.step_text}"
        right_info = f"Mod: {self.mod_text} | BW: {self.bw_text}"
        self.screen.blit(self.tiny_font.render(left_info, True, info_color), (scale_area.left, scale_area.top - 6))
        right_surf = self.tiny_font.render(right_info, True, info_color)
        self.screen.blit(right_surf, right_surf.get_rect(topright=(scale_area.right, scale_area.top - 6)))
        
        # Center frequency marker (only if view range is valid)
        if self.view_freq_range:
             try:
                 center_f = float(self.center_freq_text.replace('?', '').replace(' ', ''))
                 view_start_f, view_end_f = self.view_freq_range
                 if view_start_f <= center_f <= view_end_f:
                     view_span = view_end_f - view_start_f
                     if view_span > 0:
                         ratio = (center_f - view_start_f) / view_span
                         x_pos = scale_area.left + (ratio * scale_area.width)
                         # Draw slightly above the waterfall top
                         marker_top_y = self.waterfall_area.top - 2
                         points = [(x_pos, marker_top_y), (x_pos - 4, marker_top_y - 6), (x_pos + 4, marker_top_y - 6)]
                         pygame.draw.polygon(self.screen, (255, 100, 100), points)
             except (ValueError, TypeError, AttributeError): pass # Ignore if OCR fails


        # Mouse crosshair and info (only if view range valid)
        mouse_pos = pygame.mouse.get_pos()
        # Only show hover if mouse is over waterfall AND POI hover isn't active
        if self.view_freq_range and self.waterfall_area.collidepoint(mouse_pos) and self.hovered_poi_info is None:
             # Horizontal line (frequency)
            pygame.draw.line(self.screen, (200,200,200, 150), (self.waterfall_area.left, mouse_pos[1]), (self.waterfall_area.right, mouse_pos[1]))
             # Vertical line (time)
            pygame.draw.line(self.screen, (200,200,200, 150), (mouse_pos[0], self.waterfall_area.top), (mouse_pos[0], self.waterfall_area.bottom))

            if self.waterfall_area.width > 0:
                x_ratio = (mouse_pos[0] - self.waterfall_area.left) / self.waterfall_area.width
                 # Calculate frequency based on current view range
                mouse_freq = self.view_freq_range[0] + (x_ratio * (self.view_freq_range[1] - self.view_freq_range[0]))
                freq_text = f"{mouse_freq:.5f} MHz"
                text_surf = self.tiny_font.render(freq_text, True, (255, 255, 255))
                text_rect = text_surf.get_rect(left=mouse_pos[0] + 10, bottom=mouse_pos[1] - 5)
                
                 # Adjust position if too close to edge
                if text_rect.right > self.screen.get_width() - 5:
                    text_rect.right = mouse_pos[0] - 10
                if text_rect.left < 5:
                     text_rect.left = mouse_pos[0] + 10 # Revert if flipping left goes off screen

                bg_rect = text_rect.inflate(6, 4)
                pygame.draw.rect(self.screen, (20, 20, 20, 200), bg_rect) # Slightly transparent background
                self.screen.blit(text_surf, text_rect)
    

    def _render_replay_controls(self):
        # Draw background for controls area if defined
        if self.info_panel_rect.height > 0:
             pygame.draw.rect(self.screen, (40, 40, 45), self.info_panel_rect)

        # --- START OF UI LAYOUT CHANGE ---
        
        # 1. Position Play/Pause button on the far left, aligned with time scale
        # We need the time_scale_area from _render_spectrum_panel.
        # Let's assume self.base_width is the start of the time scale area.
        self.btn_play_pause.topleft = (self.base_width + 5, self.info_panel_rect.centery - 10)
        
        # --- END OF UI LAYOUT CHANGE ---
        
        pygame.draw.rect(self.screen, (100,100,100), self.btn_play_pause)
        play_pause_text = "||" if not self.replay_is_paused else "►"
        play_pause_surf = self.font.render(play_pause_text, True, (255,255,255))
        self.screen.blit(play_pause_surf, play_pause_surf.get_rect(center=self.btn_play_pause.center))
        
        # Seek Slider
        # --- START OF UI LAYOUT CHANGE ---
        
        # 2. Position slider to the right of the button
        slider_x_start = self.btn_play_pause.right + 10
        # 3. Make slider fill the remaining width
        slider_width = self.info_panel_rect.right - slider_x_start - 5 # 5px padding right
        
        # --- END OF UI LAYOUT CHANGE ---
        
        self.slider_seek.rect.topleft = (slider_x_start, self.info_panel_rect.centery - 4)
        self.slider_seek.rect.width = max(50, slider_width)
        
        self.slider_seek.max_val = max(0, self.total_frames_in_session - 1)

        if not self.slider_seek.dragging:
            self.slider_seek.val = max(0, min(self.replay_frame_index, self.slider_seek.max_val))

        self.slider_seek.update_handle_pos()
        progress_ratio = self.slider_seek.val / self.slider_seek.max_val if self.slider_seek.max_val > 0 else 0
        frame_text = f"{int(self.slider_seek.val)}/{int(self.slider_seek.max_val)}"
        
        # --- START OF NEW FEATURE ---
        # Pass the new energy map data to the slider's draw method
        self.slider_seek.draw(
            self.screen, "Frame: ", self.small_font, 
            is_percent=False, 
            actual_value=frame_text, 
            progress_ratio=progress_ratio,
            energy_map=self.replay_energy_map,
            max_energy=self.replay_max_energy,
            color_config=self.color_config
        )
        # --- END OF NEW FEATURE ---
        
        # Timestamp display
        current_frame = self._get_current_replay_frame()
        # ... (rest of the function is unchanged) ...
        if self.replay_session_start_dt and current_frame:
            try:
                frame_timestamp_sec = current_frame['timestamp']
                current_abs_time = self.replay_session_start_dt + datetime.timedelta(seconds=frame_timestamp_sec)
                time_str = current_abs_time.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]

                if self.replay_sessions:
                     current_session_info = next((s for s in self.replay_sessions if s[0] == self.current_session_identifier), None)
                     if current_session_info:
                         total_duration_sec = current_session_info[3]
                         if total_duration_sec > 0:
                             current_rel_min, current_rel_sec_ms = divmod(frame_timestamp_sec, 60)
                             total_rel_min, total_rel_sec = divmod(total_duration_sec, 60)
                             rel_time_str = f"{int(current_rel_min):02}:{current_rel_sec_ms:06.3f} / {int(total_rel_min):02}:{int(total_rel_sec):02}"
                             time_str += f" ({rel_time_str})"


                time_surf = self.tiny_font.render(time_str, True, (200,200,200)) 
                time_rect = time_surf.get_rect(right=self.slider_seek.rect.right, bottom=self.slider_seek.rect.top - 2)
                self.screen.blit(time_surf, time_rect)
            except (TypeError, ValueError) as e:
                 print(f"[Warn] Error formatting replay time: {e}")


    def _render_toolbar(self):
        win_width, _ = self.screen.get_size()
        toolbar_rect = pygame.Rect(0, 0, win_width, self.TOOLBAR_HEIGHT)
        pygame.draw.rect(self.screen, (50, 50, 50), toolbar_rect)
        
        # Left-side buttons
        pygame.draw.rect(self.screen, (100,100,100), self.btn_preview_toggle)
        preview_text = 'Hide Preview' if self.show_preview_area else 'Show Preview'
        self.screen.blit(self.font.render(preview_text, True, (255,255,255)), (self.btn_preview_toggle.x+10, self.btn_preview_toggle.y+7))
        
        if self.app_state == 'LIVE':
            pygame.draw.rect(self.screen, (100,100,100), self.btn_replay_toggle)
            self.screen.blit(self.font.render("Replay", True, (255,255,255)), (self.btn_replay_toggle.x+15, self.btn_replay_toggle.y+7))
            pygame.draw.rect(self.screen, (150, 80, 80), self.btn_disconnect)
            self.screen.blit(self.font.render("Disconnect", True, (255,255,255)), (self.btn_disconnect.x+10, self.btn_disconnect.y+7))
            
            # Recording Indicator (based on self.is_recording flag)
            rec_indicator_x = self.btn_disconnect.right + 15
            if self.is_recording:
                pygame.draw.circle(self.screen, (255, 0, 0), (rec_indicator_x, self.TOOLBAR_HEIGHT // 2), 7)
                rec_ind_surf = self.small_font.render('REC', True, (255,100,100)) # Use small font
                rec_ind_rect = rec_ind_surf.get_rect(left=rec_indicator_x + 10, centery=self.TOOLBAR_HEIGHT // 2)
                self.screen.blit(rec_ind_surf, rec_ind_rect)
                latency_start_x = rec_ind_rect.right + 10
            else:
                 latency_start_x = rec_indicator_x # No REC text, start latency text closer


            # Latency display (only if connected)
            if self.ser:
                 latency_text = f"Lat: {self.current_frame_latency:.3f}s" # Shorter format
                 latency_surf = self.small_font.render(latency_text, True, (200, 200, 200))
                 latency_rect = latency_surf.get_rect(left=latency_start_x, centery=self.TOOLBAR_HEIGHT // 2)
                 self.screen.blit(latency_surf, latency_rect)


        elif self.app_state == 'REPLAYING':
            pygame.draw.rect(self.screen, (80, 150, 80), self.btn_radio_menu)
            self.screen.blit(self.font.render("Radio", True, (255,255,255)), (self.btn_radio_menu.x+15, self.btn_radio_menu.y+7))
            
            # Speed Slider
            speed_index = min(round(self.slider_replay_speed.val), len(self.replay_speed_options) - 1)
            # Update multiplier in case it changed via direct value setting (though not implemented)
            self.replay_speed_multiplier = self.replay_speed_options[speed_index]
            self.slider_replay_speed.draw(self.screen, "Speed: ", self.small_font, is_percent=False, actual_value=f"{self.replay_speed_multiplier}x")


        # Right-side (side panel) controls (always visible if not in connection menu)
        if self.app_state != 'CONNECTION_MENU':
            scheme_name = self.preset_names[self.color_config['current_preset_index']]
            pygame.draw.rect(self.screen, (100,100,100), self.btn_scheme_cycle)
            scheme_surf = self.font.render(scheme_name, True, (255,255,255))
            self.screen.blit(scheme_surf, scheme_surf.get_rect(center=self.btn_scheme_cycle.center))
            
            self.slider_low.draw(self.screen, "Low", self.small_font)
            self.slider_high.draw(self.screen, "High", self.small_font)
            
            div_index = min(round(self.slider_divs.val), len(self.division_options) - 1)
            self.slider_divs.draw(self.screen, "Divs", self.small_font, is_percent=False, actual_value=self.division_options[div_index])
            
            pygame.draw.rect(self.screen, (100,100,100), self.btn_auto_align)
            auto_align_surf = self.font.render('Auto-Align', True, (255,255,255))
            self.screen.blit(auto_align_surf, auto_align_surf.get_rect(center=self.btn_auto_align.center))
            
            offset_surf = self.small_font.render(f"Offset: {self.calibration_pixel_offset}px", True, (200, 200, 200))
            # Position offset relative to the right edge
            self.screen.blit(offset_surf, offset_surf.get_rect(right=win_width - 10, centery=self.TOOLBAR_HEIGHT // 2))


    def _render_preview_area(self):
        win_width, win_height = self.screen.get_size()
        preview_panel_y = win_height - self.PREVIEW_AREA_HEIGHT
        preview_panel_rect = pygame.Rect(0, preview_panel_y, win_width, self.PREVIEW_AREA_HEIGHT)
        pygame.draw.rect(self.screen, (30,30,40), preview_panel_rect)
        
        # Only draw OCR previews if in LIVE mode, as main_framebuffer isn't updated in REPLAY
        if self.app_state == 'LIVE':
             # Use self.main_framebuffer which holds the latest live frame
            draw_ocr_preview(self.screen, self.main_framebuffer, CENTER_FREQ_RECT, (20, preview_panel_y + 30), "Center Freq Area", self.small_font)
            draw_ocr_preview(self.screen, self.main_framebuffer, START_FREQ_RECT, (20, preview_panel_y + 115), "Start Freq Area", self.small_font)
            draw_ocr_preview(self.screen, self.main_framebuffer, END_FREQ_RECT, (20, preview_panel_y + 180), "End Freq Area", self.small_font)
            draw_ocr_preview(self.screen, self.main_framebuffer, ZONE_A_RECT, (450, preview_panel_y + 30), "Zone A", self.small_font)
            draw_ocr_preview(self.screen, self.main_framebuffer, ZONE_B_RECT, (450, preview_panel_y + 115), "Zone B", self.small_font)
            draw_ocr_preview(self.screen, self.main_framebuffer, ZONE_C_RECT, (700, preview_panel_y + 30), "Zone C", self.small_font)
            draw_ocr_preview(self.screen, self.main_framebuffer, ZONE_D_RECT, (700, preview_panel_y + 115), "Zone D", self.small_font)
            draw_ocr_preview(self.screen, self.main_framebuffer, ZONE_E_RECT, (700, preview_panel_y + 180), "Zone E", self.small_font)
            draw_area_highlight_preview(self.screen, SPECTRUM_AREA_RECT, (850, preview_panel_y + 30), "Spectrum Area", self.small_font)
        elif self.app_state == 'REPLAYING':
             # Optionally show placeholder text in preview area during replay
            replay_preview_surf = self.font.render("OCR Preview disabled during Replay", True, (150, 150, 150))
            self.screen.blit(replay_preview_surf, replay_preview_surf.get_rect(center=preview_panel_rect.center))


    def _render_waterfall_markers(self):
        if not self.waterfall_markers or self.waterfall_area.height <= 0:
            return
        
        line_height = self.waterfall_area.height / self.waterfall_len
        
        for marker in self.waterfall_markers:
             # Calculate current Y based on offset (could be positive or negative)
            current_y_idx = marker['initial_y_idx'] + marker['y_offset_updates']
            
             # Only draw if currently within the visible waterfall area
            if 0 <= current_y_idx < self.waterfall_len:
                x_pos = self.waterfall_area.left + (marker['x_ratio'] * self.waterfall_area.width)
                y_pos = self.waterfall_area.top + (current_y_idx * line_height)
                
                # Draw crosshair
                pygame.draw.line(self.screen, (255, 255, 0), (x_pos - 4, y_pos), (x_pos + 4, y_pos), 1)
                pygame.draw.line(self.screen, (255, 255, 0), (x_pos, y_pos - 4), (x_pos, y_pos + 4), 1)


    def _render_and_handle_poi_markers(self):
        if not self.replay_pois or self.waterfall_area.height <= 0 or not self.view_freq_range:
            self.hovered_poi_info = None
            return

        line_height = self.waterfall_area.height / self.waterfall_len
        mouse_pos = pygame.mouse.get_pos()
        found_hover = False

        for poi in self.replay_pois:
            # Check if POI is in the current time view (Y axis)
            y_offset = self.replay_frame_index - poi['frame_index']
            if not (0 <= y_offset < self.waterfall_len):
                continue

            # Check if POI is in the current frequency view (X axis)
            view_start_f, view_end_f = self.view_freq_range
            view_span = view_end_f - view_start_f
            if not (view_start_f <= poi['freq'] <= view_end_f) or view_span <= 0:
                continue
            
            # It's visible, so calculate its screen position
            x_ratio = (poi['freq'] - view_start_f) / view_span
            x_pos = self.waterfall_area.left + (x_ratio * self.waterfall_area.width)
            y_pos = self.waterfall_area.top + (y_offset * line_height)

            # Draw the marker (e.g., a magenta cross)
            marker_color = (255, 0, 255)
            pygame.draw.line(self.screen, marker_color, (x_pos - 5, y_pos), (x_pos + 5, y_pos), 1)
            pygame.draw.line(self.screen, marker_color, (x_pos, y_pos - 5), (x_pos, y_pos + 5), 1)


            # Check for hover
            marker_hover_rect = pygame.Rect(x_pos - 5, y_pos - 5, 10, 10)
            if not found_hover and marker_hover_rect.collidepoint(mouse_pos):
                found_hover = True
                
                # Construct hover info box
                freq_text = f"{poi['freq']:.5f} MHz"
                time_text = poi['timestamp'] # Already formatted string
                desc_text = poi['desc'] if poi['desc'] else "(No description)" # Handle empty description
                
                freq_surf = self.tiny_font.render(freq_text, True, (255, 255, 255))
                time_surf = self.tiny_font.render(time_text, True, (220, 220, 220))
                desc_surf = self.tiny_font.render(desc_text, True, (200, 200, 200))

                surfaces = [freq_surf, time_surf, desc_surf]
                max_width = max(s.get_width() for s in surfaces) if surfaces else 0
                flip_to_left = (mouse_pos[0] + 15 + max_width > self.screen.get_width())

                drawable_items = []
                all_rects = []
                current_y = mouse_pos[1] - 8 # Start slightly above cursor
                
                line_height_render = 14 # Spacing between lines in hover box
                for surf in surfaces:
                    rect = surf.get_rect(top=current_y) # Align top instead of center
                    if flip_to_left:
                        rect.right = mouse_pos[0] - 15
                    else:
                        rect.left = mouse_pos[0] + 15
                    drawable_items.append((surf, rect))
                    all_rects.append(rect)
                    current_y += line_height_render
                
                # Create background rect based on collected item rects
                if all_rects:
                     bg_rect = all_rects[0].unionall(all_rects).inflate(8, 6)
                     self.hovered_poi_info = (bg_rect, drawable_items)
                else: # Should not happen if surfaces exist, but safe fallback
                     self.hovered_poi_info = None


        if not found_hover:
            self.hovered_poi_info = None

    def _render_modal(self):
        # Draw semi-transparent overlay
        overlay = pygame.Surface(self.screen.get_size(), pygame.SRCALPHA)
        overlay.fill((0, 0, 0, 180))
        self.screen.blit(overlay, (0, 0))

        # Center and draw modal box
        self.modal_rect.center = self.screen.get_rect().center
        pygame.draw.rect(self.screen, (50, 50, 60), self.modal_rect)
        pygame.draw.rect(self.screen, (100, 100, 120), self.modal_rect, 2)
        
        # Draw title
        title_surf = self.font.render("Save Point of Interest", True, (220, 220, 220))
        self.screen.blit(title_surf, (self.modal_rect.x + 10, self.modal_rect.y + 10))

        # Draw text input box
        input_rect = pygame.Rect(self.modal_rect.x + 10, self.modal_rect.y + 40, self.modal_rect.width - 20, 35)
        pygame.draw.rect(self.screen, (20, 20, 20), input_rect)
        pygame.draw.rect(self.screen, (150, 150, 150), input_rect, 1)

        # --- START OF FIXES ---

        # 1. Set the correct text color
        text_surf = self.font.render(self.modal_input_text, True, (255, 255, 255))
        
        # 2. Define the destination area with padding
        text_render_rect = input_rect.inflate(-10, -10) # 5px padding
        
        text_width = text_surf.get_width()
        text_height = text_surf.get_height()
        
        # 3. Calculate text overflow and cursor position
        overflow = text_width - text_render_rect.width
        
        if overflow > 0:
            # Text is wider than the box, so we must clip it
            # We clip from the *source* surface, starting from the right
            clip_x_start = overflow
            clip_width = text_render_rect.width
            blit_area = pygame.Rect(clip_x_start, 0, clip_width, text_height)
            
            # Blit the *clipped area* to the *destination rect's topleft*
            self.screen.blit(text_surf, text_render_rect.topleft, blit_area)
            
            # Cursor should be at the far right of the box
            cursor_x = text_render_rect.right
        else:
            # Text fits, blit normally (no 3rd 'area' argument)
            self.screen.blit(text_surf, text_render_rect.topleft)
            
            # Cursor should be at the end of the text
            cursor_x = text_render_rect.left + text_width

        # 4. Draw blinking cursor at the correct position
        if time.time() % 1 > 0.5:
             # Constrain cursor X to be inside the visible text area
             cursor_x = max(text_render_rect.left, min(cursor_x, text_render_rect.right))
             pygame.draw.line(self.screen, (220, 220, 220), (cursor_x, text_render_rect.top), (cursor_x, text_render_rect.bottom), 1)

        # --- END OF FIXES ---
            
        # Position and draw buttons
        self.modal_cancel_btn.bottomright = (self.modal_rect.right - 10, self.modal_rect.bottom - 10)
        self.modal_save_btn.bottomright = (self.modal_cancel_btn.left - 10, self.modal_rect.bottom - 10)
        
        pygame.draw.rect(self.screen, (150, 80, 80), self.modal_cancel_btn)
        pygame.draw.rect(self.screen, (80, 150, 80), self.modal_save_btn)

        cancel_surf = self.font.render("Cancel", True, (255, 255, 255))
        save_surf = self.font.render("Save", True, (255, 255, 255))
        self.screen.blit(cancel_surf, cancel_surf.get_rect(center=self.modal_cancel_btn.center))
        self.screen.blit(save_surf, save_surf.get_rect(center=self.modal_save_btn.center))
    
    # --- Action and Helper Methods below this line ---

    def _save_point_of_interest(self):
        if not self.modal_data_to_save or self.modal_data_to_save.get('recording_id') is None:
             print("[Warn] Cannot save POI - no valid recording context.")
             self.modal_data_to_save = None # Clear invalid data
             self.pending_marker = None
             return
        
        data = self.modal_data_to_save
        description = self.modal_input_text.strip() # Use the entered text

        try:
            conn = sqlite3.connect(DB_FILE)
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO points_of_interest (recording_id, frequency_mhz, description, absolute_timestamp)
                VALUES (?, ?, ?, ?)
            """, (data['recording_id'], data['frequency_mhz'], description, data['absolute_timestamp']))
            conn.commit()
            conn.close()
            print(f"[DB] Saved point of interest at {data['frequency_mhz']:.4f} MHz with desc: '{description}'.")
            
            # --- THIS IS THE FIX ---
            # Only add the temporary (non-hover) marker if we are in LIVE mode.
            # In REPLAY mode, the _load_pois_for_session call below
            # will add the marker from the DB, so we don't add this one.
            if self.pending_marker and self.app_state == 'LIVE':
                self.waterfall_markers.append(self.pending_marker)
            # --- END FIX ---
            
            # If in replay mode, reload POIs to include the new one immediately
            if self.app_state == 'REPLAYING':
                 self._load_pois_for_session(self.current_session_identifier)


        except sqlite3.Error as e:
            print(f"[DB Error] Error saving point of interest: {e}")
        
        # Clear modal state regardless of success/failure
        self.modal_data_to_save = None
        self.pending_marker = None
        self.modal_input_text = "" # Clear input field


    def _load_pois_for_session(self, session_identifier):
        self.replay_pois = [] # Clear existing list
        try:
            conn = sqlite3.connect(DB_FILE)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()

            # First, get an ordered list of all recording IDs FOR THIS SESSION
            # to determine frame indices relative to the start of the session.
            cursor.execute("""
                SELECT r.id FROM recordings r
                JOIN sessions s ON r.session_id = s.id
                WHERE s.identifier = ?
                ORDER BY r.timestamp ASC
            """, (session_identifier,))
            # Create a mapping from recording_id to its 0-based index within the session
            session_recording_ids = {row['id']: index for index, row in enumerate(cursor.fetchall())}

            if not session_recording_ids:
                 print(f"[DB] No recordings found for session {session_identifier} to load POIs.")
                 conn.close()
                 return


            # Now, fetch the POIs associated with recordings in THIS session
            cursor.execute("""
                SELECT p.recording_id, p.frequency_mhz, p.description, p.absolute_timestamp
                FROM points_of_interest p
                JOIN recordings r ON p.recording_id = r.id
                JOIN sessions s ON r.session_id = s.id
                WHERE s.identifier = ?
            """, (session_identifier,))
            
            pois_from_db = cursor.fetchall()
            loaded_count = 0
            for poi_row in pois_from_db:
                 recording_id = poi_row['recording_id']
                 # Look up the frame index using the map created earlier
                 frame_index = session_recording_ids.get(recording_id)

                 if frame_index is not None: # Check if the recording ID belongs to this session
                     self.replay_pois.append({
                         'freq': poi_row['frequency_mhz'],
                         'desc': poi_row['description'],
                         'timestamp': poi_row['absolute_timestamp'], # Store the absolute timestamp string
                         'frame_index': frame_index # Store the relative frame index
                     })
                     loaded_count += 1
                 else:
                      print(f"[Warn] POI found linked to recording ID {recording_id}, but that ID doesn't belong to session {session_identifier}. Skipping.")


            conn.close()
            print(f"[REPLAY] Loaded {loaded_count} points of interest for session {session_identifier}.")
        except sqlite3.Error as e:
            print(f"[DB Error] Error loading points of interest: {e}")
            if conn: conn.close() # Ensure connection is closed on error


    def _start_replay_session(self, identifier, start_time):
        self.current_session_identifier = identifier
        self.total_frames_in_session = get_session_frame_count(identifier)
        if self.total_frames_in_session <= 0:
            print(f"[Replay] Session {identifier} has 0 frames. Cannot replay.")
            return

        print(f"[Replay] Starting session: {identifier} ({self.total_frames_in_session} frames)")

        # --- START OF NEW FEATURE ---
        # Run a fast query to get the energy map for the *entire* session
        self.replay_energy_map = []
        self.replay_max_energy = 0
        try:
            conn = sqlite3.connect(DB_FILE)
            cursor = conn.cursor()
            cursor.execute("""
                SELECT r.spectrum_sum 
                FROM recordings as r
                JOIN sessions as s ON r.session_id = s.id
                WHERE s.identifier = ?
                ORDER BY r.timestamp ASC
            """, (identifier,))
            # Fetch all results, un-tuple them, and handle None values
            self.replay_energy_map = [row[0] if row[0] is not None else 0 for row in cursor.fetchall()]
            conn.close()
            
            if self.replay_energy_map:
                self.replay_max_energy = max(self.replay_energy_map)
            print(f"[Replay] Loaded energy map ({len(self.replay_energy_map)} points, max: {self.replay_max_energy}).")
        except sqlite3.Error as e:
            print(f"[DB Error] Could not load energy map: {e}")
        # --- END OF NEW FEATURE ---
        
        self.spectrum_mode_active = True
        self.waterfall_updates_since_start = 0 
        self.waterfall_markers.clear()
        self.replay_pois = []
        
        try:
            self.replay_session_start_dt = datetime.datetime.fromisoformat(start_time)
        except (ValueError, TypeError):
             print(f"[Warn] Could not parse session start time: {start_time}")
             self.replay_session_start_dt = None
        
        initial_chunk = get_session_data_chunk(self.current_session_identifier, 0, self.REPLAY_FETCH_CHUNK_SIZE)
        if not initial_chunk:
             print(f"[Replay Error] Failed to fetch initial data for session {identifier}.")
             return
        
        self._load_pois_for_session(identifier)
        
        first_frame = initial_chunk[0]
        self._update_display_from_replay_frame(first_frame) 

        self.replay_buffer = deque(initial_chunk)
        self.replay_buffer_start_index = 0
        self.waterfall_data.clear()
        for _ in range(self.waterfall_len): self.waterfall_data.append(self.empty_scan)
        
        self.slider_seek.max_val = max(0, self.total_frames_in_session - 1)
        self.slider_seek.val = 0
        self.replay_frame_index = 0
        self.replay_start_time = time.monotonic()
        self.replay_is_paused = False 
        
        self.app_state = 'REPLAYING'


    def _disconnect_serial(self):
          print("[Serial] Disconnecting...")
          
          # Stop recording first if active
          if self.is_recording:
                self.is_recording = False # Set flag first
                
                # Signal the writer thread to stop.
                if self.db_stop_event:
                    print("[REC] Recording stopped due to disconnect. Signaling writer thread...")
                    self.db_stop_event.set()
                    # We do not .join() here. We want the GUI to be
                    # responsive immediately. The thread will stop
                    # gracefully in the background.
                
                # Clear all recording-thread-related variables
                self.db_write_queue, self.db_stop_event, self.db_writer_thread = None, None, None
                self.session_id_pk = None

          # Stop the reader thread and close the port
          if self.ser:
                if self.stop_event: 
                    self.stop_event.set()
                if self.reader_thread and self.reader_thread.is_alive():
                    self.reader_thread.join(timeout=1) # Wait briefly for thread exit
                try:
                    self.ser.close()
                except serial.SerialException as e:
                     print(f"[Serial Error] Error closing port: {e}")

          # Reset serial related state variables
          self.ser, self.data_queue, self.stop_event, self.reader_thread = None, None, None, None
          
          # Reset other relevant state
          self.app_state = 'CONNECTION_MENU'
          self.last_frame_time = None
          self.current_frame_latency = 0.0
          self.waterfall_markers.clear() # Clear temporary markers
          self.spectrum_mode_active = False # Assume inactive when disconnected
          self.was_spectrum_mode_active = False

        
    def _toggle_play_pause(self):
        if self.app_state != 'REPLAYING': return # Only works in replay

        if self.replay_is_paused and self.replay_frame_index >= self.total_frames_in_session - 1:
            # Restart playback if paused at the very end
            print("[Replay] Restarting from beginning.")
            self._seek_replay_to(0) # Use seek function to handle buffer reload
            self.replay_is_paused = False # Start playing after seek
        else:
            # Toggle pause state
            self.replay_is_paused = not self.replay_is_paused
            if self.replay_is_paused:
                self.time_when_paused = time.monotonic()
                print("[Replay] Paused.")
            else:
                # Adjust start time to account for paused duration
                if self.time_when_paused > 0: # Ensure it was actually paused
                    paused_duration = time.monotonic() - self.time_when_paused
                    self.replay_start_time += paused_duration
                    self.time_when_paused = 0 # Reset pause timer
                print("[Replay] Resumed.")

    
    def _return_to_live_or_connection(self):
        print("[State] Returning to Live/Connection Screen...")
        # Clear replay-specific state first
        self.current_session_identifier = None
        self.replay_buffer.clear()
        self.replay_buffer_start_index = 0
        self.replay_frame_index = 0
        self.replay_pois.clear()
        self.waterfall_markers.clear() # Clear temp markers too

        if self.ser: # If serial connection is still active
            # Always clear the UI waterfall deque
            self.waterfall_data.clear()
            
            # If a recording session is ACTUALLY running in the background
            if self.is_recording and self.session_id_pk is not None:
                print("[State] Repopulating waterfall from active recording...")
                try:
                    with sqlite3.connect(DB_FILE) as temp_conn:
                        temp_conn.row_factory = sqlite3.Row
                        temp_cursor = temp_conn.cursor()
                        # Fetch latest frames from the *current* background recording session
                        temp_cursor.execute("SELECT spectrum_data FROM recordings WHERE session_id = ? ORDER BY timestamp DESC LIMIT ?", (self.session_id_pk, self.waterfall_len))
                        rows = temp_cursor.fetchall()
                        rows.reverse() # Need oldest first for appendleft correct order
                        for row in rows:
                            try:
                                self.waterfall_data.appendleft(json.loads(row['spectrum_data']))
                            except json.JSONDecodeError:
                                self.waterfall_data.appendleft(self.empty_scan) # Add empty on error
                except sqlite3.Error as e:
                     print(f"[DB Error] Failed to repopulate waterfall: {e}")

            # Always pad the waterfall to its full length after clearing/repopulating
            while len(self.waterfall_data) < self.waterfall_len:
                self.waterfall_data.append(self.empty_scan) # Append (adds to right, appears at bottom)

            # Update OCR variables from the latest main_framebuffer
            # This might be slightly behind if frames arrived while switching
            if self.main_framebuffer:
                 self._update_ocr_variables()
            
            # Reset UI waterfall update timer to force immediate refresh
            self.last_waterfall_update_time = 0
            # Reset waterfall update counter? Maybe based on DB count if repopulated?
            # Let's reset it for simplicity, markers might be slightly off initially.
            self.waterfall_updates_since_start = 0

            self.app_state = 'LIVE'
            print("[State] Switched to LIVE mode.")

        else: # No serial connection
            self.app_state = 'CONNECTION_MENU'
            # Ensure waterfall is cleared if returning to connection menu
            self.waterfall_data.clear()
            while len(self.waterfall_data) < self.waterfall_len:
                 self.waterfall_data.append(self.empty_scan)
            print("[State] Switched to CONNECTION_MENU mode.")


    def _seek_replay_to(self, new_index):
        if self.app_state != 'REPLAYING': return
        
        # Clamp index to valid range
        new_index = max(0, min(new_index, self.total_frames_in_session - 1))

        print(f"[Replay] Seeking to frame index: {new_index}")
        self.replay_frame_index = new_index
        self.slider_seek.val = new_index # Update slider UI value

        # Determine the ideal start of the buffer for the new index
        # Aim to have the new index somewhere in the middle of the *fetched chunk*.
        
        # --- THIS IS THE FIX ---
        # Was: self.REPLAY_BUFFER_SIZE // 2
        # Is:  self.REPLAY_FETCH_CHUNK_SIZE // 2
        buffer_start = max(0, self.replay_frame_index - (self.REPLAY_FETCH_CHUNK_SIZE // 2))
        # --- END FIX ---
        
        # Ensure buffer start doesn't go past the end
        # (This logic is fine, but was being fed a bad buffer_start)
        latest_possible_start_index = max(0, self.total_frames_in_session - self.REPLAY_FETCH_CHUNK_SIZE)
        buffer_start = min(buffer_start, latest_possible_start_index)


        print(f"[Replay] Reloading buffer starting from index: {buffer_start}")
        new_chunk = get_session_data_chunk(self.current_session_identifier, buffer_start, self.REPLAY_FETCH_CHUNK_SIZE)
        
        if not new_chunk:
             print("[Replay Error] Failed to fetch data chunk during seek.")
             # Keep old buffer? Or clear? Let's clear and show empty.
             self.replay_buffer.clear()
             self.replay_buffer_start_index = new_index # Or 0?
             self.waterfall_data.clear()
             while len(self.waterfall_data) < self.waterfall_len: self.waterfall_data.append(self.empty_scan)
             return

        self.replay_buffer = deque(new_chunk)
        self.replay_buffer_start_index = buffer_start
        
        # --- Repopulate waterfall UI based on new position ---
        self.waterfall_data.clear()
        # Iterate backwards from the current frame index to fill the deque
        for i in range(self.waterfall_len):
            frame_idx_to_fetch = self.replay_frame_index - i
            if frame_idx_to_fetch < 0: # Stop if we go before the start
                 break
                 
            # Try to get frame from buffer first
            frame_data = self._get_replay_frame_by_index(frame_idx_to_fetch)
            
            if frame_data and 'spectrum_data' in frame_data:
                 try:
                     self.waterfall_data.append(json.loads(frame_data['spectrum_data'])) # Append adds to right (bottom of waterfall)
                 except json.JSONDecodeError:
                     self.waterfall_data.append(self.empty_scan)
            else:
                 self.waterfall_data.append(self.empty_scan) # Pad with empty if frame missing


        # Ensure deque has the correct length, padding with empty if needed
        while len(self.waterfall_data) < self.waterfall_len:
             self.waterfall_data.append(self.empty_scan) # Add empty to the left (top)


        # Update display variables and reset timer based on the new current frame
        current_frame_data = self._get_current_replay_frame()
        if current_frame_data:
            self._update_display_from_replay_frame(current_frame_data)
            # Reset the playback timer based on the timestamp of the frame we landed on
            self.replay_start_time = time.monotonic() - (current_frame_data['timestamp'] / self.replay_speed_multiplier)
            self.time_when_paused = 0 # Ensure not considered paused after seek
        else:
             print("[Replay Warn] Could not get current frame data after seek.")


    def _get_current_replay_frame(self):
        return self._get_replay_frame_by_index(self.replay_frame_index)

    def _get_replay_frame_by_index(self, index):
        """Gets frame data from the buffer if available, returns None otherwise."""
        if not self.replay_buffer: return None # Handle empty buffer case

        if self.replay_buffer_start_index <= index < self.replay_buffer_start_index + len(self.replay_buffer):
            buffer_idx = index - self.replay_buffer_start_index
            try:
                return self.replay_buffer[buffer_idx]
            except IndexError:
                 # This might happen if index calculation is off, or buffer changed unexpectedly
                 print(f"[Warn] Replay buffer index {buffer_idx} out of range (buffer len {len(self.replay_buffer)}, start {self.replay_buffer_start_index}, requested index {index}).")
                 return None

        # print(f"[Debug] Frame index {index} not in current buffer (start {self.replay_buffer_start_index}, len {len(self.replay_buffer)}).")
        return None # Frame not in the current buffer segment


    def _update_display_from_replay_frame(self, frame_data):
        """Updates OCR text variables from a loaded replay frame dictionary."""
        # Update class attributes using data from the dictionary
        self.center_freq_text = frame_data.get('center_freq', "N/A")
        self.start_freq_text = frame_data.get('start_freq', "N/A")
        self.end_freq_text = frame_data.get('end_freq', "N/A")
        self.impedance_low_text = frame_data.get('impedance_low', "N/A")
        self.impedance_high_text = frame_data.get('impedance_high', "N/A")
        self.bars_text = frame_data.get('bars', "N/A")
        self.step_text = frame_data.get('step', "N/A")
        self.mod_text = frame_data.get('modulation', "N/A")
        self.bw_text = frame_data.get('bandwidth', "N/A")
        
    def _handle_keyboard_shortcuts(self, event):
        keys = pygame.key.get_pressed()
        is_ctrl = keys[pygame.K_LCTRL] or keys[pygame.K_RCTRL]
        is_shift = keys[pygame.K_LSHIFT] or keys[pygame.K_RSHIFT]


        # General Quit
        if event.key == pygame.K_q: self.running = False
        
        # --- Display Options ---
        if event.key == pygame.K_p: self.pixel_lcd = 1 - self.pixel_lcd
        if event.key == pygame.K_i: self.bg_color, self.fg_color = (self.fg_color, self.bg_color)
        if event.unicode in COLOR_SETS: self.fg_color, self.bg_color = COLOR_SETS[event.unicode][1:]
        if event.key == pygame.K_UP and not is_shift: # Increase pixel size (no shift)
            if self.pixel_size < 12: self.pixel_size += 1; self._resize_window()
        if event.key == pygame.K_DOWN and not is_shift: # Decrease pixel size (no shift)
            if self.pixel_size > 3: self.pixel_size -= 1; self._resize_window()


        # --- Waterfall / Replay Controls (only if spectrum panel is relevant) ---
        is_spectrum_visible = self.app_state == 'LIVE' or self.app_state == 'REPLAYING'
        if not is_spectrum_visible: return # Don't process below shortcuts if not relevant


        # Waterfall History Length (Ctrl + Z/X)
        if is_ctrl and event.key in (pygame.K_z, pygame.K_x):
            self._adjust_waterfall_history(event.key)
            return

        # Waterfall Zoom (Z/X without Ctrl) - requires valid frequency range
        if not is_ctrl and event.key in (pygame.K_z, pygame.K_x) and self.view_freq_range:
            self._zoom_waterfall(event.key)


        # Calibration Offset (Arrow Keys without Shift)
        if event.key == pygame.K_LEFT and not is_shift: self.calibration_pixel_offset -= 1
        if event.key == pygame.K_RIGHT and not is_shift: self.calibration_pixel_offset += 1
            
        # Replay Seek (Shift + Arrow Keys) - only in Replay mode
        if self.app_state == 'REPLAYING' and is_shift:
             seek_amount = 1 # Default seek by 1 frame
             if keys[pygame.K_LCTRL] or keys[pygame.K_RCTRL]: seek_amount = 100 # Ctrl+Shift seeks faster
             
             if event.key == pygame.K_LEFT:
                 self._seek_replay_to(self.replay_frame_index - seek_amount)
             if event.key == pygame.K_RIGHT:
                 self._seek_replay_to(self.replay_frame_index + seek_amount)


        # Replay Play/Pause (Space Bar) - only in Replay mode
        if self.app_state == 'REPLAYING' and event.key == pygame.K_SPACE:
             self._toggle_play_pause()


    def _adjust_waterfall_history(self, key):
        old_len = self.waterfall_len
        new_len = self.waterfall_len
        
        # Use large step with Ctrl, small step with Ctrl+Shift
        is_shift = pygame.key.get_pressed()[pygame.K_LSHIFT] or pygame.key.get_pressed()[pygame.K_RSHIFT]
        step = 1 if is_shift else 10 # 10 for normal, 1 for fine-tune

        if key == pygame.K_z: new_len = max(10, old_len - step)
        elif key == pygame.K_x: new_len = min(500, old_len + step) # Allow larger max history
        
        if new_len == old_len:
            return # No change needed

        print(f"[UI] Rebuilding waterfall history from {old_len} to {new_len}")
        self.waterfall_len = new_len
        
        # Create a new deque with the new max length
        new_deque = deque(maxlen=new_len)
        
        # --- Repopulate logic ---
        
        if self.app_state == 'LIVE' and self.is_recording and self.session_id_pk is not None:
            # --- This logic is copied from _return_to_live_or_connection ---
            # It reloads the waterfall cleanly from the database.
            print(f"[UI] Fetching {new_len} latest frames from DB...")
            try:
                with sqlite3.connect(DB_FILE) as temp_conn:
                    temp_conn.row_factory = sqlite3.Row
                    temp_cursor = temp_conn.cursor()
                    # Fetch latest 'new_len' frames
                    temp_cursor.execute("SELECT spectrum_data FROM recordings WHERE session_id = ? ORDER BY timestamp DESC LIMIT ?", 
                                        (self.session_id_pk, new_len))
                    rows = temp_cursor.fetchall()
                    rows.reverse() # Oldest-fetched (N-newest) to Newest
                    
                    # We use appendleft() to add data so that data[0] is the newest.
                    for row in rows:
                        try:
                            new_deque.appendleft(json.loads(row['spectrum_data']))
                        except json.JSONDecodeError:
                            new_deque.appendleft(self.empty_scan)
            except sqlite3.Error as e:
                print(f"[DB Error] Failed to repopulate waterfall: {e}")
        
        elif self.app_state == 'REPLAYING':
            # --- This logic is copied from _seek_replay_to ---
            # It reloads the waterfall cleanly from the replay buffer.
            print(f"[UI] Fetching {new_len} latest frames from Replay buffer...")
            temp_lines = []
            for i in range(new_len):
                frame_idx_to_fetch = self.replay_frame_index - i
                if frame_idx_to_fetch < 0:
                    break # Stop if we go before the start
                
                frame_data = self._get_replay_frame_by_index(frame_idx_to_fetch)
                
                if frame_data and 'spectrum_data' in frame_data:
                    try:
                        temp_lines.append(json.loads(frame_data['spectrum_data']))
                    except json.JSONDecodeError:
                        temp_lines.append(self.empty_scan)
                else:
                    temp_lines.append(self.empty_scan)
            
            # temp_lines is now [Newest, Older, Oldest]
            # .extend() adds to the right, so new_deque[0] = Newest
            new_deque.extend(temp_lines) 
        
        # --- End Repopulate ---

        # Finally, pad the *bottom* (right side) with empty scans
        while len(new_deque) < new_len:
            new_deque.append(self.empty_scan)
        
        # Replace the old deque
        self.waterfall_data = new_deque
        
        # Reset the UI update counter. This is important for 
        # temporary markers ('pending_marker') to be placed correctly.
        self.waterfall_updates_since_start = 0


    def _zoom_waterfall(self, key):
        mouse_pos = pygame.mouse.get_pos()
         # Check if mouse is over the waterfall area specifically
        if not self.waterfall_area.collidepoint(mouse_pos) or not self.view_freq_range or not self.full_freq_range:
             print("[Zoom] Zoom action requires mouse over waterfall and valid frequency ranges.")
             return # Cannot zoom if ranges invalid or mouse outside
        
        view_start_f, view_end_f = self.view_freq_range
        current_span = view_end_f - view_start_f
        
         # Avoid zooming further if span is already tiny or huge
        full_start_f, full_end_f = self.full_freq_range
        total_span = full_end_f - full_start_f
        MIN_SPAN = 0.001 # Minimum practical span in MHz


        mouse_ratio = (mouse_pos[0] - self.waterfall_area.left) / self.waterfall_area.width if self.waterfall_area.width > 0 else 0.5
        mouse_freq = view_start_f + (mouse_ratio * current_span)
        
        zoom_factor = 0.8 if key == pygame.K_z else 1.25 # z = zoom in, x = zoom out

        if key == pygame.K_z and current_span * zoom_factor < MIN_SPAN:
            print("[Zoom] Minimum span reached.")
            new_span = MIN_SPAN # Clamp to min span
        elif key == pygame.K_x and current_span * zoom_factor > total_span:
             print("[Zoom] Maximum span reached.")
             self.view_freq_range = self.full_freq_range # Reset to full view
             return
        else:
             new_span = current_span * zoom_factor

        
        # Calculate new start/end based on mouse position as pivot
        new_start_f = mouse_freq - (new_span * mouse_ratio)
        new_end_f = new_start_f + new_span
        
        # Clamp the new range to the full available range
        new_start_f = max(full_start_f, new_start_f)
        new_end_f = min(full_end_f, new_end_f)

         # Recalculate span after clamping edges
        final_span = new_end_f - new_start_f
        # Adjust the opposite edge if clamping shortened the span significantly
        if abs(final_span - new_span) > 0.0001: # Tolerance for float precision
            if new_start_f == full_start_f: # Clamped at start
                new_end_f = new_start_f + new_span
            elif new_end_f == full_end_f: # Clamped at end
                 new_start_f = new_end_f - new_span
            # Re-clamp after adjustment (shouldn't be necessary if logic is right, but safe)
            new_start_f = max(full_start_f, new_start_f)
            new_end_f = min(full_end_f, new_end_f)


        print(f"[Zoom] New view range: {new_start_f:.4f} - {new_end_f:.4f} MHz")
        self.view_freq_range = (new_start_f, new_end_f)


    def _execute_auto_align(self):
        if not self.spectrum_mode_active or not self.full_freq_range or not self.view_freq_range:
            print("[Align] Cannot auto-align: Spectrum not active or frequency ranges unknown.")
            self.auto_align_triggered = False
            return

        self.auto_align_triggered = False # Reset trigger immediately
        print("[Align] Attempting auto-align...")

        try:
             # Use the most recent framebuffer data
            latest_scan = get_spectrum_data(self.main_framebuffer)
            if not latest_scan or max(latest_scan) == 0:
                 print("[Align] No signal detected in latest scan.")
                 return

            peak_value = max(latest_scan)
             # Find *first* occurrence of peak value
            peak_index = latest_scan.index(peak_value)
            
            full_start_f, full_end_f = self.full_freq_range
            full_span = full_end_f - full_start_f
            num_points = len(latest_scan)

            if num_points <= 0 or full_span <= 0:
                 print("[Align] Invalid scan data or frequency span.")
                 return

             # Calculate frequency at the center of the peak bin
            peak_freq = full_start_f + ((peak_index + 0.5) / num_points) * full_span
            
            # Get target frequency from OCR (might have '?' or spaces)
            target_freq_str = self.center_freq_text.replace('?', '').strip()
            if not target_freq_str:
                 print("[Align] Could not read target center frequency from OCR.")
                 return
                 
            target_freq = float(target_freq_str)
            
            freq_diff = target_freq - peak_freq
            
            view_start_f, view_end_f = self.view_freq_range
            view_span = view_end_f - view_start_f

            if view_span <= 0 or self.waterfall_area.width <= 0:
                 print("[Align] Invalid view span or waterfall width.")
                 return

            pixels_per_mhz = self.waterfall_area.width / view_span
            pixel_shift = freq_diff * pixels_per_mhz
            
            # Apply reasonable limits to the offset?
            max_offset = self.waterfall_area.width // 2
            final_offset = int(round(max( -max_offset, min(pixel_shift, max_offset))))

            print(f"[Align] Peak @ {peak_freq:.4f}, Target @ {target_freq:.4f}, Diff: {freq_diff:.4f} MHz, Pixel Shift: {pixel_shift:.1f} -> Offset: {final_offset}px")
            self.calibration_pixel_offset = final_offset

        except (ValueError, TypeError, ZeroDivisionError, AttributeError, IndexError) as e:
            print(f"[Align Error] Auto-align failed: {e}. Check signal and OCR.")
            # Optionally reset offset on failure?
            # self.calibration_pixel_offset = 0


    def _handle_waterfall_click(self, pos):
        # Ensure we have valid ranges to work with
        if not self.view_freq_range or self.waterfall_area.width <= 0 or self.waterfall_area.height <= 0: return

        relative_x = pos[0] - self.waterfall_area.left
        relative_y = pos[1] - self.waterfall_area.top
        
        # Calculate frequency from X position based on current VIEW range
        x_ratio = relative_x / self.waterfall_area.width
        view_start_f, view_end_f = self.view_freq_range
        freq_span = view_end_f - view_start_f
        mouse_freq = view_start_f + (x_ratio * freq_span)
        freq_text = f"{mouse_freq:.5f} MHz"
        
        # Calculate time index (Y) relative to the top of the waterfall
        line_height = self.waterfall_area.height / self.waterfall_len
        # Clamp y_idx to valid range [0, waterfall_len - 1]
        y_idx = max(0, min(int(relative_y / line_height), self.waterfall_len - 1))

        
        timestamp_str, info1_str, info2_str = "N/A", "N/A", "N/A"
        historical_data_dict = None # Use a dictionary to store fetched data
        recording_pk = None # The primary key of the recording table entry
        absolute_timestamp_obj = None # Store as datetime object if possible

        # --- Fetch Historical Data based on Mode ---
        if self.app_state == 'REPLAYING':
             # Calculate the absolute frame index corresponding to the click
            clicked_frame_index = self.replay_frame_index - y_idx
            # Get data from buffer/DB for that index
            frame_data = self._get_replay_frame_by_index(clicked_frame_index)
            if frame_data and self.replay_session_start_dt:
                try:
                    frame_timestamp_sec = frame_data['timestamp']
                    absolute_timestamp_obj = self.replay_session_start_dt + datetime.timedelta(seconds=frame_timestamp_sec)
                    timestamp_str = absolute_timestamp_obj.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3] # With ms
                    historical_data_dict = frame_data # Store the whole dict
                    recording_pk = frame_data['id']
                except (TypeError, ValueError) as e:
                     print(f"[Warn] Error calculating replay timestamp: {e}")
                     timestamp_str = f"Rel: {frame_data.get('timestamp', '?'):.3f}s" # Fallback relative time

        elif self.app_state == 'LIVE' and self.is_recording and self.session_id_pk is not None:
             # Fetch the specific historical frame from the DB based on offset from latest
             try:
                 with sqlite3.connect(DB_FILE) as temp_conn:
                     temp_conn.row_factory = sqlite3.Row
                     temp_cursor = temp_conn.cursor()
                     # Fetch the specific frame using LIMIT 1 OFFSET y_idx
                     temp_cursor.execute("""SELECT r.*, s.start_time
                                           FROM recordings r JOIN sessions s ON r.session_id = s.id
                                           WHERE r.session_id = ? 
                                           ORDER BY r.timestamp DESC 
                                           LIMIT 1 OFFSET ?""", 
                                         (self.session_id_pk, y_idx))
                     row = temp_cursor.fetchone()
                     if row:
                         historical_data_dict = dict(row) # Convert Row to dict
                         recording_pk = historical_data_dict['id']
                         try:
                             start_dt = datetime.datetime.fromisoformat(historical_data_dict['start_time'])
                             time_offset = datetime.timedelta(seconds=historical_data_dict['timestamp'])
                             absolute_timestamp_obj = start_dt + time_offset
                             timestamp_str = absolute_timestamp_obj.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
                         except (ValueError, TypeError):
                             # Fallback relative time if absolute fails
                             minutes, seconds = divmod(historical_data_dict['timestamp'], 60)
                             timestamp_str = f"T+{int(minutes):02}:{seconds:06.3f}"
             except sqlite3.Error as e:
                 print(f"[DB Error] Failed fetching historical frame for click: {e}")

        
        # If no historical data could be fetched, cannot create info box or marker
        if not historical_data_dict:
            print("[Warn] No historical data found for clicked point.")
            self.clicked_freq_info = None # Ensure previous box is cleared
            self.pending_marker = None
            return

        # --- Create Info Box Content ---
        info1_str = f"dB: {historical_data_dict.get('impedance_low','?')}/{historical_data_dict.get('impedance_high','?')} | Bars: {historical_data_dict.get('bars','?')} | Step: {historical_data_dict.get('step','?')}"
        info2_str = f"Mod: {historical_data_dict.get('modulation','?')} | BW: {historical_data_dict.get('bandwidth','?')}"

        freq_surf = self.tiny_font.render(freq_text, True, (255, 255, 255))
        time_surf = self.tiny_font.render(timestamp_str, True, (220, 220, 220))
        info1_surf = self.tiny_font.render(info1_str, True, (200, 200, 200))
        info2_surf = self.tiny_font.render(info2_str, True, (200, 200, 200))
        
        surfaces = [freq_surf, time_surf, info1_surf, info2_surf]
        save_btn_rect = pygame.Rect(0, 0, 20, 20) # Save button size
        
        # --- Calculate Info Box Position ---
        max_width = max(s.get_width() for s in surfaces) if surfaces else 0
        flip_to_left = (pos[0] + 15 + max_width > self.screen.get_width() - 10) # Check against screen edge

        drawable_items = []
        all_rects = []
        line_height_render = 14 # Spacing for info box lines
        current_y = pos[1] - (line_height_render * len(surfaces) / 2) # Center vertically roughly

        
        for surf in surfaces:
            rect = surf.get_rect(top=current_y)
            if flip_to_left:
                rect.right = pos[0] - 15
            else:
                rect.left = pos[0] + 15
            drawable_items.append((surf, rect))
            all_rects.append(rect)
            current_y += line_height_render

        # Position save button below last text item
        if all_rects:
             save_btn_rect.top = all_rects[-1].bottom + 5
             if flip_to_left:
                 save_btn_rect.right = all_rects[-1].right
             else:
                 save_btn_rect.left = all_rects[-1].left
             all_rects.append(save_btn_rect) # Add for background calculation
        else: # No text items, position button near click
             save_btn_rect.topleft = (pos[0] + 10, pos[1] + 10)


        # Calculate background rectangle based on all elements
        bg_rect = pygame.Rect(0,0,0,0) # Default empty rect
        if all_rects:
            bg_rect = all_rects[0].unionall(all_rects).inflate(8, 6) # Padding


        # --- Prepare Data for Saving POI ---
        # Ensure timestamp_str is suitable for DB (use the formatted string)
        data_for_saving = {
            "recording_id": recording_pk, # Can be None if not recording
            "frequency_mhz": mouse_freq,
            "absolute_timestamp": timestamp_str # Use the string representation
        }

        # --- Set State for Rendering ---
        # Store info needed to draw the box and handle save button click
        self.clicked_freq_info = (bg_rect, drawable_items, data_for_saving, save_btn_rect)

        # Create a potential visual marker, store but don't add to main list yet
        # Determine the initial update count based on mode
        initial_update_count = 0
        if self.app_state == 'LIVE':
             # Calculate how many UI updates ago this click corresponds to
             # This is approximate as UI update isn't perfectly synced with y_idx
             initial_update_count = max(0, self.waterfall_updates_since_start - y_idx)
        elif self.app_state == 'REPLAYING':
             initial_update_count = clicked_frame_index # Use the frame index


        self.pending_marker = {
            'x_ratio': x_ratio,
            'initial_y_idx': y_idx, # Store the relative y index clicked
            'initial_update_count': initial_update_count,
             # 'y_offset_updates' will be calculated dynamically based on current time/frame
        }


def main():
    parser = argparse.ArgumentParser(prog="K5Viewer", description="A live viewer for UV-K5 radios")
    parser.add_argument("--list-ports", action="store_true", help="list available ports and exit")
    parser.add_argument("--version", action="version", version=f"%(prog)s {VERSION}") # Corrected line
    args = parser.parse_args()
    
    init_db()

    if args.list_ports:
        cmd_list_ports(args)
        sys.exit(0)
    
    app = None
    try:
        app = K5ViewerApp(args)
        app.run()
    except KeyboardInterrupt:
        print("\n[✔] Exiting")
    except Exception as e:
        print("\n[!!!] A FATAL ERROR OCCURRED [!!!]")
        import traceback
        traceback.print_exc() # This prints the full error message
    finally:
        if app:
            app.running = False # Ensure loop terminates
            app._cleanup()
        pygame.quit()
        # Use os._exit to force exit if threads are stuck (use cautiously)
        # sys.exit()
        os._exit(0) # Force exit if serial thread hangs


if __name__ == "__main__":
    main()
