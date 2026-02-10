"""
╔══════════════════════════════════════════════════════════════════════════════════╗
║                                                                                  ║
║     ██╗   ██╗███╗   ██╗██╗██╗   ██╗███████╗██████╗ ███████╗ █████╗ ██╗           ║
║     ██║   ██║████╗  ██║██║██║   ██║██╔════╝██╔══██╗██╔════╝██╔══██╗██║           ║
║     ██║   ██║██╔██╗ ██║██║██║   ██║█████╗  ██████╔╝███████╗███████║██║           ║
║     ██║   ██║██║╚██╗██║██║╚██╗ ██╔╝██╔══╝  ██╔══██╗╚════██║██╔══██║██║           ║
║     ╚██████╔╝██║ ╚████║██║ ╚████╔╝ ███████╗██║  ██║███████║██║  ██║███████╗      ║
║      ╚═════╝ ╚═╝  ╚═══╝╚═╝  ╚═══╝  ╚══════╝╚═╝  ╚═╝╚══════╝╚═╝  ╚═╝╚══════╝      ║
║                                                                                  ║
║              ██╗      ██████╗  ██████╗     ██████╗  ██████╗ ████████╗            ║
║              ██║     ██╔═══██╗██╔════╝     ██╔══██╗██╔═══██╗╚══██╔══╝            ║
║              ██║     ██║   ██║██║  ███╗    ██████╔╝██║   ██║   ██║               ║
║              ██║     ██║   ██║██║   ██║    ██╔══██╗██║   ██║   ██║               ║
║              ███████╗╚██████╔╝╚██████╔╝    ██████╔╝╚██████╔╝   ██║               ║
║              ╚══════╝ ╚═════╝  ╚═════╝     ╚═════╝  ╚═════╝    ╚═╝               ║
║                                                                                  ║
║                    Universal Log Bot - Admin Edition                             ║
║                           Version 2.1.0                                          ║
║                                                                                  ║
║        Dynamisches Log-System mit Admin-Panel & Google Sheets                    ║
║        Komplett-Rewrite: Async Sheets, Auto-Refresh, Screenshots                 ║
║                                                                                  ║
╚══════════════════════════════════════════════════════════════════════════════════╝

Changelog v2.1.0:
  - NEU: Beweis-Bilder werden als =IMAGE()-Formel direkt in der Zelle angezeigt
  - NEU: Spalte H automatisch auf 200px Breite formatiert
  - NEU: Zeilenhoehe automatisch auf 100px fuer sichtbare Bildvorschau
  - NEU: Archiv-Tab bekommt ebenfalls Bildformatierung
  - FIX: Header "Bild-URL" umbenannt zu "Beweis-Bild"

Changelog v2.0.0:
  - FIX: Google Sheets Calls laufen jetzt async (kein Event-Loop-Blocking)
  - FIX: Button-Row-Berechnung korrigiert (max 5 pro Row, max 20 Kategorien)
  - FIX: Leaderboard-Command Response-Handling
  - FIX: Panel aktualisiert sich automatisch bei Kategorie-Aenderungen
  - NEU: Direkter Screenshot-Upload statt URL-Eingabe (Zwei-Schritt-Flow)
  - NEU: Retry-Mechanismus für Google Sheets API (3 Versuche)
  - NEU: Bestaetigungsdialog vor Auszahlung
  - NEU: Audit-Log für Admin-Aktionen
  - NEU: Rate-Limiting (1 Log pro 30 Sekunden pro User)
  - NEU: Einstellungen-Tab wird aktiv genutzt
  - NEU: Bild-URL-Validierung
"""

import discord
from discord.ext import commands, tasks
from discord import app_commands
import os
import json
import re
import base64
import asyncio
import traceback
from datetime import datetime
from collections import defaultdict
from typing import Optional

# ══════════════════════════════════════════════════════════════════════════════
# Google Sheets Imports
# ══════════════════════════════════════════════════════════════════════════════
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# ══════════════════════════════════════════════════════════════════════════════
# KONFIGURATION
# ══════════════════════════════════════════════════════════════════════════════

BOT_NAME = "Universal Log Bot"
BOT_VERSION = "2.1.0"
MAX_RETRIES = 3
RETRY_DELAY = 2  # Sekunden zwischen Retries
LOG_COOLDOWN = 30  # Sekunden zwischen Logs pro User


class Colors:
    PRIMARY = 0x5865F2      # Discord Blurple
    SUCCESS = 0x57F287      # Gruen
    WARNING = 0xFEE75C      # Gelb
    ERROR = 0xED4245        # Rot
    INFO = 0x5865F2         # Blau
    ADMIN = 0xEB459E        # Pink/Magenta
    PAYOUT = 0x57F287       # Gruen
    STATS = 0x5865F2        # Blau
    PANEL = 0x2F3136        # Dunkelgrau
    LOG = 0x5865F2          # Blurple
    NEUTRAL = 0x99AAB5      # Grau
    AUDIT = 0xFFA500        # Orange


# Google Sheets Tab-Namen (KEINE Emojis!)
SHEET_TABS = {
    "logs": "Logs",
    "kategorien": "Kategorien",
    "auszahlungen": "Auszahlungen",
    "dashboard": "Dashboard",
    "archiv": "Archiv",
    "einstellungen": "Einstellungen",
    "audit": "Audit-Log"
}

# ══════════════════════════════════════════════════════════════════════════════
# ENVIRONMENT VARIABLEN
# ══════════════════════════════════════════════════════════════════════════════

DISCORD_TOKEN = os.getenv("DISCORD_TOKEN", "")
SPREADSHEET_ID = os.getenv("GOOGLE_SHEET_ID", "")
GOOGLE_CREDENTIALS_BASE64 = os.getenv("GOOGLE_CREDENTIALS_BASE64", "")
ADMIN_ROLE_NAME = os.getenv("ADMIN_ROLE_NAME", "Admin")
LOG_CHANNEL_ID = os.getenv("LOG_CHANNEL_ID", "")

# ══════════════════════════════════════════════════════════════════════════════
# BOT SETUP
# ══════════════════════════════════════════════════════════════════════════════

intents = discord.Intents.default()
intents.message_content = True
intents.members = True

bot = commands.Bot(
    command_prefix="!",
    intents=intents,
    help_command=None
)

# Globale Variablen
bot.sheets_service = None
bot.categories_cache = {}       # {name: {"betrag": int, "emoji": str, "beschreibung": str, "aktiv": bool}}
bot.settings_cache = {}         # Einstellungen aus dem Sheet
bot.panel_messages = {}         # {channel_id: message_id} - für Auto-Refresh
bot.user_cooldowns = {}         # {user_id: datetime} - Rate Limiting


# ══════════════════════════════════════════════════════════════════════════════
# GOOGLE SHEETS SERVICE + ASYNC WRAPPER
# ══════════════════════════════════════════════════════════════════════════════

def init_google_sheets():
    """Initialisiere Google Sheets Verbindung"""
    try:
        if not GOOGLE_CREDENTIALS_BASE64:
            print("WARNUNG: GOOGLE_CREDENTIALS_BASE64 nicht gesetzt!")
            return None

        creds_json = base64.b64decode(GOOGLE_CREDENTIALS_BASE64)
        creds_dict = json.loads(creds_json)

        credentials = Credentials.from_service_account_info(
            creds_dict,
            scopes=[
                'https://www.googleapis.com/auth/spreadsheets',
                'https://www.googleapis.com/auth/drive'
            ]
        )

        service = build('sheets', 'v4', credentials=credentials)
        print("Google Sheets verbunden!")
        return service

    except Exception as e:
        print(f"Google Sheets Fehler: {e}")
        traceback.print_exc()
        return None


async def sheets_call(func, *args, retries=MAX_RETRIES, **kwargs):
    """
    Async Wrapper für synchrone Google Sheets API Calls.
    - Laeuft in einem Thread-Pool (blockiert den Event Loop nicht)
    - Retry-Mechanismus bei temporaeren Fehlern
    """
    last_error = None
    for attempt in range(1, retries + 1):
        try:
            result = await asyncio.to_thread(func, *args, **kwargs)
            return result
        except HttpError as e:
            last_error = e
            status = e.resp.status if hasattr(e, 'resp') else 0
            # Nicht-retrybare Fehler sofort abbrechen
            if status in (400, 401, 403, 404):
                print(f"Sheets API Fehler (nicht retrybar): {status} - {e}")
                raise
            print(f"Sheets API Fehler (Versuch {attempt}/{retries}): {status} - {e}")
        except Exception as e:
            last_error = e
            print(f"Sheets Call Fehler (Versuch {attempt}/{retries}): {e}")

        if attempt < retries:
            await asyncio.sleep(RETRY_DELAY * attempt)

    print(f"Sheets Call endgueltig fehlgeschlagen nach {retries} Versuchen: {last_error}")
    raise last_error


# ══════════════════════════════════════════════════════════════════════════════
# SHEETS HELPER FUNKTIONEN
# ══════════════════════════════════════════════════════════════════════════════

async def ensure_sheet_tabs():
    """Erstelle alle benoetigten Tabs falls sie nicht existieren"""
    if not bot.sheets_service:
        return False

    try:
        sheet = bot.sheets_service.spreadsheets()
        spreadsheet = await sheets_call(
            sheet.get(spreadsheetId=SPREADSHEET_ID).execute
        )
        existing_tabs = [s['properties']['title'] for s in spreadsheet.get('sheets', [])]

        tabs_to_create = []
        for key, tab_name in SHEET_TABS.items():
            if tab_name not in existing_tabs:
                tabs_to_create.append({
                    'addSheet': {
                        'properties': {'title': tab_name}
                    }
                })

        if tabs_to_create:
            await sheets_call(
                sheet.batchUpdate(
                    spreadsheetId=SPREADSHEET_ID,
                    body={'requests': tabs_to_create}
                ).execute
            )
            print(f"{len(tabs_to_create)} Tab(s) erstellt")

        await set_sheet_headers()
        await format_image_column()
        return True

    except Exception as e:
        print(f"Tab-Erstellung Fehler: {e}")
        traceback.print_exc()
        return False


async def set_sheet_headers():
    """Setze Header für alle Tabs"""
    if not bot.sheets_service:
        return

    try:
        sheet = bot.sheets_service.spreadsheets()

        headers = {
            f"{SHEET_TABS['logs']}!A1:I1": [[
                "Zeitstempel", "Kalenderwoche", "User", "User-ID",
                "Kategorie", "Beschreibung", "Betrag", "Beweis-Bild", "Log-ID"
            ]],
            f"{SHEET_TABS['kategorien']}!A1:F1": [[
                "Kategorie", "Betrag", "Emoji", "Beschreibung", "Aktiv", "Erstellt am"
            ]],
            f"{SHEET_TABS['auszahlungen']}!A1:H1": [[
                "Zeitstempel", "Kalenderwoche", "User", "User-ID",
                "Betrag", "Anzahl Logs", "Status", "Ausgezahlt von"
            ]],
            f"{SHEET_TABS['dashboard']}!A1:F1": [[
                "User", "User-ID", "Aktuelle KW Logs", "Aktuelle KW Betrag",
                "Gesamt Logs", "Gesamt Betrag"
            ]],
            f"{SHEET_TABS['archiv']}!A1:J1": [[
                "Zeitstempel", "Kalenderwoche", "User", "User-ID",
                "Kategorie", "Beschreibung", "Betrag", "Beweis-Bild", "Log-ID", "Archiviert am"
            ]],
            f"{SHEET_TABS['einstellungen']}!A1:B1": [[
                "Einstellung", "Wert"
            ]],
            f"{SHEET_TABS['audit']}!A1:F1": [[
                "Zeitstempel", "Admin", "Admin-ID", "Aktion", "Details", "Betroffenes Objekt"
            ]]
        }

        for range_name, values in headers.items():
            try:
                existing = await sheets_call(
                    sheet.values().get(
                        spreadsheetId=SPREADSHEET_ID,
                        range=range_name
                    ).execute
                )
                if existing.get('values'):
                    continue
            except Exception:
                pass

            await sheets_call(
                sheet.values().update(
                    spreadsheetId=SPREADSHEET_ID,
                    range=range_name,
                    valueInputOption='USER_ENTERED',
                    body={'values': values}
                ).execute
            )

        print("Sheet-Header gesetzt")

    except Exception as e:
        print(f"Header Fehler: {e}")


async def format_image_column():
    """
    Formatiere die Beweis-Bild-Spalte (H) in Logs und Archiv:
    - Spaltenbreite auf 200px (damit Bilder sichtbar sind)
    - Standard-Zeilenhoehe auf 100px fuer Bildspalte
    """
    if not bot.sheets_service:
        return

    try:
        sheet = bot.sheets_service.spreadsheets()

        # Sheet-IDs holen (jeder Tab hat eine eigene numerische ID)
        spreadsheet = await sheets_call(
            sheet.get(spreadsheetId=SPREADSHEET_ID).execute
        )
        sheet_ids = {}
        for s in spreadsheet.get('sheets', []):
            title = s['properties']['title']
            sid = s['properties']['sheetId']
            sheet_ids[title] = sid

        requests = []

        for tab_name in [SHEET_TABS['logs'], SHEET_TABS['archiv']]:
            sid = sheet_ids.get(tab_name)
            if sid is None:
                continue

            # Spalte H (Index 7) auf 200px Breite setzen
            requests.append({
                'updateDimensionProperties': {
                    'range': {
                        'sheetId': sid,
                        'dimension': 'COLUMNS',
                        'startIndex': 7,
                        'endIndex': 8
                    },
                    'properties': {
                        'pixelSize': 200
                    },
                    'fields': 'pixelSize'
                }
            })

            # Standardmaessig Zeilen ab Row 2 auf 100px Hoehe
            # (Row 1 = Header, bleibt normal)
            requests.append({
                'updateDimensionProperties': {
                    'range': {
                        'sheetId': sid,
                        'dimension': 'ROWS',
                        'startIndex': 1,   # Ab Zeile 2 (0-indexed)
                        'endIndex': 1000   # Erste 1000 Zeilen
                    },
                    'properties': {
                        'pixelSize': 100
                    },
                    'fields': 'pixelSize'
                }
            })

        if requests:
            await sheets_call(
                sheet.batchUpdate(
                    spreadsheetId=SPREADSHEET_ID,
                    body={'requests': requests}
                ).execute
            )
            print("Bild-Spalte formatiert (200px breit, 100px Zeilenhoehe)")

    except Exception as e:
        print(f"Bild-Spalte Formatierung Fehler: {e}")


async def write_audit_log(admin: discord.Member, aktion: str, details: str, objekt: str = ""):
    """Schreibe einen Eintrag ins Audit-Log"""
    if not bot.sheets_service:
        return

    try:
        sheet = bot.sheets_service.spreadsheets()
        now = datetime.now()

        values = [[
            now.strftime("%d.%m.%Y %H:%M:%S"),
            admin.display_name,
            str(admin.id),
            aktion,
            details,
            objekt
        ]]

        await sheets_call(
            sheet.values().append(
                spreadsheetId=SPREADSHEET_ID,
                range=f"{SHEET_TABS['audit']}!A:F",
                valueInputOption='USER_ENTERED',
                body={'values': values}
            ).execute
        )

    except Exception as e:
        print(f"Audit-Log Fehler: {e}")


async def load_categories():
    """Lade Kategorien aus Google Sheets"""
    if not bot.sheets_service:
        return {}

    try:
        sheet = bot.sheets_service.spreadsheets()
        result = await sheets_call(
            sheet.values().get(
                spreadsheetId=SPREADSHEET_ID,
                range=f"{SHEET_TABS['kategorien']}!A2:F"
            ).execute
        )

        values = result.get('values', [])
        categories = {}

        for row in values:
            if len(row) >= 2:
                name = row[0].strip()
                if not name:
                    continue
                try:
                    betrag = int(str(row[1]).replace(".", "").replace(",", "").replace("$", "").strip())
                except (ValueError, IndexError):
                    betrag = 0
                categories[name] = {
                    "betrag": betrag,
                    "emoji": row[2].strip() if len(row) > 2 else "",
                    "beschreibung": row[3].strip() if len(row) > 3 else "",
                    "aktiv": row[4].strip().lower() == "ja" if len(row) > 4 else True
                }

        bot.categories_cache = categories
        print(f"{len(categories)} Kategorie(n) geladen")
        return categories

    except Exception as e:
        print(f"Kategorien laden Fehler: {e}")
        return {}


async def save_category(name: str, betrag: int, emoji: str = "", beschreibung: str = "", aktiv: bool = True):
    """Speichere eine Kategorie in Google Sheets"""
    if not bot.sheets_service:
        return False

    try:
        sheet = bot.sheets_service.spreadsheets()

        # Pruefe ob Kategorie existiert
        result = await sheets_call(
            sheet.values().get(
                spreadsheetId=SPREADSHEET_ID,
                range=f"{SHEET_TABS['kategorien']}!A2:A"
            ).execute
        )
        existing = result.get('values', [])
        row_index = None

        for i, row in enumerate(existing):
            if row and row[0].strip() == name:
                row_index = i + 2
                break

        values = [[
            name, betrag, emoji, beschreibung,
            "Ja" if aktiv else "Nein",
            datetime.now().strftime("%d.%m.%Y %H:%M")
        ]]

        if row_index:
            await sheets_call(
                sheet.values().update(
                    spreadsheetId=SPREADSHEET_ID,
                    range=f"{SHEET_TABS['kategorien']}!A{row_index}:F{row_index}",
                    valueInputOption='USER_ENTERED',
                    body={'values': values}
                ).execute
            )
        else:
            await sheets_call(
                sheet.values().append(
                    spreadsheetId=SPREADSHEET_ID,
                    range=f"{SHEET_TABS['kategorien']}!A:F",
                    valueInputOption='USER_ENTERED',
                    body={'values': values}
                ).execute
            )

        # Cache aktualisieren
        bot.categories_cache[name] = {
            "betrag": betrag,
            "emoji": emoji,
            "beschreibung": beschreibung,
            "aktiv": aktiv
        }

        return True

    except Exception as e:
        print(f"Kategorie speichern Fehler: {e}")
        return False


async def delete_category_from_sheet(name: str):
    """Deaktiviere eine Kategorie in Google Sheets"""
    if not bot.sheets_service:
        return False

    try:
        sheet = bot.sheets_service.spreadsheets()

        result = await sheets_call(
            sheet.values().get(
                spreadsheetId=SPREADSHEET_ID,
                range=f"{SHEET_TABS['kategorien']}!A2:A"
            ).execute
        )
        existing = result.get('values', [])

        for i, row in enumerate(existing):
            if row and row[0].strip() == name:
                row_index = i + 2
                await sheets_call(
                    sheet.values().update(
                        spreadsheetId=SPREADSHEET_ID,
                        range=f"{SHEET_TABS['kategorien']}!E{row_index}",
                        valueInputOption='USER_ENTERED',
                        body={'values': [["Nein"]]}
                    ).execute
                )
                break

        if name in bot.categories_cache:
            bot.categories_cache[name]["aktiv"] = False

        return True

    except Exception as e:
        print(f"Kategorie loeschen Fehler: {e}")
        return False


async def save_log_entry(user: discord.Member, kategorie: str, beschreibung: str, image_url: str = ""):
    """Speichere einen Log-Eintrag in Google Sheets. Gibt die Row-Nummer zurueck oder 0 bei Fehler."""
    if not bot.sheets_service:
        return 0

    try:
        sheet = bot.sheets_service.spreadsheets()

        now = datetime.now()
        timestamp = now.strftime("%d.%m.%Y %H:%M:%S")
        week_number = now.isocalendar()[1]
        year = now.year

        betrag = bot.categories_cache.get(kategorie, {}).get("betrag", 0)
        log_id = f"LOG-{now.strftime('%Y%m%d%H%M%S')}-{user.id}"

        # Bild als IMAGE()-Formel fuer direkte Anzeige im Sheet
        bild_zelle = f'=IMAGE("{image_url}")' if image_url else ""

        values = [[
            timestamp,
            f"KW{week_number}/{year}",
            user.display_name,
            str(user.id),
            kategorie,
            beschreibung,
            betrag,
            bild_zelle,
            log_id
        ]]

        result = await sheets_call(
            sheet.values().append(
                spreadsheetId=SPREADSHEET_ID,
                range=f"{SHEET_TABS['logs']}!A:I",
                valueInputOption='USER_ENTERED',
                body={'values': values}
            ).execute
        )

        # Ermittle die geschriebene Zeile
        updated_range = result.get('updates', {}).get('updatedRange', '')
        # Format: "Logs!A42:I42" -> 42
        match = re.search(r'!A(\d+)', updated_range)
        row_number = int(match.group(1)) if match else 0

        print(f"Log gespeichert: {user.display_name} - {kategorie} (Zeile {row_number})")
        return row_number

    except Exception as e:
        print(f"Log speichern Fehler: {e}")
        traceback.print_exc()
        return 0


async def update_log_image(row_number: int, image_url: str):
    """Aktualisiere das Beweis-Bild eines bestehenden Log-Eintrags (als IMAGE-Formel)"""
    if not bot.sheets_service or row_number <= 0:
        return False

    try:
        sheet = bot.sheets_service.spreadsheets()

        # IMAGE()-Formel fuer direkte Anzeige im Sheet
        bild_zelle = f'=IMAGE("{image_url}")' if image_url else ""

        await sheets_call(
            sheet.values().update(
                spreadsheetId=SPREADSHEET_ID,
                range=f"{SHEET_TABS['logs']}!H{row_number}",
                valueInputOption='USER_ENTERED',
                body={'values': [[bild_zelle]]}
            ).execute
        )
        return True

    except Exception as e:
        print(f"Bild-Update Fehler: {e}")
        return False


async def get_user_stats(user_id: int, week_filter: str = None):
    """Hole Statistiken für einen User"""
    if not bot.sheets_service:
        return {"logs": 0, "betrag": 0, "details": {}}

    try:
        sheet = bot.sheets_service.spreadsheets()
        result = await sheets_call(
            sheet.values().get(
                spreadsheetId=SPREADSHEET_ID,
                range=f"{SHEET_TABS['logs']}!A2:I"
            ).execute
        )

        values = result.get('values', [])
        stats = {"logs": 0, "betrag": 0, "details": defaultdict(lambda: {"count": 0, "betrag": 0})}

        for row in values:
            if len(row) >= 7 and row[3] == str(user_id):
                if week_filter and row[1] != week_filter:
                    continue
                stats["logs"] += 1
                try:
                    betrag = int(str(row[6]).replace(".", "").replace(",", "").replace("$", "").strip())
                except (ValueError, IndexError):
                    betrag = 0
                stats["betrag"] += betrag
                kategorie = row[4] if len(row) > 4 else "Unbekannt"
                stats["details"][kategorie]["count"] += 1
                stats["details"][kategorie]["betrag"] += betrag

        return stats

    except Exception as e:
        print(f"Stats Fehler: {e}")
        return {"logs": 0, "betrag": 0, "details": {}}


async def get_all_user_stats(week_filter: str = None):
    """Hole Statistiken für alle User"""
    if not bot.sheets_service:
        return {}

    try:
        sheet = bot.sheets_service.spreadsheets()
        result = await sheets_call(
            sheet.values().get(
                spreadsheetId=SPREADSHEET_ID,
                range=f"{SHEET_TABS['logs']}!A2:I"
            ).execute
        )

        values = result.get('values', [])
        users = defaultdict(lambda: {"name": "", "logs": 0, "betrag": 0})

        for row in values:
            if len(row) >= 7:
                if week_filter and row[1] != week_filter:
                    continue
                user_id = row[3]
                users[user_id]["name"] = row[2]
                users[user_id]["logs"] += 1
                try:
                    betrag = int(str(row[6]).replace(".", "").replace(",", "").replace("$", "").strip())
                except (ValueError, IndexError):
                    betrag = 0
                users[user_id]["betrag"] += betrag

        return dict(users)

    except Exception as e:
        print(f"All Stats Fehler: {e}")
        return {}


async def save_payout(user_id: str, username: str, amount: int, week: str, log_count: int, admin_name: str):
    """Speichere Auszahlung"""
    if not bot.sheets_service:
        return False

    try:
        sheet = bot.sheets_service.spreadsheets()
        now = datetime.now()
        timestamp = now.strftime("%d.%m.%Y %H:%M:%S")

        values = [[
            timestamp, week, username, str(user_id),
            amount, log_count, "Ausgezahlt", admin_name
        ]]

        await sheets_call(
            sheet.values().append(
                spreadsheetId=SPREADSHEET_ID,
                range=f"{SHEET_TABS['auszahlungen']}!A:H",
                valueInputOption='USER_ENTERED',
                body={'values': values}
            ).execute
        )

        return True

    except Exception as e:
        print(f"Payout Fehler: {e}")
        return False


async def archive_user_logs(user_id: int, week: str):
    """Archiviere User-Logs einer bestimmten Woche"""
    if not bot.sheets_service:
        return False

    try:
        sheet = bot.sheets_service.spreadsheets()
        result = await sheets_call(
            sheet.values().get(
                spreadsheetId=SPREADSHEET_ID,
                range=f"{SHEET_TABS['logs']}!A2:I"
            ).execute
        )

        values = result.get('values', [])
        logs_to_archive = []
        rows_to_keep = []

        for row in values:
            if len(row) >= 4 and row[1] == week and row[3] == str(user_id):
                archived_row = row + [datetime.now().strftime("%d.%m.%Y %H:%M:%S")]
                logs_to_archive.append(archived_row)
            else:
                rows_to_keep.append(row)

        if logs_to_archive:
            await sheets_call(
                sheet.values().append(
                    spreadsheetId=SPREADSHEET_ID,
                    range=f"{SHEET_TABS['archiv']}!A:J",
                    valueInputOption='USER_ENTERED',
                    body={'values': logs_to_archive}
                ).execute
            )

            await sheets_call(
                sheet.values().clear(
                    spreadsheetId=SPREADSHEET_ID,
                    range=f"{SHEET_TABS['logs']}!A2:I"
                ).execute
            )

            if rows_to_keep:
                await sheets_call(
                    sheet.values().update(
                        spreadsheetId=SPREADSHEET_ID,
                        range=f"{SHEET_TABS['logs']}!A2:I",
                        valueInputOption='USER_ENTERED',
                        body={'values': rows_to_keep}
                    ).execute
                )

        return True

    except Exception as e:
        print(f"Archiv Fehler: {e}")
        return False


async def update_dashboard():
    """Aktualisiere das Dashboard-Tab in Google Sheets"""
    if not bot.sheets_service:
        return

    try:
        now = datetime.now()
        week_number = now.isocalendar()[1]
        year = now.year
        current_week = f"KW{week_number}/{year}"

        all_stats = await get_all_user_stats()
        week_stats = await get_all_user_stats(week_filter=current_week)

        sheet = bot.sheets_service.spreadsheets()

        await sheets_call(
            sheet.values().clear(
                spreadsheetId=SPREADSHEET_ID,
                range=f"{SHEET_TABS['dashboard']}!A2:F"
            ).execute
        )

        all_users = set(list(all_stats.keys()) + list(week_stats.keys()))
        dashboard_data = []

        for user_id in all_users:
            all_data = all_stats.get(user_id, {"name": "Unbekannt", "logs": 0, "betrag": 0})
            week_data = week_stats.get(user_id, {"logs": 0, "betrag": 0})

            dashboard_data.append([
                all_data["name"],
                user_id,
                week_data["logs"],
                f"{week_data['betrag']:,}$".replace(",", "."),
                all_data["logs"],
                f"{all_data['betrag']:,}$".replace(",", ".")
            ])

        dashboard_data.sort(
            key=lambda x: int(str(x[3]).replace(".", "").replace("$", "") or "0"),
            reverse=True
        )

        if dashboard_data:
            await sheets_call(
                sheet.values().update(
                    spreadsheetId=SPREADSHEET_ID,
                    range=f"{SHEET_TABS['dashboard']}!A2:F",
                    valueInputOption='USER_ENTERED',
                    body={'values': dashboard_data}
                ).execute
            )

        print("Dashboard aktualisiert")

    except Exception as e:
        print(f"Dashboard Update Fehler: {e}")


async def load_settings():
    """Lade Einstellungen aus Google Sheets"""
    if not bot.sheets_service:
        return {}

    try:
        sheet = bot.sheets_service.spreadsheets()
        result = await sheets_call(
            sheet.values().get(
                spreadsheetId=SPREADSHEET_ID,
                range=f"{SHEET_TABS['einstellungen']}!A2:B"
            ).execute
        )

        values = result.get('values', [])
        settings = {}
        for row in values:
            if len(row) >= 2:
                settings[row[0].strip()] = row[1].strip()

        bot.settings_cache = settings
        return settings

    except Exception as e:
        print(f"Settings laden Fehler: {e}")
        return {}


async def save_settings(settings: dict):
    """Speichere Einstellungen in Google Sheets"""
    if not bot.sheets_service:
        return False

    try:
        sheet = bot.sheets_service.spreadsheets()

        await sheets_call(
            sheet.values().clear(
                spreadsheetId=SPREADSHEET_ID,
                range=f"{SHEET_TABS['einstellungen']}!A2:B"
            ).execute
        )

        values = [[k, v] for k, v in settings.items()]
        if values:
            await sheets_call(
                sheet.values().update(
                    spreadsheetId=SPREADSHEET_ID,
                    range=f"{SHEET_TABS['einstellungen']}!A2:B",
                    valueInputOption='USER_ENTERED',
                    body={'values': values}
                ).execute
            )

        bot.settings_cache = settings
        return True

    except Exception as e:
        print(f"Settings speichern Fehler: {e}")
        return False


async def init_default_settings():
    """Erstelle Standard-Einstellungen falls Tab leer"""
    current = await load_settings()
    if not current:
        defaults = {
            "Log-Cooldown (Sekunden)": str(LOG_COOLDOWN),
            "Admin-Rolle": ADMIN_ROLE_NAME,
            "Dashboard Auto-Update (Stunden)": "1",
            "Auszahlungs-Waehrung": "$",
            "Max Kategorien": "20",
            "Screenshot Pflicht": "Nein",
            "Bot Version": BOT_VERSION
        }
        await save_settings(defaults)


# ══════════════════════════════════════════════════════════════════════════════
# HILFSFUNKTIONEN
# ══════════════════════════════════════════════════════════════════════════════

def is_admin(interaction: discord.Interaction) -> bool:
    """Pruefe ob der User Admin-Rechte hat"""
    if interaction.user.guild_permissions.administrator:
        return True
    admin_role = bot.settings_cache.get("Admin-Rolle", ADMIN_ROLE_NAME)
    for role in interaction.user.roles:
        if role.name.lower() == admin_role.lower():
            return True
    return False


def get_current_week() -> str:
    """Hole aktuelle Kalenderwoche als String"""
    now = datetime.now()
    return f"KW{now.isocalendar()[1]}/{now.year}"


def format_currency(amount: int) -> str:
    """Formatiere Betrag als Waehrung"""
    symbol = bot.settings_cache.get("Auszahlungs-Waehrung", "$")
    return f"{amount:,}{symbol}".replace(",", ".")


def check_cooldown(user_id: int) -> Optional[int]:
    """
    Pruefe Rate-Limit. Gibt verbleibende Sekunden zurueck oder None wenn OK.
    """
    now = datetime.now()
    last_log = bot.user_cooldowns.get(user_id)
    if last_log:
        try:
            cooldown_sec = int(bot.settings_cache.get("Log-Cooldown (Sekunden)", LOG_COOLDOWN))
        except ValueError:
            cooldown_sec = LOG_COOLDOWN
        diff = (now - last_log).total_seconds()
        if diff < cooldown_sec:
            return int(cooldown_sec - diff)
    return None


def set_cooldown(user_id: int):
    """Setze Cooldown für User"""
    bot.user_cooldowns[user_id] = datetime.now()



# ══════════════════════════════════════════════════════════════════════════════
# PANEL AUTO-REFRESH
# ══════════════════════════════════════════════════════════════════════════════

async def refresh_all_panels():
    """Aktualisiere alle gespeicherten Panel-Nachrichten"""
    if not bot.panel_messages:
        return

    to_remove = []

    for channel_id, message_id in bot.panel_messages.items():
        try:
            channel = bot.get_channel(channel_id)
            if not channel:
                to_remove.append(channel_id)
                continue

            message = await channel.fetch_message(message_id)
            if not message:
                to_remove.append(channel_id)
                continue

            # Neues Embed und View erstellen
            embed = build_panel_embed()
            view = LogPanelView()

            await message.edit(embed=embed, view=view)
            print(f"Panel in #{channel.name} aktualisiert")

        except discord.NotFound:
            to_remove.append(channel_id)
        except discord.Forbidden:
            to_remove.append(channel_id)
        except Exception as e:
            print(f"Panel-Refresh Fehler (Channel {channel_id}): {e}")

    for cid in to_remove:
        del bot.panel_messages[cid]


def build_panel_embed() -> discord.Embed:
    """Erstelle das Panel-Embed (wiederverwendbar für Refresh)"""
    active_count = sum(1 for v in bot.categories_cache.values() if v.get("aktiv", True))

    embed = discord.Embed(
        title="Log-Panel",
        description=(
            "Waehle eine Kategorie um einen Log-Eintrag zu erstellen.\n"
            "Deine Logs werden automatisch in Google Sheets gespeichert\n"
            "und für die woechentliche Auszahlung berechnet."
        ),
        color=Colors.PANEL
    )

    if bot.categories_cache:
        cat_text = ""
        for name, data in bot.categories_cache.items():
            if data.get("aktiv", True):
                emoji = data.get("emoji", "")
                cat_text += f"**{emoji} {name}** - {format_currency(data['betrag'])}\n"
        if cat_text:
            embed.add_field(name="Verfuegbare Kategorien", value=cat_text, inline=False)

    embed.set_footer(text=f"{BOT_NAME} v{BOT_VERSION} | {active_count} aktive Kategorien")
    embed.timestamp = datetime.now()

    return embed


# ══════════════════════════════════════════════════════════════════════════════
# UI KOMPONENTEN - MODALS
# ══════════════════════════════════════════════════════════════════════════════

class LogModal(discord.ui.Modal, title="Log eintragen"):
    """Modal für Log-Eintraege (nur Beschreibung, Bild kommt separat)"""

    def __init__(self, kategorie: str):
        super().__init__()
        self.kategorie = kategorie

    beschreibung = discord.ui.TextInput(
        label="Beschreibung",
        placeholder="Was hast du gemacht?",
        style=discord.TextStyle.paragraph,
        required=True,
        max_length=500
    )

    async def on_submit(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)

        # Rate-Limit pruefen
        remaining = check_cooldown(interaction.user.id)
        if remaining is not None:
            embed = discord.Embed(
                title="Cooldown aktiv",
                description=f"Du kannst in **{remaining} Sekunden** wieder loggen.",
                color=Colors.WARNING
            )
            await interaction.followup.send(embed=embed, ephemeral=True)
            return

        beschreibung_text = str(self.beschreibung)

        row_number = await save_log_entry(
            user=interaction.user,
            kategorie=self.kategorie,
            beschreibung=beschreibung_text,
            image_url=""
        )

        if row_number:
            set_cooldown(interaction.user.id)

            betrag = bot.categories_cache.get(self.kategorie, {}).get("betrag", 0)
            emoji = bot.categories_cache.get(self.kategorie, {}).get("emoji", "")

            embed = discord.Embed(
                title="Log erfolgreich!",
                color=Colors.SUCCESS
            )
            embed.add_field(name="Kategorie", value=f"{emoji} {self.kategorie}", inline=True)
            embed.add_field(name="Betrag", value=format_currency(betrag), inline=True)
            embed.add_field(name="Beschreibung", value=beschreibung_text[:100], inline=False)
            embed.set_footer(text=f"{BOT_NAME} v{BOT_VERSION}")
            embed.timestamp = datetime.now()

            # Screenshot-Upload Button anbieten
            view = ScreenshotUploadView(
                row_number=row_number,
                user_id=interaction.user.id,
                channel_id=interaction.channel_id
            )

            await interaction.followup.send(
                embed=embed,
                view=view,
                ephemeral=True
            )

            # Log-Channel benachrichtigen
            await send_log_notification(
                interaction.user, self.kategorie, beschreibung_text, betrag, emoji
            )
        else:
            embed = discord.Embed(
                title="Fehler",
                description="Log konnte nicht gespeichert werden. Bitte versuche es erneut.",
                color=Colors.ERROR
            )
            await interaction.followup.send(embed=embed, ephemeral=True)


class AddCategoryModal(discord.ui.Modal, title="Neue Kategorie erstellen"):
    """Modal zum Erstellen einer neuen Kategorie"""

    def __init__(self, admin: discord.Member):
        super().__init__()
        self.admin = admin

    name = discord.ui.TextInput(
        label="Name der Kategorie",
        placeholder="z.B. Düngen, Reparieren, Panel",
        style=discord.TextStyle.short,
        required=True,
        max_length=50
    )

    betrag = discord.ui.TextInput(
        label="Auszahlungsbetrag ($)",
        placeholder="z.B. 10000",
        style=discord.TextStyle.short,
        required=True,
        max_length=15
    )

    emoji_field = discord.ui.TextInput(
        label="Emoji (optional)",
        placeholder="z.B. Düngen, Hammer, Pflanze",
        style=discord.TextStyle.short,
        required=False,
        max_length=30
    )

    beschreibung = discord.ui.TextInput(
        label="Beschreibung (optional)",
        placeholder="Kurze Beschreibung der Kategorie",
        style=discord.TextStyle.paragraph,
        required=False,
        max_length=200
    )

    async def on_submit(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)

        cat_name = str(self.name).strip()

        # Max-Kategorien pruefen
        try:
            max_cats = int(bot.settings_cache.get("Max Kategorien", "20"))
        except ValueError:
            max_cats = 20

        active_count = sum(1 for v in bot.categories_cache.values() if v.get("aktiv", True))
        if active_count >= max_cats and cat_name not in bot.categories_cache:
            await interaction.followup.send(
                embed=discord.Embed(
                    title="Limit erreicht",
                    description=f"Maximal {max_cats} aktive Kategorien erlaubt.",
                    color=Colors.ERROR
                ),
                ephemeral=True
            )
            return

        try:
            betrag = int(str(self.betrag).replace(".", "").replace(",", "").replace("$", "").strip())
        except ValueError:
            await interaction.followup.send(
                embed=discord.Embed(
                    title="Fehler",
                    description="Ungueltiger Betrag! Bitte nur Zahlen eingeben.",
                    color=Colors.ERROR
                ),
                ephemeral=True
            )
            return

        emoji = str(self.emoji_field).strip() if self.emoji_field else ""
        beschreibung = str(self.beschreibung).strip() if self.beschreibung else ""

        success = await save_category(cat_name, betrag, emoji, beschreibung, True)

        if success:
            # Audit-Log
            await write_audit_log(
                self.admin, "Kategorie erstellt",
                f"Betrag: {format_currency(betrag)}, Emoji: {emoji}",
                cat_name
            )

            embed = discord.Embed(
                title="Kategorie erstellt!",
                color=Colors.SUCCESS
            )
            embed.add_field(name="Name", value=f"{emoji} {cat_name}" if emoji else cat_name, inline=True)
            embed.add_field(name="Betrag", value=format_currency(betrag), inline=True)
            if beschreibung:
                embed.add_field(name="Beschreibung", value=beschreibung, inline=False)
            embed.set_footer(text="Panel wird automatisch aktualisiert!")
            await interaction.followup.send(embed=embed, ephemeral=True)

            # Panels automatisch aktualisieren
            await refresh_all_panels()
        else:
            await interaction.followup.send(
                embed=discord.Embed(
                    title="Fehler",
                    description="Kategorie konnte nicht gespeichert werden.",
                    color=Colors.ERROR
                ),
                ephemeral=True
            )


class EditCategoryModal(discord.ui.Modal, title="Kategorie bearbeiten"):
    """Modal zum Bearbeiten einer Kategorie"""

    def __init__(self, kategorie_name: str, current_betrag: int, current_emoji: str, current_beschreibung: str, admin: discord.Member):
        super().__init__()
        self.kategorie_name = kategorie_name
        self.admin = admin
        self.betrag_input.default = str(current_betrag)
        self.emoji_input.default = current_emoji
        self.beschreibung_input.default = current_beschreibung

    betrag_input = discord.ui.TextInput(
        label="Neuer Auszahlungsbetrag ($)",
        style=discord.TextStyle.short,
        required=True,
        max_length=15
    )

    emoji_input = discord.ui.TextInput(
        label="Emoji",
        style=discord.TextStyle.short,
        required=False,
        max_length=30
    )

    beschreibung_input = discord.ui.TextInput(
        label="Beschreibung",
        style=discord.TextStyle.paragraph,
        required=False,
        max_length=200
    )

    async def on_submit(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)

        try:
            betrag = int(str(self.betrag_input).replace(".", "").replace(",", "").replace("$", "").strip())
        except ValueError:
            await interaction.followup.send(
                embed=discord.Embed(title="Fehler", description="Ungueltiger Betrag!", color=Colors.ERROR),
                ephemeral=True
            )
            return

        emoji = str(self.emoji_input).strip() if self.emoji_input else ""
        beschreibung = str(self.beschreibung_input).strip() if self.beschreibung_input else ""

        old_betrag = bot.categories_cache.get(self.kategorie_name, {}).get("betrag", 0)

        success = await save_category(self.kategorie_name, betrag, emoji, beschreibung, True)

        if success:
            await write_audit_log(
                self.admin, "Kategorie bearbeitet",
                f"Betrag: {format_currency(old_betrag)} -> {format_currency(betrag)}",
                self.kategorie_name
            )

            embed = discord.Embed(
                title="Kategorie aktualisiert!",
                color=Colors.SUCCESS
            )
            embed.add_field(name="Kategorie", value=self.kategorie_name, inline=True)
            embed.add_field(name="Neuer Betrag", value=format_currency(betrag), inline=True)
            embed.set_footer(text="Panel wird automatisch aktualisiert!")
            await interaction.followup.send(embed=embed, ephemeral=True)

            # Panels automatisch aktualisieren
            await refresh_all_panels()
        else:
            await interaction.followup.send(
                embed=discord.Embed(title="Fehler", description="Update fehlgeschlagen.", color=Colors.ERROR),
                ephemeral=True
            )


# ══════════════════════════════════════════════════════════════════════════════
# UI KOMPONENTEN - VIEWS
# ══════════════════════════════════════════════════════════════════════════════

class ScreenshotUploadView(discord.ui.View):
    """View mit Screenshot-Upload Button nach Log-Erstellung"""

    def __init__(self, row_number: int, user_id: int, channel_id: int):
        super().__init__(timeout=120)
        self.row_number = row_number
        self.user_id = user_id
        self.channel_id = channel_id
        self.screenshot_uploaded = False

    @discord.ui.button(label="Screenshot hinzufügen", style=discord.ButtonStyle.secondary, emoji="📷")
    async def add_screenshot(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user.id != self.user_id:
            await interaction.response.send_message("Das ist nicht dein Log!", ephemeral=True)
            return

        await interaction.response.send_message(
            embed=discord.Embed(
                title="Screenshot hochladen",
                description=(
                    "Sende deinen **Screenshot als naechste Nachricht** in diesem Channel.\n"
                    "Du hast **60 Sekunden** Zeit.\n\n"
                    "Einfach das Bild per Drag & Drop oder über das + Symbol hochladen."
                ),
                color=Colors.INFO
            ),
            ephemeral=True
        )

        # Warte auf Nachricht mit Attachment
        def check(m):
            return (
                m.author.id == self.user_id
                and m.channel.id == self.channel_id
                and m.attachments
            )

        try:
            msg = await bot.wait_for('message', check=check, timeout=60.0)

            # Erstes Bild-Attachment nehmen
            image_attachment = None
            for att in msg.attachments:
                if att.content_type and att.content_type.startswith('image/'):
                    image_attachment = att
                    break

            if not image_attachment:
                image_attachment = msg.attachments[0]  # Fallback: erstes Attachment

            image_url = image_attachment.url

            # Log aktualisieren
            success = await update_log_image(self.row_number, image_url)

            if success:
                self.screenshot_uploaded = True

                # Bestaetigungsreaction
                try:
                    await msg.add_reaction("✅")
                except Exception:
                    pass

                # Nachricht des Users optional loeschen (Channel sauber halten)
                try:
                    await msg.delete(delay=3)
                except Exception:
                    pass

                # Log-Channel aktualisieren
                if LOG_CHANNEL_ID:
                    try:
                        channel = bot.get_channel(int(LOG_CHANNEL_ID))
                        if channel:
                            embed = discord.Embed(
                                title="Screenshot nachgereicht",
                                description=f"Von {interaction.user.mention}",
                                color=Colors.LOG
                            )
                            embed.set_image(url=image_url)
                            embed.set_footer(text=f"Log-Zeile: {self.row_number}")
                            await channel.send(embed=embed)
                    except Exception:
                        pass
            else:
                try:
                    await msg.add_reaction("❌")
                except Exception:
                    pass

        except asyncio.TimeoutError:
            # Timeout - nichts tun, User hat kein Bild gesendet
            pass

        # Button deaktivieren
        button.disabled = True
        button.label = "Screenshot hinzugefuegt" if self.screenshot_uploaded else "Abgelaufen"
        try:
            await interaction.edit_original_response(view=self)
        except Exception:
            pass

        self.stop()


class LogPanelView(discord.ui.View):
    """
    Interaktives Log-Panel mit dynamischen Buttons.
    Buttons haben KEINE Callbacks - die Logik wird komplett über
    on_interaction behandelt (siehe unten). Das erlaubt korrekte
    Behandlung nach Bot-Restart, auch wenn Kategorien sich geaendert haben.
    """

    def __init__(self):
        super().__init__(timeout=None)
        self._build_buttons()

    def _build_buttons(self):
        """Erstelle Buttons basierend auf aktiven Kategorien mit korrekter Row-Verteilung"""
        self.clear_items()

        active_categories = {
            k: v for k, v in bot.categories_cache.items() if v.get("aktiv", True)
        }

        if not active_categories:
            return

        # Max 4 Rows für Kategorien (Row 0-3), Row 4 für Stats
        # Max 5 Buttons pro Row -> max 20 Kategorie-Buttons
        cat_list = list(active_categories.items())[:20]

        for idx, (name, data) in enumerate(cat_list):
            row = idx // 5  # 0-4 pro Row, max Row 3
            if row > 3:
                break  # Sicherheit: nicht mehr als 4 Rows für Kategorien

            emoji_text = data.get("emoji", "")
            label = f"{emoji_text} {name}" if emoji_text else name

            button = discord.ui.Button(
                label=label[:80],
                style=discord.ButtonStyle.primary,
                custom_id=f"log_cat_{name}",
                row=row
            )
            # KEIN Callback - wird über on_interaction gehandelt
            self.add_item(button)

        # Stats-Button in der letzten Row (4)
        stats_button = discord.ui.Button(
            label="Meine Stats",
            style=discord.ButtonStyle.secondary,
            custom_id="log_my_stats",
            row=4
        )
        # KEIN Callback - wird über on_interaction gehandelt
        self.add_item(stats_button)


class AdminPanelView(discord.ui.View):
    """Admin-Panel mit Verwaltungsfunktionen"""

    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(label="Kategorie hinzufügen", style=discord.ButtonStyle.success, custom_id="admin_add_cat", row=0)
    async def add_category(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not is_admin(interaction):
            await interaction.response.send_message(
                embed=discord.Embed(title="Keine Berechtigung", description="Nur Admins!", color=Colors.ERROR),
                ephemeral=True
            )
            return
        modal = AddCategoryModal(admin=interaction.user)
        await interaction.response.send_modal(modal)

    @discord.ui.button(label="Kategorie bearbeiten", style=discord.ButtonStyle.primary, custom_id="admin_edit_cat", row=0)
    async def edit_category(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not is_admin(interaction):
            await interaction.response.send_message(
                embed=discord.Embed(title="Keine Berechtigung", description="Nur Admins!", color=Colors.ERROR),
                ephemeral=True
            )
            return

        if not bot.categories_cache:
            await interaction.response.send_message(
                embed=discord.Embed(title="Keine Kategorien", description="Erstelle zuerst eine Kategorie.", color=Colors.WARNING),
                ephemeral=True
            )
            return

        view = CategorySelectView(mode="edit", admin=interaction.user)
        await interaction.response.send_message(
            embed=discord.Embed(
                title="Kategorie bearbeiten",
                description="Waehle eine Kategorie zum Bearbeiten:",
                color=Colors.ADMIN
            ),
            view=view,
            ephemeral=True
        )

    @discord.ui.button(label="Kategorie deaktivieren", style=discord.ButtonStyle.danger, custom_id="admin_del_cat", row=0)
    async def delete_category(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not is_admin(interaction):
            await interaction.response.send_message(
                embed=discord.Embed(title="Keine Berechtigung", description="Nur Admins!", color=Colors.ERROR),
                ephemeral=True
            )
            return

        if not bot.categories_cache:
            await interaction.response.send_message(
                embed=discord.Embed(title="Keine Kategorien", description="Keine Kategorien vorhanden.", color=Colors.WARNING),
                ephemeral=True
            )
            return

        view = CategorySelectView(mode="delete", admin=interaction.user)
        await interaction.response.send_message(
            embed=discord.Embed(
                title="Kategorie deaktivieren",
                description="Waehle eine Kategorie zum Deaktivieren:",
                color=Colors.ERROR
            ),
            view=view,
            ephemeral=True
        )

    @discord.ui.button(label="Alle Kategorien", style=discord.ButtonStyle.secondary, custom_id="admin_show_cats", row=1)
    async def show_categories(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not is_admin(interaction):
            await interaction.response.send_message(
                embed=discord.Embed(title="Keine Berechtigung", description="Nur Admins!", color=Colors.ERROR),
                ephemeral=True
            )
            return

        embed = discord.Embed(title="Alle Kategorien", color=Colors.ADMIN)

        if not bot.categories_cache:
            embed.description = "Noch keine Kategorien erstellt."
        else:
            for name, data in bot.categories_cache.items():
                emoji = data.get("emoji", "")
                status = "Aktiv" if data.get("aktiv", True) else "Inaktiv"
                beschreibung = data.get("beschreibung", "Keine Beschreibung")
                embed.add_field(
                    name=f"{emoji} {name}" if emoji else name,
                    value=f"Betrag: **{format_currency(data['betrag'])}**\nStatus: **{status}**\n{beschreibung}",
                    inline=True
                )

        embed.set_footer(text=f"Gesamt: {len(bot.categories_cache)} Kategorien")
        await interaction.response.send_message(embed=embed, ephemeral=True)

    @discord.ui.button(label="Wochen-Übersicht", style=discord.ButtonStyle.secondary, custom_id="admin_week_stats", row=1)
    async def week_overview(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not is_admin(interaction):
            await interaction.response.send_message(
                embed=discord.Embed(title="Keine Berechtigung", description="Nur Admins!", color=Colors.ERROR),
                ephemeral=True
            )
            return

        await interaction.response.defer(ephemeral=True)

        current_week = get_current_week()
        all_stats = await get_all_user_stats(week_filter=current_week)

        embed = discord.Embed(
            title=f"Wochen-Übersicht ({current_week})",
            color=Colors.STATS
        )

        if not all_stats:
            embed.description = "Keine Logs diese Woche."
        else:
            sorted_users = sorted(all_stats.items(), key=lambda x: x[1]["betrag"], reverse=True)
            total_logs = 0
            total_betrag = 0

            for i, (user_id, data) in enumerate(sorted_users[:15], 1):
                embed.add_field(
                    name=f"#{i} {data['name']}",
                    value=f"Logs: **{data['logs']}** | Betrag: **{format_currency(data['betrag'])}**",
                    inline=False
                )
                total_logs += data["logs"]
                total_betrag += data["betrag"]

            embed.add_field(
                name="\u2500" * 20,
                value=f"**Gesamt:** {total_logs} Logs | {format_currency(total_betrag)}",
                inline=False
            )

        embed.set_footer(text=f"{BOT_NAME} v{BOT_VERSION}")
        embed.timestamp = datetime.now()
        await interaction.followup.send(embed=embed, ephemeral=True)

    @discord.ui.button(label="Dashboard aktualisieren", style=discord.ButtonStyle.secondary, custom_id="admin_refresh_dash", row=2)
    async def refresh_dashboard(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not is_admin(interaction):
            await interaction.response.send_message(
                embed=discord.Embed(title="Keine Berechtigung", description="Nur Admins!", color=Colors.ERROR),
                ephemeral=True
            )
            return

        await interaction.response.defer(ephemeral=True)
        await update_dashboard()

        await interaction.followup.send(
            embed=discord.Embed(
                title="Dashboard aktualisiert!",
                description=f"Das Google Sheets Dashboard wurde aktualisiert.\n\n[Zum Sheet](https://docs.google.com/spreadsheets/d/{SPREADSHEET_ID}/edit)",
                color=Colors.SUCCESS
            ),
            ephemeral=True
        )

    @discord.ui.button(label="Kategorien neu laden", style=discord.ButtonStyle.secondary, custom_id="admin_reload_cats", row=2)
    async def reload_categories(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Kategorien aus Sheet neu laden (z.B. nach manueller Bearbeitung im Sheet)"""
        if not is_admin(interaction):
            await interaction.response.send_message(
                embed=discord.Embed(title="Keine Berechtigung", description="Nur Admins!", color=Colors.ERROR),
                ephemeral=True
            )
            return

        await interaction.response.defer(ephemeral=True)
        await load_categories()
        await refresh_all_panels()

        active = sum(1 for v in bot.categories_cache.values() if v.get("aktiv", True))
        await interaction.followup.send(
            embed=discord.Embed(
                title="Kategorien neu geladen!",
                description=f"**{len(bot.categories_cache)}** Kategorien geladen ({active} aktiv).\nAlle Panels wurden aktualisiert.",
                color=Colors.SUCCESS
            ),
            ephemeral=True
        )


class CategorySelectView(discord.ui.View):
    """Dropdown für Kategorie-Auswahl"""

    def __init__(self, mode: str = "edit", admin: discord.Member = None):
        super().__init__(timeout=120)
        self.mode = mode
        self.admin = admin

        options = []
        for name, data in bot.categories_cache.items():
            emoji_text = data.get("emoji", "")
            label = f"{emoji_text} {name}" if emoji_text else name
            options.append(
                discord.SelectOption(
                    label=label[:100],
                    value=name,
                    description=f"Betrag: {format_currency(data['betrag'])} | {'Aktiv' if data.get('aktiv', True) else 'Inaktiv'}"
                )
            )

        if options:
            select = discord.ui.Select(
                placeholder="Kategorie waehlen...",
                options=options[:25],
                custom_id=f"cat_select_{mode}"
            )
            select.callback = self.select_callback
            self.add_item(select)

    async def select_callback(self, interaction: discord.Interaction):
        selected = interaction.data["values"][0]
        data = bot.categories_cache.get(selected, {})

        if self.mode == "edit":
            modal = EditCategoryModal(
                kategorie_name=selected,
                current_betrag=data.get("betrag", 0),
                current_emoji=data.get("emoji", ""),
                current_beschreibung=data.get("beschreibung", ""),
                admin=self.admin or interaction.user
            )
            await interaction.response.send_modal(modal)

        elif self.mode == "delete":
            # Bestaetigung vor Deaktivierung
            view = ConfirmDeleteView(kategorie_name=selected, admin=self.admin or interaction.user)
            await interaction.response.send_message(
                embed=discord.Embed(
                    title="Bestaetigung",
                    description=f"Kategorie **{selected}** wirklich deaktivieren?\n\nBestehende Logs bleiben erhalten.",
                    color=Colors.WARNING
                ),
                view=view,
                ephemeral=True
            )


class ConfirmDeleteView(discord.ui.View):
    """Bestaetigung vor Kategorie-Deaktivierung"""

    def __init__(self, kategorie_name: str, admin: discord.Member):
        super().__init__(timeout=30)
        self.kategorie_name = kategorie_name
        self.admin = admin

    @discord.ui.button(label="Ja, deaktivieren", style=discord.ButtonStyle.danger)
    async def confirm(self, interaction: discord.Interaction, button: discord.ui.Button):
        success = await delete_category_from_sheet(self.kategorie_name)

        if success:
            await write_audit_log(
                self.admin, "Kategorie deaktiviert",
                f"Kategorie '{self.kategorie_name}' wurde deaktiviert",
                self.kategorie_name
            )

            embed = discord.Embed(
                title="Kategorie deaktiviert!",
                description=f"**{self.kategorie_name}** wurde deaktiviert.\nPanel wird automatisch aktualisiert.",
                color=Colors.WARNING
            )
            embed.set_footer(text="Im Google Sheet Spalte E auf 'Ja' setzen zum Reaktivieren")
            await interaction.response.edit_message(embed=embed, view=None)

            await refresh_all_panels()
        else:
            await interaction.response.edit_message(
                embed=discord.Embed(
                    title="Fehler",
                    description="Deaktivierung fehlgeschlagen.",
                    color=Colors.ERROR
                ),
                view=None
            )

    @discord.ui.button(label="Abbrechen", style=discord.ButtonStyle.secondary)
    async def cancel(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.edit_message(
            embed=discord.Embed(
                title="Abgebrochen",
                description="Kategorie wurde nicht deaktiviert.",
                color=Colors.NEUTRAL
            ),
            view=None
        )


class PayoutConfirmView(discord.ui.View):
    """Bestaetigung vor Auszahlung"""

    def __init__(self, week: str, selected_users: list, users_data: dict, admin: discord.Member):
        super().__init__(timeout=60)
        self.week = week
        self.selected_users = selected_users
        self.users_data = users_data
        self.admin = admin

    @discord.ui.button(label="Auszahlung bestaetigen", style=discord.ButtonStyle.success, emoji="💰")
    async def confirm(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer(ephemeral=True)

        results = []
        for user_id in self.selected_users:
            data = self.users_data.get(user_id, {})
            if data:
                success = await save_payout(
                    user_id=user_id,
                    username=data["name"],
                    amount=data["betrag"],
                    week=self.week,
                    log_count=data["logs"],
                    admin_name=self.admin.display_name
                )

                if success:
                    await archive_user_logs(int(user_id), self.week)
                    results.append(f"**{data['name']}**: {format_currency(data['betrag'])} ({data['logs']} Logs)")

        # Audit-Log
        total = sum(self.users_data.get(uid, {}).get("betrag", 0) for uid in self.selected_users)
        await write_audit_log(
            self.admin, "Auszahlung durchgefuehrt",
            f"{len(results)} User, Gesamt: {format_currency(total)}",
            self.week
        )

        embed = discord.Embed(
            title=f"Auszahlung abgeschlossen ({self.week})",
            color=Colors.PAYOUT
        )

        if results:
            embed.description = "\n".join(results)
            embed.add_field(
                name="Gesamt",
                value=f"{len(results)} User ausgezahlt | {format_currency(total)}",
                inline=False
            )
        else:
            embed.description = "Keine Auszahlungen durchgefuehrt."

        embed.set_footer(text=f"Von {self.admin.display_name}")
        embed.timestamp = datetime.now()

        await interaction.followup.send(embed=embed, ephemeral=True)
        await update_dashboard()

    @discord.ui.button(label="Abbrechen", style=discord.ButtonStyle.secondary)
    async def cancel(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.edit_message(
            embed=discord.Embed(
                title="Abgebrochen",
                description="Auszahlung wurde nicht durchgefuehrt.",
                color=Colors.NEUTRAL
            ),
            view=None
        )


class PayoutSelectView(discord.ui.View):
    """View für Auszahlungsauswahl"""

    def __init__(self, week: str, users: dict, admin: discord.Member):
        super().__init__(timeout=300)
        self.week = week
        self.users = users
        self.admin = admin

        options = []
        for user_id, data in sorted(users.items(), key=lambda x: x[1]["betrag"], reverse=True):
            if data["betrag"] > 0:
                options.append(
                    discord.SelectOption(
                        label=data["name"][:100],
                        value=str(user_id),
                        description=f"{data['logs']} Logs | {format_currency(data['betrag'])}"
                    )
                )

        if options:
            select = discord.ui.Select(
                placeholder="User für Auszahlung waehlen...",
                options=options[:25],
                max_values=min(len(options), 25),
                custom_id="payout_select"
            )
            select.callback = self.payout_callback
            self.add_item(select)

    async def payout_callback(self, interaction: discord.Interaction):
        if not is_admin(interaction):
            await interaction.response.send_message(
                embed=discord.Embed(title="Keine Berechtigung", description="Nur Admins!", color=Colors.ERROR),
                ephemeral=True
            )
            return

        selected_users = interaction.data["values"]

        # Zusammenfassung anzeigen mit Bestaetigung
        total = sum(self.users.get(uid, {}).get("betrag", 0) for uid in selected_users)
        total_logs = sum(self.users.get(uid, {}).get("logs", 0) for uid in selected_users)

        summary_lines = []
        for uid in selected_users:
            data = self.users.get(uid, {})
            summary_lines.append(f"**{data.get('name', '?')}**: {format_currency(data.get('betrag', 0))} ({data.get('logs', 0)} Logs)")

        embed = discord.Embed(
            title=f"Auszahlung bestaetigen ({self.week})",
            description="\n".join(summary_lines),
            color=Colors.WARNING
        )
        embed.add_field(
            name="Zusammenfassung",
            value=f"**{len(selected_users)} User** | **{total_logs} Logs** | **{format_currency(total)}**",
            inline=False
        )
        embed.set_footer(text="Logs werden nach Auszahlung archiviert!")

        confirm_view = PayoutConfirmView(
            week=self.week,
            selected_users=selected_users,
            users_data=self.users,
            admin=self.admin or interaction.user
        )

        await interaction.response.send_message(embed=embed, view=confirm_view, ephemeral=True)


# ══════════════════════════════════════════════════════════════════════════════
# LOG-CHANNEL BENACHRICHTIGUNG
# ══════════════════════════════════════════════════════════════════════════════

async def send_log_notification(user: discord.Member, kategorie: str, beschreibung: str, betrag: int, emoji: str, image_url: str = ""):
    """Sende Log-Benachrichtigung in den Log-Channel"""
    if not LOG_CHANNEL_ID:
        return

    try:
        channel = bot.get_channel(int(LOG_CHANNEL_ID))
        if not channel:
            return

        log_embed = discord.Embed(title="Neuer Log-Eintrag", color=Colors.LOG)
        log_embed.add_field(name="User", value=user.mention, inline=True)
        log_embed.add_field(name="Kategorie", value=f"{emoji} {kategorie}", inline=True)
        log_embed.add_field(name="Betrag", value=format_currency(betrag), inline=True)
        log_embed.add_field(name="Beschreibung", value=beschreibung[:200], inline=False)
        if image_url:
            log_embed.set_image(url=image_url)
        log_embed.set_footer(text=f"von {user.display_name}")
        log_embed.timestamp = datetime.now()
        await channel.send(embed=log_embed)

    except Exception as e:
        print(f"Log-Channel Fehler: {e}")


# ══════════════════════════════════════════════════════════════════════════════
# SLASH COMMANDS
# ══════════════════════════════════════════════════════════════════════════════

@bot.tree.command(name="setup", description="Erstmalige Bot-Einrichtung (nur Admins)")
async def setup_command(interaction: discord.Interaction):
    if not is_admin(interaction):
        await interaction.response.send_message(
            embed=discord.Embed(title="Keine Berechtigung", description="Nur Admins!", color=Colors.ERROR),
            ephemeral=True
        )
        return

    await interaction.response.defer(ephemeral=True)

    steps = []

    if bot.sheets_service:
        result = await ensure_sheet_tabs()
        steps.append("Google Sheets Tabs erstellt" if result else "Google Sheets Tabs - Fehler!")
    else:
        steps.append("Google Sheets nicht verbunden!")

    # Einstellungen initialisieren
    await init_default_settings()
    steps.append("Einstellungen initialisiert")

    # Kategorien laden
    await load_categories()
    steps.append(f"{len(bot.categories_cache)} Kategorien geladen")

    # Standard-Kategorien erstellen falls leer
    if not bot.categories_cache:
        default_cats = [
            ("Düngen", 10000, "Pflanze", "Plantagen düngen"),
            ("Reparieren", 15000, "Werkzeug", "Fahrzeuge/Gebaeude reparieren"),
            ("Panel", 20000, "Monitor", "Panel platziert"),
        ]
        for name, betrag, emoji_text, beschreibung in default_cats:
            await save_category(name, betrag, emoji_text, beschreibung)
        steps.append(f"{len(default_cats)} Standard-Kategorien erstellt")

    # Audit-Log
    await write_audit_log(interaction.user, "Setup durchgefuehrt", f"{len(steps)} Schritte", "Bot")

    embed = discord.Embed(
        title="Setup abgeschlossen!",
        color=Colors.SUCCESS
    )
    embed.description = "\n".join([f"  {s}" for s in steps])
    embed.add_field(
        name="Naechste Schritte",
        value="1. `/panel` - Log-Panel für User erstellen\n"
              "2. `/admin` - Admin-Panel oeffnen\n"
              "3. `/hilfe` - Alle Befehle anzeigen",
        inline=False
    )
    embed.add_field(
        name="Google Sheet",
        value=f"[Oeffnen](https://docs.google.com/spreadsheets/d/{SPREADSHEET_ID}/edit)",
        inline=False
    )
    embed.set_footer(text=f"{BOT_NAME} v{BOT_VERSION}")

    await interaction.followup.send(embed=embed, ephemeral=True)


@bot.tree.command(name="panel", description="Log-Panel für Mitglieder erstellen")
async def panel_command(interaction: discord.Interaction):
    if not is_admin(interaction):
        await interaction.response.send_message(
            embed=discord.Embed(title="Keine Berechtigung", description="Nur Admins!", color=Colors.ERROR),
            ephemeral=True
        )
        return

    await load_categories()

    embed = build_panel_embed()
    view = LogPanelView()

    await interaction.response.send_message(embed=embed, view=view)

    # Panel-Nachricht merken für Auto-Refresh
    try:
        msg = await interaction.original_response()
        bot.panel_messages[interaction.channel_id] = msg.id
        print(f"Panel registriert: Channel {interaction.channel_id} -> Message {msg.id}")
    except Exception as e:
        print(f"Panel-Registrierung Fehler: {e}")


@bot.tree.command(name="admin", description="Admin-Panel oeffnen (nur Admins)")
async def admin_command(interaction: discord.Interaction):
    if not is_admin(interaction):
        await interaction.response.send_message(
            embed=discord.Embed(title="Keine Berechtigung", description="Nur Admins!", color=Colors.ERROR),
            ephemeral=True
        )
        return

    await load_categories()

    active_count = sum(1 for v in bot.categories_cache.values() if v.get("aktiv", True))

    embed = discord.Embed(
        title="Admin-Panel",
        description=(
            "Verwalte Kategorien, Auszahlungen und Einstellungen.\n\n"
            f"**Aktive Kategorien:** {active_count}\n"
            f"**Gesamt Kategorien:** {len(bot.categories_cache)}\n"
            f"**Registrierte Panels:** {len(bot.panel_messages)}"
        ),
        color=Colors.ADMIN
    )

    embed.add_field(
        name="Verwaltung",
        value=(
            "**Kategorie hinzufügen** - Neue Log-Kategorie erstellen\n"
            "**Kategorie bearbeiten** - Betrag/Infos anpassen\n"
            "**Kategorie deaktivieren** - Aus Panel entfernen"
        ),
        inline=False
    )

    embed.add_field(
        name="Auswertung & Tools",
        value=(
            "**Alle Kategorien** - Übersicht aller Kategorien\n"
            "**Wochen-Übersicht** - Stats der aktuellen Woche\n"
            "**Dashboard aktualisieren** - Google Sheets updaten\n"
            "**Kategorien neu laden** - Aus Sheet synchronisieren"
        ),
        inline=False
    )

    embed.add_field(
        name="Google Sheets",
        value=f"[Zum Sheet](https://docs.google.com/spreadsheets/d/{SPREADSHEET_ID}/edit)\n"
              "Tabs: Logs, Kategorien, Auszahlungen, Dashboard, Archiv, Einstellungen, Audit-Log",
        inline=False
    )

    embed.set_footer(text=f"{BOT_NAME} v{BOT_VERSION}")
    embed.timestamp = datetime.now()

    view = AdminPanelView()
    await interaction.response.send_message(embed=embed, view=view, ephemeral=True)


@bot.tree.command(name="auszahlung", description="Auszahlung für die aktuelle Woche (nur Admins)")
async def payout_command(interaction: discord.Interaction):
    if not is_admin(interaction):
        await interaction.response.send_message(
            embed=discord.Embed(title="Keine Berechtigung", description="Nur Admins!", color=Colors.ERROR),
            ephemeral=True
        )
        return

    await interaction.response.defer(ephemeral=True)

    current_week = get_current_week()
    week_stats = await get_all_user_stats(week_filter=current_week)

    if not week_stats:
        await interaction.followup.send(
            embed=discord.Embed(
                title="Keine Logs",
                description=f"Keine Logs für {current_week} gefunden.",
                color=Colors.WARNING
            ),
            ephemeral=True
        )
        return

    embed = discord.Embed(
        title=f"Auszahlung - {current_week}",
        description="Waehle die User aus, die du auszahlen moechtest:",
        color=Colors.PAYOUT
    )

    total_betrag = 0
    for user_id, data in sorted(week_stats.items(), key=lambda x: x[1]["betrag"], reverse=True):
        if data["betrag"] > 0:
            embed.add_field(
                name=data["name"],
                value=f"Logs: **{data['logs']}** | Betrag: **{format_currency(data['betrag'])}**",
                inline=True
            )
            total_betrag += data["betrag"]

    embed.add_field(
        name="Gesamt",
        value=f"**{format_currency(total_betrag)}**",
        inline=False
    )

    view = PayoutSelectView(week=current_week, users=week_stats, admin=interaction.user)
    await interaction.followup.send(embed=embed, view=view, ephemeral=True)


@bot.tree.command(name="stats", description="Deine persoenlichen Statistiken")
async def stats_command(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True)

    current_week = get_current_week()
    week_stats = await get_user_stats(interaction.user.id, week_filter=current_week)
    total_stats = await get_user_stats(interaction.user.id)

    embed = discord.Embed(
        title=f"Statistiken - {interaction.user.display_name}",
        color=Colors.STATS
    )
    embed.set_thumbnail(url=interaction.user.display_avatar.url)

    embed.add_field(
        name=f"Diese Woche ({current_week})",
        value=f"Logs: **{week_stats['logs']}**\nBetrag: **{format_currency(week_stats['betrag'])}**",
        inline=True
    )

    embed.add_field(
        name="Gesamt (alle Zeiten)",
        value=f"Logs: **{total_stats['logs']}**\nBetrag: **{format_currency(total_stats['betrag'])}**",
        inline=True
    )

    if week_stats['details']:
        details_text = ""
        for kat, data in sorted(week_stats['details'].items(), key=lambda x: x[1]["betrag"], reverse=True):
            emoji = bot.categories_cache.get(kat, {}).get("emoji", "")
            details_text += f"**{emoji} {kat}**: {data['count']}x = {format_currency(data['betrag'])}\n"
        embed.add_field(name="Aufschluesselung (diese Woche)", value=details_text[:1024], inline=False)

    embed.set_footer(text=f"{BOT_NAME} v{BOT_VERSION}")
    embed.timestamp = datetime.now()

    await interaction.followup.send(embed=embed, ephemeral=True)


@bot.tree.command(name="leaderboard", description="Rangliste der aktuellen Woche")
async def leaderboard_command(interaction: discord.Interaction):
    await interaction.response.defer()

    current_week = get_current_week()
    all_stats = await get_all_user_stats(week_filter=current_week)

    embed = discord.Embed(
        title=f"Rangliste - {current_week}",
        color=Colors.STATS
    )

    if not all_stats:
        embed.description = "Noch keine Logs diese Woche."
    else:
        sorted_users = sorted(all_stats.items(), key=lambda x: x[1]["betrag"], reverse=True)

        medals = ["🥇", "🥈", "🥉"]
        description_lines = []

        for i, (user_id, data) in enumerate(sorted_users[:10]):
            rank = medals[i] if i < 3 else f"#{i + 1}"
            description_lines.append(
                f"{rank} **{data['name']}** - {data['logs']} Logs | {format_currency(data['betrag'])}"
            )

        embed.description = "\n".join(description_lines)

        total_logs = sum(d["logs"] for d in all_stats.values())
        total_betrag = sum(d["betrag"] for d in all_stats.values())
        embed.add_field(
            name="Gesamt",
            value=f"**{len(all_stats)}** aktive User | **{total_logs}** Logs | **{format_currency(total_betrag)}**",
            inline=False
        )

    embed.set_footer(text=f"{BOT_NAME} v{BOT_VERSION}")
    embed.timestamp = datetime.now()

    await interaction.followup.send(embed=embed)


@bot.tree.command(name="log", description="Schnell-Log per Slash-Command")
@app_commands.describe(
    kategorie="Waehle eine Kategorie",
    beschreibung="Was hast du gemacht?",
    bild="Screenshot/Beweis (optional)"
)
async def log_command(
    interaction: discord.Interaction,
    kategorie: str,
    beschreibung: str,
    bild: discord.Attachment = None
):
    await interaction.response.defer(ephemeral=True)

    # Rate-Limit
    remaining = check_cooldown(interaction.user.id)
    if remaining is not None:
        await interaction.followup.send(
            embed=discord.Embed(
                title="Cooldown aktiv",
                description=f"Du kannst in **{remaining} Sekunden** wieder loggen.",
                color=Colors.WARNING
            ),
            ephemeral=True
        )
        return

    # Kategorie validieren
    if kategorie not in bot.categories_cache:
        active_cats = [k for k, v in bot.categories_cache.items() if v.get("aktiv", True)]
        await interaction.followup.send(
            embed=discord.Embed(
                title="Ungueltige Kategorie",
                description=f"Verfuegbare Kategorien: {', '.join(active_cats)}",
                color=Colors.ERROR
            ),
            ephemeral=True
        )
        return

    if not bot.categories_cache[kategorie].get("aktiv", True):
        await interaction.followup.send(
            embed=discord.Embed(title="Kategorie inaktiv", description="Diese Kategorie ist deaktiviert.", color=Colors.WARNING),
            ephemeral=True
        )
        return

    # Bild validieren
    image_url = ""
    if bild:
        if bild.content_type and bild.content_type.startswith('image/'):
            image_url = bild.url
        else:
            # Trotzdem akzeptieren, aber warnen
            image_url = bild.url

    row_number = await save_log_entry(
        user=interaction.user,
        kategorie=kategorie,
        beschreibung=beschreibung,
        image_url=image_url
    )

    if row_number:
        set_cooldown(interaction.user.id)

        betrag = bot.categories_cache[kategorie]["betrag"]
        emoji = bot.categories_cache[kategorie].get("emoji", "")

        embed = discord.Embed(title="Log gespeichert!", color=Colors.SUCCESS)
        embed.add_field(name="Kategorie", value=f"{emoji} {kategorie}", inline=True)
        embed.add_field(name="Betrag", value=format_currency(betrag), inline=True)
        embed.add_field(name="Beschreibung", value=beschreibung[:200], inline=False)
        if image_url:
            embed.set_thumbnail(url=image_url)
        embed.set_footer(text=f"{BOT_NAME} v{BOT_VERSION}")
        embed.timestamp = datetime.now()

        await interaction.followup.send(embed=embed, ephemeral=True)

        # Log-Channel
        await send_log_notification(
            interaction.user, kategorie, beschreibung, betrag, emoji, image_url
        )
    else:
        await interaction.followup.send(
            embed=discord.Embed(title="Fehler", description="Log konnte nicht gespeichert werden.", color=Colors.ERROR),
            ephemeral=True
        )


@log_command.autocomplete('kategorie')
async def log_category_autocomplete(interaction: discord.Interaction, current: str):
    """Autocomplete für Kategorien"""
    active_cats = [
        app_commands.Choice(name=f"{v.get('emoji', '')} {k} ({format_currency(v['betrag'])})", value=k)
        for k, v in bot.categories_cache.items()
        if v.get("aktiv", True) and current.lower() in k.lower()
    ]
    return active_cats[:25]


@bot.tree.command(name="kategorien", description="Zeige alle verfuegbaren Kategorien")
async def kategorien_command(interaction: discord.Interaction):
    embed = discord.Embed(title="Verfuegbare Kategorien", color=Colors.INFO)

    active_cats = {k: v for k, v in bot.categories_cache.items() if v.get("aktiv", True)}

    if not active_cats:
        embed.description = "Keine aktiven Kategorien vorhanden."
    else:
        for name, data in active_cats.items():
            emoji = data.get("emoji", "")
            beschreibung = data.get("beschreibung", "Keine Beschreibung")
            embed.add_field(
                name=f"{emoji} {name}",
                value=f"Betrag: **{format_currency(data['betrag'])}**\n{beschreibung}",
                inline=True
            )

    embed.set_footer(text=f"{len(active_cats)} aktive Kategorien | {BOT_NAME} v{BOT_VERSION}")
    await interaction.response.send_message(embed=embed, ephemeral=True)


@bot.tree.command(name="hilfe", description="Zeige alle verfuegbaren Befehle")
async def help_command(interaction: discord.Interaction):
    embed = discord.Embed(
        title=f"{BOT_NAME} - Hilfe",
        description="Alle verfuegbaren Befehle:",
        color=Colors.INFO
    )

    embed.add_field(
        name="User-Befehle",
        value=(
            "**/stats** - Deine persoenlichen Statistiken\n"
            "**/log** - Schnell-Log mit Screenshot-Upload\n"
            "**/kategorien** - Verfuegbare Kategorien anzeigen\n"
            "**/leaderboard** - Rangliste der Woche\n"
            "**/hilfe** - Diese Hilfe anzeigen"
        ),
        inline=False
    )

    if is_admin(interaction):
        embed.add_field(
            name="Admin-Befehle",
            value=(
                "**/setup** - Erstmalige Bot-Einrichtung\n"
                "**/panel** - Log-Panel für Mitglieder erstellen\n"
                "**/admin** - Admin-Panel oeffnen\n"
                "**/auszahlung** - Woechentliche Auszahlung"
            ),
            inline=False
        )

    embed.add_field(
        name="Screenshot-Upload",
        value="Per **/log** direkt als Anhang, oder im Panel über den 📷-Button nach dem Loggen.",
        inline=False
    )

    embed.set_footer(text=f"{BOT_NAME} v{BOT_VERSION}")
    await interaction.response.send_message(embed=embed, ephemeral=True)


# ══════════════════════════════════════════════════════════════════════════════
# BOT EVENTS
# ══════════════════════════════════════════════════════════════════════════════

@bot.event
async def on_ready():
    """Bot ist bereit"""
    print(f"""
╔══════════════════════════════════════════════════════════╗
║          {BOT_NAME} v{BOT_VERSION}                       
║          Bot ist online!                                 
║          Eingeloggt als: {bot.user.name}                 
║          Server: {len(bot.guilds)}                       
╚══════════════════════════════════════════════════════════╝
    """)

    # Google Sheets initialisieren
    bot.sheets_service = init_google_sheets()

    if bot.sheets_service:
        print("Google Sheets verbunden - initialisiere...")
        await ensure_sheet_tabs()
        await load_categories()
        await init_default_settings()
        await load_settings()
        print(f"{len(bot.categories_cache)} Kategorien geladen")
        print(f"{len(bot.settings_cache)} Einstellungen geladen")
    else:
        print("WARNUNG: Google Sheets nicht verbunden!")

    # Slash Commands synchronisieren (nur wenn noetig)
    try:
        synced = await bot.tree.sync()
        print(f"{len(synced)} Slash-Commands synchronisiert")
    except Exception as e:
        print(f"Sync Fehler: {e}")

    # Persistente Views registrieren
    # AdminPanelView hat feste custom_ids -> sicher persistent
    bot.add_view(AdminPanelView())
    # LogPanelView wird NICHT registriert - on_interaction behandelt
    # alle log_cat_* und log_my_stats custom_ids dynamisch.
    # Das ist sicherer nach Restarts wenn Kategorien sich geaendert haben.

    # Background Tasks starten
    if not auto_dashboard_update.is_running():
        auto_dashboard_update.start()

    # Status setzen
    await bot.change_presence(
        activity=discord.Activity(
            type=discord.ActivityType.watching,
            name="/hilfe | Log Bot v2"
        )
    )


@bot.event
async def on_interaction(interaction: discord.Interaction):
    """
    Dynamischer Interaction-Handler für persistente Buttons.
    Faengt ALLE log_cat_* und log_my_stats Custom-IDs ab,
    auch wenn die Kategorien sich seit dem Panel-Post geaendert haben.
    Dies ist der Fix für das Problem, dass Buttons nach Bot-Restart
    oder Kategorie-Aenderungen nicht mehr reagieren.
    """
    if interaction.type != discord.InteractionType.component:
        return

    custom_id = interaction.data.get("custom_id", "")

    # Log-Kategorie Buttons: "log_cat_<name>"
    if custom_id.startswith("log_cat_"):
        kategorie = custom_id[8:]  # Alles nach "log_cat_"

        # Cooldown pruefen
        remaining = check_cooldown(interaction.user.id)
        if remaining is not None:
            await interaction.response.send_message(
                embed=discord.Embed(
                    title="Cooldown aktiv",
                    description=f"Du kannst in **{remaining} Sekunden** wieder loggen.",
                    color=Colors.WARNING
                ),
                ephemeral=True
            )
            return

        # Pruefen ob Kategorie existiert und aktiv ist
        cat_data = bot.categories_cache.get(kategorie)
        if not cat_data or not cat_data.get("aktiv", True):
            await interaction.response.send_message(
                embed=discord.Embed(
                    title="Kategorie nicht verfuegbar",
                    description=f"Die Kategorie **{kategorie}** ist nicht mehr aktiv.\nBitte nutze das aktualisierte Panel.",
                    color=Colors.WARNING
                ),
                ephemeral=True
            )
            return

        # Modal oeffnen
        modal = LogModal(kategorie)
        await interaction.response.send_modal(modal)
        return

    # Stats-Button
    if custom_id == "log_my_stats":
        await interaction.response.defer(ephemeral=True)

        current_week = get_current_week()
        stats = await get_user_stats(interaction.user.id, week_filter=current_week)
        total_stats = await get_user_stats(interaction.user.id)

        embed = discord.Embed(
            title=f"Statistiken - {interaction.user.display_name}",
            color=Colors.STATS
        )

        embed.add_field(
            name=f"Aktuelle Woche ({current_week})",
            value=f"Logs: **{stats['logs']}**\nBetrag: **{format_currency(stats['betrag'])}**",
            inline=True
        )

        embed.add_field(
            name="Gesamt",
            value=f"Logs: **{total_stats['logs']}**\nBetrag: **{format_currency(total_stats['betrag'])}**",
            inline=True
        )

        if stats['details']:
            details_text = ""
            for kat, data in stats['details'].items():
                emoji = bot.categories_cache.get(kat, {}).get("emoji", "")
                details_text += f"{emoji} **{kat}**: {data['count']}x ({format_currency(data['betrag'])})\n"
            embed.add_field(name="Details (diese Woche)", value=details_text[:1024], inline=False)

        embed.set_footer(text=f"{BOT_NAME} v{BOT_VERSION}")
        embed.timestamp = datetime.now()

        await interaction.followup.send(embed=embed, ephemeral=True)
        return


@bot.event
async def on_guild_join(guild):
    """Bot wurde zu einem Server hinzugefuegt"""
    print(f"Bot zu Server hinzugefuegt: {guild.name} (ID: {guild.id})")


# ══════════════════════════════════════════════════════════════════════════════
# BACKGROUND TASKS
# ══════════════════════════════════════════════════════════════════════════════

@tasks.loop(hours=1)
async def auto_dashboard_update():
    """Aktualisiere Dashboard automatisch"""
    try:
        await update_dashboard()
    except Exception as e:
        print(f"Auto-Dashboard Fehler: {e}")


@auto_dashboard_update.before_loop
async def before_dashboard():
    await bot.wait_until_ready()


# ══════════════════════════════════════════════════════════════════════════════
# ERROR HANDLING
# ══════════════════════════════════════════════════════════════════════════════

@bot.tree.error
async def on_app_command_error(interaction: discord.Interaction, error: app_commands.AppCommandError):
    """Globaler Error Handler für Slash Commands"""
    print(f"Command Error: {error}")
    traceback.print_exc()

    embed = discord.Embed(
        title="Ein Fehler ist aufgetreten",
        description=f"```{str(error)[:500]}```",
        color=Colors.ERROR
    )
    embed.set_footer(text="Bitte versuche es erneut oder kontaktiere einen Admin.")

    try:
        if interaction.response.is_done():
            await interaction.followup.send(embed=embed, ephemeral=True)
        else:
            await interaction.response.send_message(embed=embed, ephemeral=True)
    except Exception:
        pass


# ══════════════════════════════════════════════════════════════════════════════
# START
# ══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    print(f"""
╔══════════════════════════════════════════════════════════╗
║          {BOT_NAME} v{BOT_VERSION}                       ║ 
║          Starte Bot...                                   ║ 
╚══════════════════════════════════════════════════════════╝
    """)

    if not DISCORD_TOKEN:
        print("FEHLER: DISCORD_TOKEN nicht gesetzt!")
        print("Setze die Environment Variable DISCORD_TOKEN")
        exit(1)

    if not SPREADSHEET_ID:
        print("WARNUNG: GOOGLE_SHEET_ID nicht gesetzt!")

    if not GOOGLE_CREDENTIALS_BASE64:
        print("WARNUNG: GOOGLE_CREDENTIALS_BASE64 nicht gesetzt!")

    bot.run(DISCORD_TOKEN)
