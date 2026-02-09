"""
╔══════════════════════════════════════════════════════════════════════════════════╗
║                                                                                  ║
║     ██╗   ██╗███╗   ██╗██╗██╗   ██╗███████╗██████╗ ███████╗ █████╗ ██╗          ║
║     ██║   ██║████╗  ██║██║██║   ██║██╔════╝██╔══██╗██╔════╝██╔══██╗██║          ║
║     ██║   ██║██╔██╗ ██║██║██║   ██║█████╗  ██████╔╝███████╗███████║██║          ║
║     ██║   ██║██║╚██╗██║██║╚██╗ ██╔╝██╔══╝  ██╔══██╗╚════██║██╔══██║██║          ║
║     ╚██████╔╝██║ ╚████║██║ ╚████╔╝ ███████╗██║  ██║███████║██║  ██║███████╗     ║
║      ╚═════╝ ╚═╝  ╚═══╝╚═╝  ╚═══╝  ╚══════╝╚═╝  ╚═╝╚══════╝╚═╝  ╚═╝╚══════╝     ║
║                                                                                  ║
║              ██╗      ██████╗  ██████╗     ██████╗  ██████╗ ████████╗            ║
║              ██║     ██╔═══██╗██╔════╝     ██╔══██╗██╔═══██╗╚══██╔══╝            ║
║              ██║     ██║   ██║██║  ███╗    ██████╔╝██║   ██║   ██║               ║
║              ██║     ██║   ██║██║   ██║    ██╔══██╗██║   ██║   ██║               ║
║              ███████╗╚██████╔╝╚██████╔╝    ██████╔╝╚██████╔╝   ██║               ║
║              ╚══════╝ ╚═════╝  ╚═════╝     ╚═════╝  ╚═════╝    ╚═╝               ║
║                                                                                  ║
║                    Universal Log Bot - Admin Edition                              ║
║                           Version 1.0.0                                          ║
║                                                                                  ║
║        Dynamisches Log-System mit Admin-Panel & Google Sheets                    ║
║                                                                                  ║
╚══════════════════════════════════════════════════════════════════════════════════╝

Features:
  - Dynamische Log-Kategorien (per Discord hinzufuegen/bearbeiten/loeschen)
  - Anpassbare Auszahlungsbetraege pro Kategorie
  - Interaktives Panel-System mit Buttons
  - Statistiken & Dashboard
  - Google Sheets Admin-Panel
  - Automatische Auszahlungsberechnung
  - Woechentliche Abrechnung mit Archiv
  - Modernes, neutrales Design
"""

import discord
from discord.ext import commands, tasks
from discord import app_commands
import os
import json
import base64
from datetime import datetime, timedelta
from collections import defaultdict
import asyncio
import traceback

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
BOT_VERSION = "1.0.0"

# Farben - Modernes, neutrales Design
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

# Google Sheets Tab-Namen (KEINE Emojis - verhindert API-Fehler!)
SHEET_TABS = {
    "logs": "Logs",
    "kategorien": "Kategorien",
    "auszahlungen": "Auszahlungen",
    "dashboard": "Dashboard",
    "archiv": "Archiv",
    "einstellungen": "Einstellungen"
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
bot.categories_cache = {}   # {name: {"betrag": int, "emoji": str, "beschreibung": str, "aktiv": bool}}
bot.settings_cache = {}     # Allgemeine Einstellungen


# ══════════════════════════════════════════════════════════════════════════════
# GOOGLE SHEETS SERVICE
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


# ══════════════════════════════════════════════════════════════════════════════
# SHEETS HELPER FUNKTIONEN
# ══════════════════════════════════════════════════════════════════════════════

async def ensure_sheet_tabs():
    """Erstelle alle benoetigten Tabs falls sie nicht existieren"""
    if not bot.sheets_service:
        return False

    try:
        sheet = bot.sheets_service.spreadsheets()
        spreadsheet = sheet.get(spreadsheetId=SPREADSHEET_ID).execute()
        existing_tabs = [s['properties']['title'] for s in spreadsheet.get('sheets', [])]

        tabs_to_create = []
        for key, tab_name in SHEET_TABS.items():
            if tab_name not in existing_tabs:
                tabs_to_create.append({
                    'addSheet': {
                        'properties': {
                            'title': tab_name
                        }
                    }
                })

        if tabs_to_create:
            sheet.batchUpdate(
                spreadsheetId=SPREADSHEET_ID,
                body={'requests': tabs_to_create}
            ).execute()
            print(f"{len(tabs_to_create)} Tab(s) erstellt")

        # Header setzen
        await set_sheet_headers()
        return True

    except Exception as e:
        print(f"Tab-Erstellung Fehler: {e}")
        traceback.print_exc()
        return False


async def set_sheet_headers():
    """Setze Header fuer alle Tabs"""
    if not bot.sheets_service:
        return

    try:
        sheet = bot.sheets_service.spreadsheets()

        headers = {
            f"{SHEET_TABS['logs']}!A1:I1": [[
                "Zeitstempel", "Kalenderwoche", "User", "User-ID",
                "Kategorie", "Beschreibung", "Betrag", "Bild-URL", "Log-ID"
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
                "Kategorie", "Beschreibung", "Betrag", "Bild-URL", "Log-ID", "Archiviert am"
            ]],
            f"{SHEET_TABS['einstellungen']}!A1:B1": [[
                "Einstellung", "Wert"
            ]]
        }

        for range_name, values in headers.items():
            # Pruefe ob Header schon existiert
            try:
                existing = sheet.values().get(
                    spreadsheetId=SPREADSHEET_ID,
                    range=range_name
                ).execute()
                if existing.get('values'):
                    continue  # Header existiert bereits
            except:
                pass

            sheet.values().update(
                spreadsheetId=SPREADSHEET_ID,
                range=range_name,
                valueInputOption='USER_ENTERED',
                body={'values': values}
            ).execute()

        print("Sheet-Header gesetzt")

    except Exception as e:
        print(f"Header Fehler: {e}")


async def load_categories():
    """Lade Kategorien aus Google Sheets"""
    if not bot.sheets_service:
        return {}

    try:
        sheet = bot.sheets_service.spreadsheets()
        result = sheet.values().get(
            spreadsheetId=SPREADSHEET_ID,
            range=f"{SHEET_TABS['kategorien']}!A2:F"
        ).execute()

        values = result.get('values', [])
        categories = {}

        for row in values:
            if len(row) >= 5:
                name = row[0]
                categories[name] = {
                    "betrag": int(row[1]) if row[1].isdigit() else 0,
                    "emoji": row[2] if len(row) > 2 else "",
                    "beschreibung": row[3] if len(row) > 3 else "",
                    "aktiv": row[4].lower() == "ja" if len(row) > 4 else True
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
        result = sheet.values().get(
            spreadsheetId=SPREADSHEET_ID,
            range=f"{SHEET_TABS['kategorien']}!A2:A"
        ).execute()
        existing = result.get('values', [])
        row_index = None

        for i, row in enumerate(existing):
            if row and row[0] == name:
                row_index = i + 2  # +2 wegen Header und 0-Index
                break

        values = [[
            name,
            betrag,
            emoji,
            beschreibung,
            "Ja" if aktiv else "Nein",
            datetime.now().strftime("%d.%m.%Y %H:%M")
        ]]

        if row_index:
            # Update existierende Kategorie
            sheet.values().update(
                spreadsheetId=SPREADSHEET_ID,
                range=f"{SHEET_TABS['kategorien']}!A{row_index}:F{row_index}",
                valueInputOption='USER_ENTERED',
                body={'values': values}
            ).execute()
        else:
            # Neue Kategorie hinzufuegen
            sheet.values().append(
                spreadsheetId=SPREADSHEET_ID,
                range=f"{SHEET_TABS['kategorien']}!A:F",
                valueInputOption='USER_ENTERED',
                body={'values': values}
            ).execute()

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
    """Loesche eine Kategorie aus Google Sheets (setzt auf Inaktiv)"""
    if not bot.sheets_service:
        return False

    try:
        sheet = bot.sheets_service.spreadsheets()

        result = sheet.values().get(
            spreadsheetId=SPREADSHEET_ID,
            range=f"{SHEET_TABS['kategorien']}!A2:A"
        ).execute()
        existing = result.get('values', [])

        for i, row in enumerate(existing):
            if row and row[0] == name:
                row_index = i + 2
                sheet.values().update(
                    spreadsheetId=SPREADSHEET_ID,
                    range=f"{SHEET_TABS['kategorien']}!E{row_index}",
                    valueInputOption='USER_ENTERED',
                    body={'values': [["Nein"]]}
                ).execute()
                break

        # Cache aktualisieren
        if name in bot.categories_cache:
            bot.categories_cache[name]["aktiv"] = False

        return True

    except Exception as e:
        print(f"Kategorie loeschen Fehler: {e}")
        return False


async def save_log_entry(user: discord.Member, kategorie: str, beschreibung: str, image_url: str = ""):
    """Speichere einen Log-Eintrag in Google Sheets"""
    if not bot.sheets_service:
        return False

    try:
        sheet = bot.sheets_service.spreadsheets()

        now = datetime.now()
        timestamp = now.strftime("%d.%m.%Y %H:%M:%S")
        week_number = now.isocalendar()[1]
        year = now.year

        betrag = bot.categories_cache.get(kategorie, {}).get("betrag", 0)
        log_id = f"LOG-{now.strftime('%Y%m%d%H%M%S')}-{user.id}"

        values = [[
            timestamp,
            f"KW{week_number}/{year}",
            user.display_name,
            str(user.id),
            kategorie,
            beschreibung,
            betrag,
            image_url,
            log_id
        ]]

        sheet.values().append(
            spreadsheetId=SPREADSHEET_ID,
            range=f"{SHEET_TABS['logs']}!A:I",
            valueInputOption='USER_ENTERED',
            body={'values': values}
        ).execute()

        print(f"Log gespeichert: {user.display_name} - {kategorie}")
        return True

    except Exception as e:
        print(f"Log speichern Fehler: {e}")
        traceback.print_exc()
        return False


async def get_user_stats(user_id: int, week_filter: str = None):
    """Hole Statistiken fuer einen User"""
    if not bot.sheets_service:
        return {"logs": 0, "betrag": 0, "details": {}}

    try:
        sheet = bot.sheets_service.spreadsheets()
        result = sheet.values().get(
            spreadsheetId=SPREADSHEET_ID,
            range=f"{SHEET_TABS['logs']}!A2:I"
        ).execute()

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
    """Hole Statistiken fuer alle User"""
    if not bot.sheets_service:
        return {}

    try:
        sheet = bot.sheets_service.spreadsheets()
        result = sheet.values().get(
            spreadsheetId=SPREADSHEET_ID,
            range=f"{SHEET_TABS['logs']}!A2:I"
        ).execute()

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

        sheet.values().append(
            spreadsheetId=SPREADSHEET_ID,
            range=f"{SHEET_TABS['auszahlungen']}!A:H",
            valueInputOption='USER_ENTERED',
            body={'values': values}
        ).execute()

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
        result = sheet.values().get(
            spreadsheetId=SPREADSHEET_ID,
            range=f"{SHEET_TABS['logs']}!A2:I"
        ).execute()

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
            # Ins Archiv schreiben
            sheet.values().append(
                spreadsheetId=SPREADSHEET_ID,
                range=f"{SHEET_TABS['archiv']}!A:J",
                valueInputOption='USER_ENTERED',
                body={'values': logs_to_archive}
            ).execute()

            # Logs-Tab aktualisieren (nur nicht-archivierte behalten)
            # Erst alles loeschen, dann neu schreiben
            sheet.values().clear(
                spreadsheetId=SPREADSHEET_ID,
                range=f"{SHEET_TABS['logs']}!A2:I"
            ).execute()

            if rows_to_keep:
                sheet.values().update(
                    spreadsheetId=SPREADSHEET_ID,
                    range=f"{SHEET_TABS['logs']}!A2:I",
                    valueInputOption='USER_ENTERED',
                    body={'values': rows_to_keep}
                ).execute()

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

        # Alle Stats holen
        all_stats = await get_all_user_stats()
        week_stats = await get_all_user_stats(week_filter=current_week)

        sheet = bot.sheets_service.spreadsheets()

        # Dashboard leeren (nicht Header)
        sheet.values().clear(
            spreadsheetId=SPREADSHEET_ID,
            range=f"{SHEET_TABS['dashboard']}!A2:F"
        ).execute()

        # Alle User zusammenfuehren
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

        # Sortieren nach aktuelle KW Betrag (absteigend)
        dashboard_data.sort(key=lambda x: int(str(x[3]).replace(".", "").replace("$", "") or 0), reverse=True)

        if dashboard_data:
            sheet.values().update(
                spreadsheetId=SPREADSHEET_ID,
                range=f"{SHEET_TABS['dashboard']}!A2:F",
                valueInputOption='USER_ENTERED',
                body={'values': dashboard_data}
            ).execute()

        print("Dashboard aktualisiert")

    except Exception as e:
        print(f"Dashboard Update Fehler: {e}")


# ══════════════════════════════════════════════════════════════════════════════
# HILFSFUNKTIONEN
# ══════════════════════════════════════════════════════════════════════════════

def is_admin(interaction: discord.Interaction) -> bool:
    """Pruefe ob der User Admin-Rechte hat"""
    if interaction.user.guild_permissions.administrator:
        return True
    for role in interaction.user.roles:
        if role.name.lower() == ADMIN_ROLE_NAME.lower():
            return True
    return False


def get_current_week() -> str:
    """Hole aktuelle Kalenderwoche als String"""
    now = datetime.now()
    return f"KW{now.isocalendar()[1]}/{now.year}"


def format_currency(amount: int) -> str:
    """Formatiere Betrag als Waehrung"""
    return f"{amount:,}$".replace(",", ".")


# ══════════════════════════════════════════════════════════════════════════════
# UI KOMPONENTEN - MODALS
# ══════════════════════════════════════════════════════════════════════════════

class LogModal(discord.ui.Modal, title="Log eintragen"):
    """Modal fuer Log-Eintraege"""

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

    bild_url = discord.ui.TextInput(
        label="Bild-URL (optional)",
        placeholder="Link zum Screenshot/Beweis",
        style=discord.TextStyle.short,
        required=False,
        max_length=500
    )

    async def on_submit(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)

        image_url = str(self.bild_url) if self.bild_url else ""
        beschreibung_text = str(self.beschreibung)

        success = await save_log_entry(
            user=interaction.user,
            kategorie=self.kategorie,
            beschreibung=beschreibung_text,
            image_url=image_url
        )

        if success:
            betrag = bot.categories_cache.get(self.kategorie, {}).get("betrag", 0)
            emoji = bot.categories_cache.get(self.kategorie, {}).get("emoji", "")

            embed = discord.Embed(
                title="Log erfolgreich!",
                color=Colors.SUCCESS
            )
            embed.add_field(name="Kategorie", value=f"{emoji} {self.kategorie}", inline=True)
            embed.add_field(name="Betrag", value=format_currency(betrag), inline=True)
            embed.add_field(name="Beschreibung", value=beschreibung_text[:100], inline=False)

            if image_url:
                embed.set_thumbnail(url=image_url)

            embed.set_footer(text=f"{BOT_NAME} v{BOT_VERSION}")
            embed.timestamp = datetime.now()

            await interaction.followup.send(embed=embed, ephemeral=True)

            # Log-Nachricht in Log-Channel senden
            if LOG_CHANNEL_ID:
                try:
                    channel = bot.get_channel(int(LOG_CHANNEL_ID))
                    if channel:
                        log_embed = discord.Embed(
                            title="Neuer Log-Eintrag",
                            color=Colors.LOG
                        )
                        log_embed.add_field(name="User", value=interaction.user.mention, inline=True)
                        log_embed.add_field(name="Kategorie", value=f"{emoji} {self.kategorie}", inline=True)
                        log_embed.add_field(name="Betrag", value=format_currency(betrag), inline=True)
                        log_embed.add_field(name="Beschreibung", value=beschreibung_text[:200], inline=False)
                        if image_url:
                            log_embed.set_image(url=image_url)
                        log_embed.set_footer(text=f"von {interaction.user.display_name}")
                        log_embed.timestamp = datetime.now()
                        await channel.send(embed=log_embed)
                except Exception as e:
                    print(f"Log-Channel Fehler: {e}")
        else:
            embed = discord.Embed(
                title="Fehler",
                description="Log konnte nicht gespeichert werden. Bitte versuche es erneut.",
                color=Colors.ERROR
            )
            await interaction.followup.send(embed=embed, ephemeral=True)


class AddCategoryModal(discord.ui.Modal, title="Neue Kategorie erstellen"):
    """Modal zum Erstellen einer neuen Kategorie"""

    name = discord.ui.TextInput(
        label="Name der Kategorie",
        placeholder="z.B. Duengen, Reparieren, Panel",
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
        placeholder="z.B. Duengen, Hammer, Pflanze",
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

        name = str(self.name).strip()
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

        success = await save_category(name, betrag, emoji, beschreibung, True)

        if success:
            embed = discord.Embed(
                title="Kategorie erstellt!",
                color=Colors.SUCCESS
            )
            embed.add_field(name="Name", value=f"{emoji} {name}" if emoji else name, inline=True)
            embed.add_field(name="Betrag", value=format_currency(betrag), inline=True)
            if beschreibung:
                embed.add_field(name="Beschreibung", value=beschreibung, inline=False)
            embed.set_footer(text="Kategorie ist sofort im Panel verfuegbar!")
            await interaction.followup.send(embed=embed, ephemeral=True)
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

    def __init__(self, kategorie_name: str, current_betrag: int, current_emoji: str, current_beschreibung: str):
        super().__init__()
        self.kategorie_name = kategorie_name
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

        success = await save_category(self.kategorie_name, betrag, emoji, beschreibung, True)

        if success:
            embed = discord.Embed(
                title="Kategorie aktualisiert!",
                color=Colors.SUCCESS
            )
            embed.add_field(name="Kategorie", value=self.kategorie_name, inline=True)
            embed.add_field(name="Neuer Betrag", value=format_currency(betrag), inline=True)
            embed.set_footer(text="Aenderungen sind sofort aktiv!")
            await interaction.followup.send(embed=embed, ephemeral=True)
        else:
            await interaction.followup.send(
                embed=discord.Embed(title="Fehler", description="Update fehlgeschlagen.", color=Colors.ERROR),
                ephemeral=True
            )


# ══════════════════════════════════════════════════════════════════════════════
# UI KOMPONENTEN - VIEWS
# ══════════════════════════════════════════════════════════════════════════════

class LogPanelView(discord.ui.View):
    """Interaktives Log-Panel mit dynamischen Buttons"""

    def __init__(self):
        super().__init__(timeout=None)
        self.update_buttons()

    def update_buttons(self):
        """Erstelle Buttons basierend auf aktiven Kategorien"""
        self.clear_items()

        active_categories = {
            k: v for k, v in bot.categories_cache.items() if v.get("aktiv", True)
        }

        if not active_categories:
            return

        # Kategorie-Buttons
        for name, data in active_categories.items():
            emoji_text = data.get("emoji", "")
            label = f"{emoji_text} {name}" if emoji_text else name
            button = discord.ui.Button(
                label=label[:80],
                style=discord.ButtonStyle.primary,
                custom_id=f"log_category_{name}",
                row=0 if list(active_categories.keys()).index(name) < 4 else 1
            )
            button.callback = self.make_log_callback(name)
            self.add_item(button)

        # Stats-Button
        stats_button = discord.ui.Button(
            label="Meine Stats",
            style=discord.ButtonStyle.secondary,
            custom_id="log_my_stats",
            row=2
        )
        stats_button.callback = self.stats_callback
        self.add_item(stats_button)

    def make_log_callback(self, kategorie: str):
        async def callback(interaction: discord.Interaction):
            modal = LogModal(kategorie)
            await interaction.response.send_modal(modal)
        return callback

    async def stats_callback(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)

        current_week = get_current_week()
        stats = await get_user_stats(interaction.user.id, week_filter=current_week)
        total_stats = await get_user_stats(interaction.user.id)

        embed = discord.Embed(
            title=f"Statistiken - {interaction.user.display_name}",
            color=Colors.STATS
        )

        # Aktuelle Woche
        embed.add_field(
            name=f"Aktuelle Woche ({current_week})",
            value=f"Logs: **{stats['logs']}**\nBetrag: **{format_currency(stats['betrag'])}**",
            inline=True
        )

        # Gesamt
        embed.add_field(
            name="Gesamt",
            value=f"Logs: **{total_stats['logs']}**\nBetrag: **{format_currency(total_stats['betrag'])}**",
            inline=True
        )

        # Details pro Kategorie (aktuelle Woche)
        if stats['details']:
            details_text = ""
            for kat, data in stats['details'].items():
                emoji = bot.categories_cache.get(kat, {}).get("emoji", "")
                details_text += f"{emoji} **{kat}**: {data['count']}x ({format_currency(data['betrag'])})\n"
            embed.add_field(name="Details (diese Woche)", value=details_text[:1024], inline=False)

        embed.set_footer(text=f"{BOT_NAME} v{BOT_VERSION}")
        embed.timestamp = datetime.now()

        await interaction.followup.send(embed=embed, ephemeral=True)


class AdminPanelView(discord.ui.View):
    """Admin-Panel mit Verwaltungsfunktionen"""

    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(label="Kategorie hinzufuegen", style=discord.ButtonStyle.success, custom_id="admin_add_cat", row=0)
    async def add_category(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not is_admin(interaction):
            await interaction.response.send_message(
                embed=discord.Embed(title="Keine Berechtigung", description="Nur Admins!", color=Colors.ERROR),
                ephemeral=True
            )
            return
        modal = AddCategoryModal()
        await interaction.response.send_modal(modal)

    @discord.ui.button(label="Kategorie bearbeiten", style=discord.ButtonStyle.primary, custom_id="admin_edit_cat", row=0)
    async def edit_category(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not is_admin(interaction):
            await interaction.response.send_message(
                embed=discord.Embed(title="Keine Berechtigung", description="Nur Admins!", color=Colors.ERROR),
                ephemeral=True
            )
            return

        # Dropdown mit allen Kategorien
        if not bot.categories_cache:
            await interaction.response.send_message(
                embed=discord.Embed(title="Keine Kategorien", description="Erstelle zuerst eine Kategorie.", color=Colors.WARNING),
                ephemeral=True
            )
            return

        view = CategorySelectView(mode="edit")
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

        view = CategorySelectView(mode="delete")
        await interaction.response.send_message(
            embed=discord.Embed(
                title="Kategorie deaktivieren",
                description="Waehle eine Kategorie zum Deaktivieren:",
                color=Colors.ERROR
            ),
            view=view,
            ephemeral=True
        )

    @discord.ui.button(label="Alle Kategorien anzeigen", style=discord.ButtonStyle.secondary, custom_id="admin_show_cats", row=1)
    async def show_categories(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not is_admin(interaction):
            await interaction.response.send_message(
                embed=discord.Embed(title="Keine Berechtigung", description="Nur Admins!", color=Colors.ERROR),
                ephemeral=True
            )
            return

        embed = discord.Embed(
            title="Alle Kategorien",
            color=Colors.ADMIN
        )

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

    @discord.ui.button(label="Wochen-Uebersicht", style=discord.ButtonStyle.secondary, custom_id="admin_week_stats", row=1)
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
            title=f"Wochen-Uebersicht ({current_week})",
            color=Colors.STATS
        )

        if not all_stats:
            embed.description = "Keine Logs diese Woche."
        else:
            # Sortieren nach Betrag
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
                name="\u2500" * 30,
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


class CategorySelectView(discord.ui.View):
    """Dropdown fuer Kategorie-Auswahl"""

    def __init__(self, mode: str = "edit"):
        super().__init__(timeout=120)
        self.mode = mode

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
                current_beschreibung=data.get("beschreibung", "")
            )
            await interaction.response.send_modal(modal)

        elif self.mode == "delete":
            success = await delete_category_from_sheet(selected)
            if success:
                embed = discord.Embed(
                    title="Kategorie deaktiviert!",
                    description=f"**{selected}** wurde deaktiviert und erscheint nicht mehr im Log-Panel.",
                    color=Colors.WARNING
                )
                embed.set_footer(text="Du kannst sie im Google Sheet wieder aktivieren (Spalte E auf 'Ja')")
            else:
                embed = discord.Embed(
                    title="Fehler",
                    description="Kategorie konnte nicht deaktiviert werden.",
                    color=Colors.ERROR
                )
            await interaction.response.send_message(embed=embed, ephemeral=True)


class PayoutSelectView(discord.ui.View):
    """View fuer Auszahlungen"""

    def __init__(self, week: str, users: dict):
        super().__init__(timeout=300)
        self.week = week
        self.users = users

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
                placeholder="User fuer Auszahlung waehlen...",
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

        await interaction.response.defer(ephemeral=True)

        selected_users = interaction.data["values"]
        results = []

        for user_id in selected_users:
            data = self.users.get(user_id, {})
            if data:
                success = await save_payout(
                    user_id=user_id,
                    username=data["name"],
                    amount=data["betrag"],
                    week=self.week,
                    log_count=data["logs"],
                    admin_name=interaction.user.display_name
                )

                if success:
                    await archive_user_logs(int(user_id), self.week)
                    results.append(f"**{data['name']}**: {format_currency(data['betrag'])} ({data['logs']} Logs)")

        embed = discord.Embed(
            title=f"Auszahlung abgeschlossen ({self.week})",
            color=Colors.PAYOUT
        )

        if results:
            embed.description = "\n".join(results)
            embed.add_field(
                name="Gesamt",
                value=f"{len(results)} User ausgezahlt",
                inline=False
            )
        else:
            embed.description = "Keine Auszahlungen durchgefuehrt."

        embed.set_footer(text=f"Von {interaction.user.display_name}")
        embed.timestamp = datetime.now()

        await interaction.followup.send(embed=embed, ephemeral=True)

        # Dashboard aktualisieren
        await update_dashboard()


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

    embed = discord.Embed(
        title="Setup wird ausgefuehrt...",
        color=Colors.INFO
    )

    # Sheets initialisieren
    steps = []

    if bot.sheets_service:
        result = await ensure_sheet_tabs()
        if result:
            steps.append("Google Sheets Tabs erstellt")
        else:
            steps.append("Google Sheets Tabs - Fehler!")
    else:
        steps.append("Google Sheets nicht verbunden!")

    # Kategorien laden
    await load_categories()
    steps.append(f"{len(bot.categories_cache)} Kategorien geladen")

    # Standard-Kategorien erstellen falls leer
    if not bot.categories_cache:
        default_cats = [
            ("Duengen", 10000, "Pflanze", "Plantagen duengen"),
            ("Reparieren", 15000, "Werkzeug", "Fahrzeuge/Gebaeude reparieren"),
            ("Panel", 20000, "Monitor", "Panel platziert"),
        ]
        for name, betrag, emoji, beschreibung in default_cats:
            await save_category(name, betrag, emoji, beschreibung)
        steps.append(f"{len(default_cats)} Standard-Kategorien erstellt")

    embed = discord.Embed(
        title="Setup abgeschlossen!",
        color=Colors.SUCCESS
    )
    embed.description = "\n".join([f"  {s}" for s in steps])
    embed.add_field(
        name="Naechste Schritte",
        value="1. `/panel` - Log-Panel fuer User erstellen\n"
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


@bot.tree.command(name="panel", description="Log-Panel fuer Mitglieder erstellen")
async def panel_command(interaction: discord.Interaction):
    if not is_admin(interaction):
        await interaction.response.send_message(
            embed=discord.Embed(title="Keine Berechtigung", description="Nur Admins!", color=Colors.ERROR),
            ephemeral=True
        )
        return

    # Kategorien neu laden
    await load_categories()

    active_count = sum(1 for v in bot.categories_cache.values() if v.get("aktiv", True))

    embed = discord.Embed(
        title="Log-Panel",
        description=(
            "Waehle eine Kategorie um einen Log-Eintrag zu erstellen.\n"
            "Deine Logs werden automatisch in Google Sheets gespeichert\n"
            "und fuer die woechentliche Auszahlung berechnet."
        ),
        color=Colors.PANEL
    )

    # Kategorien auflisten
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

    view = LogPanelView()
    await interaction.response.send_message(embed=embed, view=view)


@bot.tree.command(name="admin", description="Admin-Panel oeffnen (nur Admins)")
async def admin_command(interaction: discord.Interaction):
    if not is_admin(interaction):
        await interaction.response.send_message(
            embed=discord.Embed(title="Keine Berechtigung", description="Nur Admins!", color=Colors.ERROR),
            ephemeral=True
        )
        return

    # Kategorien neu laden
    await load_categories()

    embed = discord.Embed(
        title="Admin-Panel",
        description=(
            "Verwalte Kategorien, Auszahlungen und Einstellungen.\n\n"
            f"**Aktive Kategorien:** {sum(1 for v in bot.categories_cache.values() if v.get('aktiv', True))}\n"
            f"**Gesamt Kategorien:** {len(bot.categories_cache)}"
        ),
        color=Colors.ADMIN
    )

    embed.add_field(
        name="Verwaltung",
        value=(
            "**Kategorie hinzufuegen** - Neue Log-Kategorie erstellen\n"
            "**Kategorie bearbeiten** - Betrag/Infos anpassen\n"
            "**Kategorie deaktivieren** - Aus Panel entfernen"
        ),
        inline=False
    )

    embed.add_field(
        name="Auswertung",
        value=(
            "**Alle Kategorien** - Uebersicht aller Kategorien\n"
            "**Wochen-Uebersicht** - Stats der aktuellen Woche\n"
            "**Dashboard aktualisieren** - Google Sheets updaten"
        ),
        inline=False
    )

    embed.add_field(
        name="Google Sheets Admin",
        value=f"[Zum Sheet](https://docs.google.com/spreadsheets/d/{SPREADSHEET_ID}/edit)\n"
              "Im Tab 'Kategorien' kannst du auch direkt bearbeiten.",
        inline=False
    )

    embed.set_footer(text=f"{BOT_NAME} v{BOT_VERSION}")
    embed.timestamp = datetime.now()

    view = AdminPanelView()
    await interaction.response.send_message(embed=embed, view=view, ephemeral=True)


@bot.tree.command(name="auszahlung", description="Auszahlung fuer die aktuelle Woche (nur Admins)")
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
                description=f"Keine Logs fuer {current_week} gefunden.",
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
        name="Gesamt Auszahlung",
        value=f"**{format_currency(total_betrag)}**",
        inline=False
    )

    view = PayoutSelectView(week=current_week, users=week_stats)
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

    # Aktuelle Woche
    embed.add_field(
        name=f"Diese Woche ({current_week})",
        value=f"Logs: **{week_stats['logs']}**\nBetrag: **{format_currency(week_stats['betrag'])}**",
        inline=True
    )

    # Gesamt
    embed.add_field(
        name="Gesamt (alle Zeiten)",
        value=f"Logs: **{total_stats['logs']}**\nBetrag: **{format_currency(total_stats['betrag'])}**",
        inline=True
    )

    # Details pro Kategorie diese Woche
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

        medals = ["1.", "2.", "3."]
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

    await interaction.response.send_message(embed=embed) if not interaction.response.is_done() else await interaction.followup.send(embed=embed)


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

    image_url = bild.url if bild else ""

    success = await save_log_entry(
        user=interaction.user,
        kategorie=kategorie,
        beschreibung=beschreibung,
        image_url=image_url
    )

    if success:
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
        if LOG_CHANNEL_ID:
            try:
                channel = bot.get_channel(int(LOG_CHANNEL_ID))
                if channel:
                    log_embed = discord.Embed(title="Neuer Log", color=Colors.LOG)
                    log_embed.add_field(name="User", value=interaction.user.mention, inline=True)
                    log_embed.add_field(name="Kategorie", value=f"{emoji} {kategorie}", inline=True)
                    log_embed.add_field(name="Betrag", value=format_currency(betrag), inline=True)
                    if beschreibung:
                        log_embed.add_field(name="Beschreibung", value=beschreibung[:200], inline=False)
                    if image_url:
                        log_embed.set_image(url=image_url)
                    log_embed.set_footer(text=f"von {interaction.user.display_name}")
                    log_embed.timestamp = datetime.now()
                    await channel.send(embed=log_embed)
            except Exception as e:
                print(f"Log-Channel Fehler: {e}")
    else:
        await interaction.followup.send(
            embed=discord.Embed(title="Fehler", description="Log konnte nicht gespeichert werden.", color=Colors.ERROR),
            ephemeral=True
        )


@log_command.autocomplete('kategorie')
async def log_category_autocomplete(interaction: discord.Interaction, current: str):
    """Autocomplete fuer Kategorien"""
    active_cats = [
        app_commands.Choice(name=f"{v.get('emoji', '')} {k} ({format_currency(v['betrag'])})", value=k)
        for k, v in bot.categories_cache.items()
        if v.get("aktiv", True) and current.lower() in k.lower()
    ]
    return active_cats[:25]


@bot.tree.command(name="kategorien", description="Zeige alle verfuegbaren Kategorien")
async def kategorien_command(interaction: discord.Interaction):
    embed = discord.Embed(
        title="Verfuegbare Kategorien",
        color=Colors.INFO
    )

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

    # User Commands
    embed.add_field(
        name="User-Befehle",
        value=(
            "**/stats** - Deine persoenlichen Statistiken\n"
            "**/log** - Schnell-Log per Slash-Command\n"
            "**/kategorien** - Verfuegbare Kategorien anzeigen\n"
            "**/leaderboard** - Rangliste der Woche\n"
            "**/hilfe** - Diese Hilfe anzeigen"
        ),
        inline=False
    )

    # Admin Commands
    admin_text = (
        "**/setup** - Erstmalige Bot-Einrichtung\n"
        "**/panel** - Log-Panel fuer Mitglieder erstellen\n"
        "**/admin** - Admin-Panel oeffnen\n"
        "**/auszahlung** - Woechentliche Auszahlung\n"
    )

    if is_admin(interaction):
        embed.add_field(name="Admin-Befehle", value=admin_text, inline=False)

    embed.add_field(
        name="Log-Panel",
        value="Nutze die Buttons im Log-Panel oder `/log` um Aktivitaeten zu loggen.",
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
        print("Google Sheets verbunden - lade Kategorien...")
        await load_categories()
        print(f"{len(bot.categories_cache)} Kategorien geladen")
    else:
        print("WARNUNG: Google Sheets nicht verbunden!")

    # Slash Commands synchronisieren
    try:
        synced = await bot.tree.sync()
        print(f"{len(synced)} Slash-Commands synchronisiert")
    except Exception as e:
        print(f"Sync Fehler: {e}")

    # Persistente Views registrieren
    bot.add_view(LogPanelView())
    bot.add_view(AdminPanelView())

    # Status setzen
    await bot.change_presence(
        activity=discord.Activity(
            type=discord.ActivityType.watching,
            name="/hilfe | Log Bot"
        )
    )


@bot.event
async def on_guild_join(guild):
    """Bot wurde zu einem Server hinzugefuegt"""
    print(f"Bot zu Server hinzugefuegt: {guild.name} (ID: {guild.id})")


# ══════════════════════════════════════════════════════════════════════════════
# BACKGROUND TASKS
# ══════════════════════════════════════════════════════════════════════════════

@tasks.loop(hours=1)
async def auto_dashboard_update():
    """Aktualisiere Dashboard automatisch jede Stunde"""
    await update_dashboard()


@auto_dashboard_update.before_loop
async def before_dashboard():
    await bot.wait_until_ready()


# ══════════════════════════════════════════════════════════════════════════════
# ERROR HANDLING
# ══════════════════════════════════════════════════════════════════════════════

@bot.tree.error
async def on_app_command_error(interaction: discord.Interaction, error: app_commands.AppCommandError):
    """Globaler Error Handler fuer Slash Commands"""
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
    except:
        pass


# ══════════════════════════════════════════════════════════════════════════════
# START
# ══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    print(f"""
╔══════════════════════════════════════════════════════════╗
║          {BOT_NAME} v{BOT_VERSION}                       
║          Starte Bot...                                   
╚══════════════════════════════════════════════════════════╝
    """)

    if not DISCORD_TOKEN:
        print("FEHLER: DISCORD_TOKEN nicht gesetzt!")
        print("Setze die Environment Variable DISCORD_TOKEN")
        exit(1)

    if not SPREADSHEET_ID:
        print("WARNUNG: GOOGLE_SHEET_ID nicht gesetzt!")
        print("Google Sheets Features werden deaktiviert.")

    if not GOOGLE_CREDENTIALS_BASE64:
        print("WARNUNG: GOOGLE_CREDENTIALS_BASE64 nicht gesetzt!")
        print("Google Sheets Features werden deaktiviert.")

    # Auto Dashboard starten
    auto_dashboard_update.start()

    bot.run(DISCORD_TOKEN)
