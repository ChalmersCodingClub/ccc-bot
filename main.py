import asyncio
import discord
from discord import app_commands
from discord.ext import commands

import db
import kattis_cmd
from scraper import Scraper
from scraper.scraper import EntityGone

kattis_conn = db.KattisDbConn("db/kattis.db")
user_conn = db.UserDbConn("db/user.db")
scraper = Scraper()

# No prefix commands any more — message_content (a privileged intent) is no
# longer needed.
intents = discord.Intents.default()

# Slash-only bot: there are no prefix commands, so the prefix is a harmless
# sentinel (and message_content stays off).
client = commands.Bot(command_prefix=commands.when_mentioned, help_command=None, intents=intents)

client.tree.add_command(kattis_cmd.setup(kattis_conn, user_conn))


@client.event
async def on_ready():
    if getattr(client, '_synced', False):
        return
    # Global sync (not per-guild copy) so the commands are available in DMs as
    # well as guilds. Global publishes can take up to ~1h to propagate the
    # first time.
    await client.tree.sync()
    client._synced = True
    print('synced slash commands globally', flush=True)


@client.tree.command(name="track-user", description="Start tracking a Kattis user by URL slug.")
@app_commands.describe(shortname="The user's Kattis URL slug, e.g. 'joshua-andersson' from https://open.kattis.com/users/joshua-andersson")
async def track_user(interaction: discord.Interaction, shortname: str):
    await interaction.response.defer(ephemeral=True)
    shortname = shortname.strip().lstrip('/').removeprefix('users/').strip('/')
    try:
        await asyncio.to_thread(scraper.scrape_user, shortname)
    except EntityGone:
        await interaction.followup.send(
            f"No Kattis user with slug `{shortname}`. Check the URL on their profile page.",
            ephemeral=True,
        )
        return
    except Exception as e:
        await interaction.followup.send(f"Error checking `{shortname}`: {e}", ephemeral=True)
        return
    await interaction.followup.send(
        f"Now tracking `{shortname}`. Their score will be scraped daily.",
        ephemeral=True,
    )


@client.tree.command(name="setname", description="Map your Discord account to a Kattis display name.")
@app_commands.describe(name="Your Kattis display name (as it appears on the ranklist).")
async def setname(interaction: discord.Interaction, name: str):
    user_conn.set_realname(str(interaction.user.id), name)
    await interaction.response.send_message(f"Your name was set to `{name}`!", ephemeral=True)


@client.tree.command(name="whoami", description="Show the Kattis name mapped to your Discord account.")
async def whoami(interaction: discord.Interaction):
    name = user_conn.get_realname(str(interaction.user.id))
    if name is None:
        await interaction.response.send_message(
            "I don't know! Set your name with `/setname`.", ephemeral=True)
    else:
        await interaction.response.send_message(f"You are `{name}`!", ephemeral=True)


@client.tree.command(name="forgetme", description="Remove the Kattis name mapped to your Discord account.")
async def forgetme(interaction: discord.Interaction):
    user_conn.remove_realname(str(interaction.user.id))
    await interaction.response.send_message("You have been forgotten!", ephemeral=True)


def main():
    with open('token.txt', 'r') as f:
        token = f.read().strip()
    client.run(token)


if(__name__ == "__main__"):
    main()
