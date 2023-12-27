import csv
import nextcord
import re
from enum import Enum
from datetime import datetime
from nextcord.ext import commands
from collections import defaultdict
from itertools import chain

BOT_TOKEN = ""
GUILD_ID = 851081824360464394
DT_FROM = datetime(year=2023, month=1, day=1)
DT_TO = datetime(year=2024, month=1, day=1)


class LunoEvent(Enum):
    STREAM = 1
    JOIN = 2
    LEAVE = 3


class ReactionCounter:
    def __init__(self, name: str, emojis: list[str]):
        """
        Counts emoji stats per username based on reactions.

        :param name: Name of the counter
        :param emojis: discord emojis starting with any of this strings will be counted
        """

        self.name = name
        self.emojis = emojis
        self.users_recv: dict[str, int] = defaultdict(lambda: 0)  # {username: count}, reaction received counter
        self.users_sent: dict[str, int] = defaultdict(lambda: 0)  # {username: count}, reaction sent counter
        self.users_recv_streams:  dict[str, int] = defaultdict(lambda: 0)  # reaction received counter (only on stream announcements)
        self.total = 0
        self.best_messages: list[list[int, nextcord.Message]] = []

    def process_reaction(
        self,
        reaction: nextcord.Reaction,
        reacted_by: list[str],
        target: str,
        luno_event: LunoEvent | None
    ):
        if not any((str(reaction).startswith(i) for i in self.emojis)):
            return

        print(
            f'{reaction.message.created_at} -- {target} {self.name} +{reaction.count} ({str(reaction)}) -- ' +
            f'{reaction.message.author.name}: {reaction.message.content}'
        )

        # update total
        self.total += reaction.count

        # update sent
        for username in reacted_by:
            self.users_sent[username] += 1

        # update recv
        self.users_recv[target] += reaction.count

        # update recv streams
        if luno_event == LunoEvent.STREAM:
            self.users_recv_streams[target] += 1

        # update best messages
        if len(self.best_messages) < 3:
            self.best_messages.append([reaction.count, reaction.message])
        elif reaction.count > self.best_messages[-1][0]:
            self.best_messages[-1] = [reaction.count, reaction.message]
            self.best_messages = sorted(self.best_messages, key=lambda i: i[0], reverse=True)


class TotalCounter:
    messages = 0
    reactions = 0
    users_messages: dict[str, int] = defaultdict(lambda: 0)
    users_streams: dict[str, int] = defaultdict(lambda: 0)
    users_joins: dict[str, int] = defaultdict(lambda: 0)
    users_reacts_sent: dict[str, int] = defaultdict(lambda: 0)
    users_reacts_recv: dict[str, int] = defaultdict(lambda: 0)


async def process_channel_messages(channel: nextcord.TextChannel):
    print(f"Getting messages from {channel.name} ({channel.id})...")
    gen = channel.history(
        after=DT_FROM,
        before=DT_TO,
        limit=None,
    )
    async for message in gen:
        await process_message(message)


async def process_message(msg: nextcord.Message):
    target = msg.author.name.lower()
    luno_event = None
    if msg.author.id == 240843400457355264:  # messages from Lunodog are special, need to find real target
        if msg.content.endswith(":tv:"):
            luno_event = LunoEvent.STREAM
            target = msg.embeds[0].author.name.lower()
            TotalCounter.users_streams[target] += 1
        elif (mg := (
            re.match("> `(.+)` обиделся и ушел.+", msg.content) or
            re.match("> `(.+)` бежит от судьбы.+", msg.content)
        )) is not None:
            luno_event = LunoEvent.LEAVE
            target = mg.group(1).lower()
        elif msg.content.find('добро пожаловать') != -1 and len(msg.mentions):
            luno_event = LunoEvent.JOIN
            target = msg.mentions[0].name.lower()
            TotalCounter.users_joins[target] += 1

    if not msg.author.bot and not msg.is_system():
        TotalCounter.messages += 1
        TotalCounter.users_messages[target] += 1

    for reaction in msg.reactions:
        reacted_by = [i.name.lower() for i in await reaction.users().flatten()]
        for i in reacted_by:
            TotalCounter.users_reacts_sent[i] += 1

        TotalCounter.reactions += reaction.count
        TotalCounter.users_reacts_recv[target] += reaction.count
        for cnt in COUNTERS:
            cnt.process_reaction(reaction, reacted_by, target, luno_event)


def export(guild: nextcord.Guild):
    print('Exporting results...')

    # get all unique usernames
    users = {
        *TotalCounter.users_messages.keys(), *TotalCounter.users_streams.keys(), *TotalCounter.users_joins.keys(),
        *TotalCounter.users_reacts_sent.keys(), *TotalCounter.users_reacts_recv.keys()
    }
    users = sorted(users)
    user_ids = {i.name.lower(): i.id for i in guild.members}

    # export users csv
    with open(f'out_users.csv', mode='w') as f:
        writer = csv.writer(f, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        columns = ['username', 'user_id', 'сообщений', 'стримов', 'заходов', 'реакций поставлено', 'реакций получено']
        for cnt in COUNTERS:
            columns += [f'{cnt.name} поставлено', f'{cnt.name} получено', f'{cnt.name} за стримы']
        writer.writerow(columns)
        gen = (
            [
                i,
                user_ids.get(i, '???'),
                TotalCounter.users_messages.get(i, 0),
                TotalCounter.users_streams.get(i, 0),
                TotalCounter.users_joins.get(i, 0),
                TotalCounter.users_reacts_sent.get(i, 0),
                TotalCounter.users_reacts_recv.get(i, 0),
                *chain(*([
                    cnt.users_sent.get(i, 0),
                    cnt.users_recv.get(i, 0),
                    cnt.users_recv_streams.get(i, 0)
                ] for cnt in COUNTERS))
            ]
            for i in users)
        writer.writerows(gen)

    with open(f'out_summary.csv', mode='w') as f:
        writer = csv.writer(f, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        writer.writerow([f'Всего сообщений', TotalCounter.messages])
        writer.writerow([f'Всего реакций', TotalCounter.reactions])
        for cnt in COUNTERS:
            writer.writerow([f'Всего реакций {cnt.name}', cnt.total])
            best_messages = (
                f"https://discord.com/channels/{msg.guild.id}/{msg.channel.id}/{msg.id}"
                for msg in (i[1] for i in cnt.best_messages)
            )
            writer.writerow([f'Лучшие сообщения {cnt.name}', *best_messages])


class Bot(commands.Bot):
    async def on_ready(self):
        guild = self.get_guild(GUILD_ID)
        for chan in guild.text_channels[:1]:
            await process_channel_messages(chan)
        export(guild)
        print("ALL DONE")


COUNTERS = [
    ReactionCounter(name="peka5", emojis=["<:peka5:"]),
    ReactionCounter(name="pa", emojis=["<:pa:"]),
    ReactionCounter(name="all-pekas", emojis=["<:peka", "<:pled", "<:trump:", "<:tyan:", "<:pa:", "<:gay:", "<:musor:"]),
    ReactionCounter(name='cringe', emojis=['<:cringe:'])
]


intents = nextcord.Intents.default()
intents.members = True
intents.message_content = False

bot = Bot(intents=intents)
bot.run(BOT_TOKEN)
