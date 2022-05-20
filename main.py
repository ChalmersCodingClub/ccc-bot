import sys
import io
import traceback
import discord
from discord.ext import commands, tasks
from datetime import datetime

from matplotlib import pyplot as plt
from matplotlib.ticker import MaxNLocator
from matplotlib.dates import num2date

import db
import scraper

kattis_conn = db.KattisDbConn("db/kattis.db")
user_conn = db.UserDbConn("db/user.db")
scraper = scraper.Scraper()

client = commands.Bot("$", help_command=None)

def main():
    with open('token.txt', 'r') as f:
        token = f.read().strip()
    client.run(token)

@client.event
async def on_ready():
    global main_channel
    main_channel = client.get_channel(804351000113971284)
    scrape_timer.start()

scrape_fails = 0
@tasks.loop(minutes=10)
async def scrape_timer():
    await client.wait_until_ready()
    global scrape_fails
    curr = datetime.now().date()
    prev = kattis_conn.max_time()
    if(prev == None or prev.date() != curr):
        try:
            print("scraping...")
            scraper.scrape()
            print("ok!")
            if(scrape_fails):
                print("Scraped successfully!")
                await main_channel.send("Lyckades scrapea nu :)")
                scrape_fails = 0
        except Exception as crap:
            print("Scraping failed!\n\n", crap, "\n")
            traceback.print_exc()
            await main_channel.send("Scraping failed!!")
            scrape_fails += 1
            if(scrape_fails >= 10):
                print("10 consecutive fails; shutting down.")
                await main_channel.send("RAGE QUITTING")
                await client.close()

@client.command()
async def setname(ctx, *args):
	name = " ".join(args)
	user_conn.set_realname(str(ctx.message.author.id), name)
	await ctx.send("Your name was set to `" + name + "`!")

@client.command()
async def whoami(ctx):
	discord_id = str(ctx.message.author.id)
	name = user_conn.get_realname(discord_id)
	if(name == None): await ctx.send("I don't know! Set your name with `setname Name`.")
	else: await ctx.send("You are `" + name + "`!")

@client.command()
async def forgetme(ctx):
	discord_id = str(ctx.message.author.id)
	name = user_conn.remove_realname(discord_id)
	await ctx.send("You have been forgotten!")


@client.command()
async def help(ctx):
	await ctx.send(
		"`$kattis [user|uni|country] [\"Name1\" \"Name2\" ...] [top=[global|swe|chalmers][x]] [score|rank|nof_unis|nof_users] [global|swe|chalmers] [days=x] [log] [nozoom] [legend|nolegend] [ignore-not-found]` (slightly simplified...)\n"
		"`$setname name`\n`$whoami`\n`$forgetme`")

# $kattis [[type=]user|uni|country] [[name=]name1,name2,...] [top[=[global|swe|chalmers][x]]] [[variable=]score|rank|nof_unis|nof_users] 
#         [[ranklist=]global|swe|chalmers] [days=x] [log] [nozoom] [ignore-not-found] [legend|nolegend]
@client.command()
async def kattis(ctx, *args):
	named_args = dict()
	bool_args = set()
	for arg in args:
		if('=' in arg):
			x, y = arg.split('=')
			if(x in named_args):
				await ctx.send(f"`{x}=...` twice :(")
				return
			named_args[x] = y
		else:
			if(arg in bool_args):
				await ctx.send(f"`{arg}` twice :(")
				return
			bool_args.add(arg)

	if('top' in bool_args and 'top' not in named_args): named_args['top'] = ""

	if('name' in named_args): named_args['name'] = [x.strip() for x in named_args['name'].split(',')]
	else: named_args['name'] = []

	newargs = dict()
	d = {
		 	'user':'type', 'uni':'type', 'country':'type',
		 	'score':'variable', 'rank':'variable', 'nof_unis':'variable', 'nof_users':'variable',
		 	'global':'ranklist', 'swe':'ranklist', 'chalmers':'ranklist',
		}
	for x in bool_args:
		if(x in d):
			if(d[x] in named_args):
				await ctx.send(f"`{x}` and `{d[x]}={named_args[d[x]]}` :(")
				return
			if(d[x] in newargs):
				await ctx.send(f"`{x}` and `{newargs[d[x]]}` :(")
				return
			newargs[d[x]] = x
		elif(' ' in x or x[0].isupper()):
			named_args['name'].append(x)
		elif(x == 'me'):
			discord_id = str(ctx.message.author.id)
			name = user_conn.get_realname(discord_id)
			if(name == None):
				await ctx.send("Set your name with `setname Name`.")
				return
			named_args['name'].append(name)
		elif(x[0:3] == "<@!"):
			discord_id = ''.join(filter(str.isdigit, x))
			name = user_conn.get_realname(discord_id)
			if(name == None):
				await ctx.send(x + " has no name :(")
				return
			named_args['name'].append(name)
		else:
			if(x not in ['log', 'nozoom', 'ignore-not-found', 'legend', 'nolegend']):
				await ctx.send(f"`{x}` ???????")
				return
	named_args.update(newargs)

	if('type' not in named_args): named_args['type'] = 'user'
	if(named_args['type'] not in ['user', 'uni', 'country']):
		await ctx.send("Unknown type, must be `user`, `uni` or `country`.")
		return

	if('variable' not in named_args): named_args['variable'] = 'score'
	if(named_args['variable'] not in ['score','rank','nof_unis','nof_users']):
		await ctx.send("Unknown variable, must be `score`, `rank`, `nof_unis` or `nof_users`.")
		return
	if(named_args['variable'] == 'nof_unis' and named_args['type'] != 'country'):
		await ctx.send("`variable=nof_unis` can only be used when `type=country`.")
		return
	if(named_args['variable'] == 'nof_users' and named_args['type'] == 'user'):
		await ctx.send("`variable=nof_users` can not be used when `type=user`.")
		return

	if(named_args['type'] == 'user' and named_args['variable'] == 'rank' and 'ranklist' not in named_args):
		named_args['ranklist'] = 'chalmers'
	if('ranklist' in named_args and named_args['ranklist'] not in ['global', 'swe', 'chalmers']):
		await ctx.send("Unknown ranklist, must be `global`, `swe` or `chalmers`.")
		return

	if('top' in named_args or named_args['name']==[]):
		top = named_args.get('top', '')
		toplist = named_args.get('ranklist', {'user':'chalmers','uni':'swe','country':'global'}[named_args['type']])
		if(top.startswith('chalmers')): toplist = 'chalmers'
		if(top.startswith('swe')): toplist = 'swe'
		if(top.startswith('global')): toplist = 'global'
		if(top.startswith(toplist)): top = top[len(toplist):]
		topcnt = 5 #default
		if(len(top)):
			topcnt = int(top) #error?
		e = kattis_conn.get_top(named_args['type'], toplist, topcnt)
		if(len(e) != topcnt and 'ignore-not-found' not in bool_args):
			await ctx.send(f"Length is only {len(e)}.")
			return
		named_args['name'].extend(e)

	named_args['name'] = list(set(named_args['name'])) #remove duplicates & sort

	if('days' not in named_args): named_args['days'] = 10**5
	else:
		named_args['days'] = int(named_args['days']) #error?
	mintimestamp = int(datetime.now().timestamp()) - int(named_args['days'])*24*3600

	plt.clf()
	plt.xticks(rotation = 20)
	plt.ylabel({'score':'Score', 'rank':'Rank', 'nof_users':'#users', 'nof_unis':'#unis'}[named_args['variable']])
	if(named_args['variable'] != 'score'):
		plt.gca().yaxis.set_major_locator(MaxNLocator(integer=True)) #only integer ticks
	if(named_args['variable'] == 'rank'):
		plt.gca().invert_yaxis()
	if("log" in bool_args):
		plt.yscale("log")
	if('log' in bool_args and 'nozoom' in bool_args):
		await ctx.send("`log` and `nozoom` ... :(")
		return
	nofLines = 0

	if(named_args['type'] == 'user'):
		history = kattis_conn.history(mintimestamp, 'user', named_args['name'], named_args.get('ranklist', 'all'))
		for name,s_history in history:
			timestamps, ranks, names, places, unis, scores = [[x[i] for x in s_history] for i in range(6)]
			dates = list(map(datetime.fromtimestamp, timestamps))
			assert(sorted(dates) == dates)
			if(dates):
				v = scores
				if(named_args['variable'] == 'rank'):
					v = ranks
				plt.plot(dates, v, label=name)
				nofLines+=1
			else:
				if('ignore-not-found' not in bool_args):
					if('ranklist' in named_args):
						await ctx.send(f"No {named_args['ranklist']} history found for the user {name} :(")
					else:
						await ctx.send(f"No history found for the user {name} :(")
					return
	elif(named_args['type'] == 'uni'):
		history = kattis_conn.history(mintimestamp, 'uni', named_args['name'], named_args.get('ranklist', 'all'))
		for name,s_history in history:
			timestamps, ranks, names, places, nof_users, scores = [[x[i] for x in s_history] for i in range(6)]
			dates = list(map(datetime.fromtimestamp, timestamps))
			assert(sorted(dates) == dates)
			if(dates):
				v = scores
				if(named_args['variable'] == 'rank'):
					v = ranks
				if(named_args['variable'] == 'nof_users'):
					v = nof_users
				plt.plot(dates, v, label=name)
				nofLines+=1
			else:
				if('ignore-not-found' not in bool_args):
					if('ranklist' in named_args):
						await ctx.send(f"No {named_args['ranklist']} history found for the uni {name} :(")
					else:
						await ctx.send(f"No history found for the uni {name} :(")
					return
	elif(named_args['type'] == 'country'):
		history = kattis_conn.history(mintimestamp, 'country', named_args['name'], named_args.get('ranklist', 'all'))
		for name,s_history in history:
			timestamps, ranks, names, nof_users, nof_unis, scores = [[x[i] for x in s_history] for i in range(6)]
			dates = list(map(datetime.fromtimestamp, timestamps))
			assert(sorted(dates) == dates)
			if(dates):
				v = scores
				if(named_args['variable'] == 'rank'):
					v = ranks
				if(named_args['variable'] == 'nof_users'):
					v = nof_users
				if(named_args['variable'] == 'nof_unis'):
					v = nof_unis
				plt.plot(dates, v, label=name)
				nofLines+=1
			else:
				if('ignore-not-found' not in bool_args):
					if('ranklist' in named_args):
						await ctx.send(f"No {named_args['ranklist']} history found for the country {name} :(")
					else:
						await ctx.send(f"No history found for the country {name} :(")
					return
	if(nofLines == 0):
		await ctx.send("Nothing to show.")
		return
	if('nozoom' in bool_args):
		#if(named_args['variable'] == 'rank'): plt.gca().set_ylim(top=1)
		#else: plt.gca().set_ylim(bottom=0)
		# small white space this way :)
		xmin, xmax = plt.gca().get_xlim()
		plt.plot([datetime.fromtimestamp((num2date(xmin).timestamp()+num2date(xmax).timestamp())/2)], [0 + (named_args['variable']=='rank')]) #add point at (x_avg,0)
	if('legend' in bool_args and 'nolegend' in bool_args):
		legs = [plt.gca().legend(loc=x) for x in range(11)]
		for x in legs: plt.gca().add_artist(x)
		await ctx.send(":D")
	elif('legend' in bool_args or ('nolegend' not in bool_args and nofLines <= 5)):
		plt.legend()
		# order legend
		handles, labels = plt.gca().get_legend_handles_labels()
		labels, handles = zip(*sorted(zip(labels, handles), key=lambda t: [-1,1][named_args['variable']=='rank'] * t[1].get_ydata()[-1]))
		plt.gca().legend(handles, labels)
	await sendgraph(ctx)

async def sendgraph(ctx):
    with io.BytesIO() as image_binary:
        plt.savefig(image_binary, format='png')
        image_binary.seek(0)
        await ctx.send(file=discord.File(fp=image_binary, filename='graph.png'))
    

if(__name__ == "__main__"):
    main()
