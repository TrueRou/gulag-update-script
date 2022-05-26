import asyncio
import logging
import os
import time
from shutil import copyfile

import config
import performance
import stored
from gamemodes import GameMode
from ppysb_pp_py import ScoreParams
from stored import db_context


def log(string: str):
    string_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())) + " " + string
    print(string_time)
    logging.info(string)


async def calc_diff(score: dict):
    mode_vn = GameMode(score['mode']).as_vanilla
    async with db_context(stored.source_pool) as (_, cur):
        await cur.execute("select id from maps where md5=%s", [score['map_md5']])
        row = await cur.fetchone()
        if row is None:
            await insert_queue(score['map_md5'], 'entry')
            return score['pp']
        else:
            osu_file_path = config.osu_file_folder + f"{str(row['id'])}.osu"
            if os.path.exists(osu_file_path):
                pp = 0
                try:
                    param = ScoreParams(mods=score['mods'], acc=score['acc'], n300=score['n300'], n100=score['n100'],
                                        n50=score['n50'],
                                        nMisses=score['nmiss'], nKatu=score['nkatu'], combo=score['max_combo'],
                                        score=score['score'])
                    (result,) = performance.calculate(mode_vn, osu_file_path, [param])
                    pp = result.pp
                finally:
                    return pp
            else:
                await insert_queue(score['map_md5'], 'file')
                return score['pp']


def handle_osr(table_name: str, score: dict, new_id: int):
    path = config.replay_folder
    if table_name == 'scores_vn':
        path += 'osr_vn'
    if table_name == 'scores_rx':
        path += 'osr_rx'
    if table_name == 'scores_ap':
        path += 'osr_ap'
    path += f"/{score['id']}.osr"
    if os.path.exists(path):
        copyfile(path, config.new_bancho_folder + f"/.data/osr/{new_id}.osr")
    else:
        log(f'Replay not exist: {path}')


def get_mode(table_name: str, mode: int) -> int:
    if table_name == 'scores_vn':
        return mode
    if table_name == 'scores_rx':
        return mode + 4
    if table_name == 'scores_ap':
        return mode + 8


async def insert_queue(md5: str, lack_type: str):
    async with db_context(stored.target_pool) as (_, cur):
        await cur.execute(f'insert into maps_lack (md5, lack_type) values (%s, %s) on duplicate key update md5=%s',
                          [md5, lack_type, md5])


async def run_scores_update():
    async with db_context(stored.source_pool) as (_, cur):
        for table_name in ['scores_vn', 'scores_rx', 'scores_ap']:
            log(f'Now handling: {table_name}')
            counter = 0
            total = 0
            await cur.execute(f'select * from {table_name}')
            async for score in cur:
                try:
                    counter += 1
                    total += 1
                    if counter >= 1000:
                        counter = 0
                        log(f"{total} scores are handled")
                    async with db_context(stored.target_pool) as (_, target_cur):
                        pp = await calc_diff(score)
                        if pp > 8192:
                            pp = 8192
                        await target_cur.execute(
                            "INSERT INTO scores "
                            "VALUES (NULL, "
                            "%s, %s, %s, %s, "
                            "%s, %s, %s, %s, "
                            "%s, %s, %s, %s, "
                            "%s, %s, %s, %s, "
                            "%s, %s, %s, %s, "
                            "%s)",
                            [
                                score['map_md5'],
                                score['score'],
                                pp,
                                score['acc'],
                                score['max_combo'],
                                score['mods'],
                                score['n300'],
                                score['n100'],
                                score['n50'],
                                score['nmiss'],
                                score['ngeki'],
                                score['nkatu'],
                                score['grade'],
                                score['status'],
                                get_mode(table_name, score['mode']),
                                score['play_time'],
                                score['time_elapsed'],
                                score['client_flags'],
                                score['userid'],
                                score['perfect'],
                                score['online_checksum'],
                            ])
                        handle_osr(table_name, score, target_cur.lastrowid)
                except:
                    continue
            log(f"{total} scores are handled, {table_name} is finished")


async def run_stats_update():
    log('begin total_hits & plays & play_time update')
    count = 0
    async with db_context(stored.target_pool) as (_, cur):
        await cur.execute('select count(*) from users')
        total = (await cur.fetchone())["count(*)"]
        await cur.execute('select id, name from users')
        async for user in cur:
            count += 1
            log(f'Handle stats ({user["id"]}, {user["name"]}) {count} / {total}')
            try:
                for mode in [0, 1, 2, 3, 4, 5, 6, 8]:  # 7 is outdated
                    async with db_context(stored.target_pool) as (_, cur1):
                        await cur1.execute(
                            f'SELECT SUM(n300 + n100 + n50 + ngeki + nkatu), COUNT(*) FROM scores WHERE userid=%s and mode=%s',
                            [user['id'], mode])
                        row_list = list((await cur1.fetchone()).values())
                        total_hits = row_list[0] if row_list[0] is not None else 0
                        plays = row_list[1]
                        await cur1.execute(
                            'select sum(total_length) from scores inner join maps on scores.map_md5 = maps.md5 where grade!=%s and scores.mode=%s and userid=%s',
                            ['F', mode, user['id']])
                        row_list = list((await cur1.fetchone()).values())
                        total_length = row_list[0] if row_list[0] is not None else 0
                        async with db_context(stored.target_pool) as (_, cur2):
                            await cur2.execute('update stats set total_hits=%s where id=%s and mode=%s',
                                               [int(total_hits), user['id'], mode])
                            await cur2.execute('update stats set plays=%s where id=%s and mode=%s and plays<%s',
                                               [plays, user['id'], mode, plays])
                            await cur2.execute('update stats set playtime=%s where id=%s and mode=%s and playtime<%s',
                                               [total_length, user['id'], mode, total_length])
            except:
                continue
    log('total_hits & plays & play_time update finished')


async def run_rank_update():
    log('begin rank & total_pp update')
    count = 0
    async with db_context(stored.target_pool) as (_, cur):
        await cur.execute('select count(*) from users')
        total = (await cur.fetchone())["count(*)"]
        await cur.execute('select id, name from users')
        async for user in cur:
            count += 1
            log(f'Handle rank ({user["id"]}, {user["name"]}) {count} / {total}')
            try:
                for mode in [0, 1, 2, 3, 4, 5, 6, 8]:  # 7 is outdated
                    async with db_context(stored.target_pool) as (_, cur1):
                        await cur1.execute(
                            f'SELECT s.pp, s.acc FROM scores s '
                            'INNER JOIN maps m ON s.map_md5 = m.md5 '
                            'WHERE s.userid = %s AND s.mode = %s '
                            'AND s.status = 2 AND m.status = 2 '  # only ranked
                            'ORDER BY s.pp DESC',
                            [user['id'], mode]
                        )
                        total_scores = cur1.rowcount
                        top_100_pp = await cur1.fetchmany(100)
                        # update total weighted accuracy
                        tot = div = 0
                        for i, row in enumerate(top_100_pp):
                            add = int((0.95 ** i) * 100)
                            tot += row['acc'] * add
                            div += add
                        acc = tot / (1 if div == 0 else div)

                        # update total weighted pp
                        weighted_pp = sum([row['pp'] * 0.95 ** i
                                           for i, row in enumerate(top_100_pp)])
                        bonus_pp = 416.6667 * (1 - 0.9994 ** total_scores)
                        pp = round(weighted_pp + bonus_pp)
                        async with db_context(stored.target_pool) as (_, cur2):
                            await cur2.execute(
                                f'UPDATE stats SET pp = %s, acc = %s '
                                'WHERE id = %s AND mode = %s',
                                [pp, acc, user['id'], mode]
                            )
            except:
                continue


async def run_task():
    await stored.create_pool()
    await run_scores_update()
    await run_stats_update()
    await run_rank_update()


if __name__ == '__main__':
    logging.basicConfig(filename='script.log', encoding='utf-8', level=logging.INFO, format='%(asctime)s %(message)s')
    loop = asyncio.get_event_loop()
    loop.run_until_complete(run_task())
