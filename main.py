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


async def insert_queue(md5: str, lack_type: str):
    async with db_context(stored.target_pool) as (_, cur):
        # Who cares about duplicated key ?
        await cur.execute(f'insert ignore into maps_lack values (%s, %s)', [md5, lack_type])


async def run_task():
    await stored.create_pool()
    async with db_context(stored.source_pool) as (_, cur):
        for table_name in ['scores_vn', 'scores_rx', 'scores_ap']:
            log(f'Now handling: {table_name}')
            counter = 0
            total = 0
            await cur.execute(f'select * from {table_name} limit 10000')
            async for score in cur:
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
                            score['mode'],
                            score['play_time'],
                            score['time_elapsed'],
                            score['client_flags'],
                            score['userid'],
                            score['perfect'],
                            score['online_checksum'],
                        ])
                    handle_osr(table_name, score, target_cur.lastrowid)
            log(f"{total} scores are handled, {table_name} is finished")


if __name__ == '__main__':
    logging.basicConfig(filename='script.log', encoding='utf-8', level=logging.INFO, format='%(asctime)s %(message)s')
    loop = asyncio.get_event_loop()
    loop.run_until_complete(run_task())
