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
        await cur.execute(f"select id from maps where md5={score['map_md5']}")
        osu_file_path = config.osu_file_folder + f"{str((await cur.fetchone())['id'])}.osu"
        if os.path.exists(osu_file_path):
            param = ScoreParams(mods=score['mods'], acc=score['acc'], n300=score['n300'], n100=score['n100'],
                                n50=score['n50'],
                                nMisses=score['nmiss'], nKatu=score['nkatu'], combo=score['max_combo'],
                                score=score['score'])
            (result, ) = performance.calculate(mode_vn, osu_file_path, param)
            return result.pp
        else:
            log(f'Map not found: {osu_file_path}')
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


async def run_task():
    await stored.create_pool()
    async with db_context(stored.source_pool) as (_, cur):
        for table_name in ['scores_vn', 'scores_rx', 'scores_ap']:
            log(f'Now handling: {table_name}')
            counter = 0
            total = 0
            await cur.execute(f'select * from {table_name}')
            async for score in cur:
                counter += 1
                total += 1
                if counter >= 1000:
                    counter = 0
                    log(f"{total} scores are handled")
                async with db_context(stored.target_pool) as (_, target_cur):
                    pp = await calc_diff(score)
                    new_id = await target_cur.execute(
                        "INSERT INTO scores "
                        "VALUES (NULL, "
                        ":map_md5, :score, :pp, :acc, "
                        ":max_combo, :mods, :n300, :n100, "
                        ":n50, :nmiss, :ngeki, :nkatu, "
                        ":grade, :status, :mode, :play_time, "
                        ":time_elapsed, :client_flags, :user_id, :perfect, "
                        ":checksum)",
                        {
                            "map_md5": score['map_md5'],
                            "score": score['score'],
                            "pp": pp,
                            "acc": score['acc'],
                            "max_combo": score['max_combo'],
                            "mods": score['mods'],
                            "n300": score['n300'],
                            "n100": score['n100'],
                            "n50": score['n50'],
                            "nmiss": score['nmiss'],
                            "ngeki": score['ngeki'],
                            "nkatu": score['nkatu'],
                            "grade": score['grade'],
                            "status": score['status'],
                            "mode": score['mode'],
                            "play_time": score['play_time'],
                            "time_elapsed": score['time_elapsed'],
                            "client_flags": score['client_flags'],
                            "user_id": score['userid'],
                            "perfect": score['perfect'],
                            "checksum": score['online_checksum'],
                        })
                    handle_osr(table_name, score, new_id)
                log(f"{total} scores are handled, {table_name} is finished")

if __name__ == '__main__':
    logging.basicConfig(filename='script.log', encoding='utf-8', level=logging.INFO, format='%(asctime)s %(message)s')
    loop = asyncio.get_event_loop()
    loop.run_until_complete(run_task())
