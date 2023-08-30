import asyncio
import sys
from datetime import date
import csv
import random
import time
from loguru import logger

from web3 import Web3
from web3.eth import AsyncEth

from src.Profile import Profile
from data.config import config
from tqdm import tqdm


with open('data/profile_ids.txt', 'r') as file:
    profile_ids = [i.strip() for i in file]

with open('data/twitter_handles.txt', 'r') as file:
    twitter_handles = [i.strip() for i in file]

with open("data/proxies.txt", "r") as f:
    proxies = [row.strip() for row in f]

with open("data/keys.txt", "r") as f:
    keys = [row.strip() for row in f]

with open("data/tasks_for_claim.txt", "r") as f:
    tasks_for_claim = [row.strip() for row in f]

def verify_on_galxe(profile: Profile):
    try:
        if config['anti_detect_browser'] == 'Dolphin':
            profile.open_dolphin_profile()
        else:
            profile.open_ads_power_profile()
        profile.driver.maximize_window()
    except Exception as e:
        logger.error(f'{profile.twitter_handle} | {profile.address} | failed to open profile, reason: {e}')
        return 'failure'

    logger.info(f'{profile.twitter_handle} | {profile.address} | verifying on galaxe')
    retries = 0
    max_retries = config['max_retries']
    while retries < max_retries:
        try:
            profile.verify_on_galaxy()
            logger.success(f'{profile.twitter_handle} | {profile.address} | verified successfully on Galxe')
            try:
                profile.close_profile()
            except Exception as e:
                logger.error(f'{profile.twitter_handle} | {profile.address} | failed to close profile, reason: {e}')

            return 'success'
        except Exception as err:
            retries += 1
            logger.error(f'{profile.twitter_handle} | {profile.address} | failed to Galxe verify, reason:\n{err}\n, trying again {retries}/{max_retries}')
            sleeping(10, 15)

    return 'failure'

async def write_to_csv(res, file_name):
    with open(f'result_{str(date.today())}_{file_name}.csv', 'a', newline='') as file:
        writer = csv.DictWriter(file, res.keys())
        if file.tell() == 0:
            writer.writeheader()
        writer.writerow(res)

def get_wallet_proxies(wallets, proxies):
    try:
        result = {}
        for i in range(len(wallets)):
            result[wallets[i]] = proxies[i % len(proxies)]
        return result
    except:
        pass


def sleeping(from_sleep, to_sleep):
    x = random.randint(from_sleep, to_sleep)
    for i in tqdm(range(x), desc='sleep ', bar_format='{desc}: {n_fmt}/{total_fmt}'):
        time.sleep(1)

async def execute_async_task(task, proxies_mapping):
    index = 0
    tasks = []
    wallets_in_batch = config['wallets_in_batch']
    random.shuffle(keys)
    batches = [keys[i:i + wallets_in_batch] for i in range(0, len(keys), wallets_in_batch)]
    for batch in batches:
        for key in batch:
            wallet = key
            if task == 'complete_social_tasks':
                w3 = Web3(Web3.AsyncHTTPProvider('https://rpc.ankr.com/eth'),
                          modules={'eth': (AsyncEth,)}, middlewares=[])
                wallet = w3.eth.account.from_key(key).address
            logger.info(f"{wallet}| starting doing {task} on Galxe on account {index}/{len(keys)}")
            profile = Profile(key, proxy=proxies_mapping[key], proxy_pool=proxies, tasks_for_claim=tasks_for_claim, task=task)
            w_validation = await profile.validation_config_w()
            if not w_validation:
                logger.error('W value is faulty, look README on how to change')
                sys.exit(1)
            if task == 'claim_points':
                tasks.append(profile.claim())
            else:
                tasks.append(profile.complete_galxe_social_tasks())
            index += 1

        res = await asyncio.gather(*tasks)
        for res_ in res:
            sorted_dict = dict(sorted(res_.items()))
            await write_to_csv(sorted_dict, task)
        tasks = []
        random_sleep_time = random.randint(config["sleep_between_accs_from"], config["sleep_between_accs_to"])
        logger.info(f'sleeping {random_sleep_time} seconds before next wallets')
        await asyncio.sleep(random_sleep_time)


def main():
    print(f'\n{" " * 32}AUTHOR - https://t.me/CodeCashMafia{" " * 32}\n')
    print(f'\n{" " * 32}donate - EVM 0x98F93AD29d4Fe210b9D33E910335C88C333db87A{" " * 32}\n')

    proxies_mapping = get_wallet_proxies(keys, proxies)
    if config['mode'] == 'complete_social_tasks' or config['mode'] == 'claim_points':
        asyncio.run(execute_async_task(config['mode'], proxies_mapping))
    else:
        for i, (profile_id, key) in enumerate(zip(profile_ids, keys)):
            logger.info(f'starting work on account {i}/{len(keys)}')
            profile = Profile(key, profile_id, twitter_handles[i], proxies_mapping[key], proxies)
            status = verify_on_galxe(profile)
            res = {'Address': profile.address, 'Key': key, 'Status': status, 'ProfileId': profile_id, 'TwitterHandle': twitter_handles[i]}
            loop = asyncio.get_event_loop()
            coroutine = write_to_csv(res, 'galxe_verify')
            loop.run_until_complete(coroutine)
            sleeping(config["sleep_between_accs_from"], config["sleep_between_accs_to"])

    logger.success(f'finished {len(keys)} wallets...')

    print(f'\n{" " * 32}AUTHOR - https://t.me/CodeCashMafia{" " * 32}\n')
    print(f'\n{" " * 32}donate - EVM 0x98F93AD29d4Fe210b9D33E910335C88C333db87A{" " * 32}\n')

if __name__ == "__main__":
    main()

