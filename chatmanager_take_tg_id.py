import json
import os
from datetime import datetime

import requests
import time
from env import path_to_main
from modules.log import logger


def get_today_date():
    today = datetime.today()
    formatted_date = today.strftime('%d-%m-%y')
    return formatted_date


def check_file_exists(file_path):
    return os.path.isfile(file_path)


def clean_old_tokens(directory, days=7):
    """Remove files older than specified number of days."""
    now = time.time()
    cutoff = now - (days * 86400)  # 86400 seconds in a day

    for filename in os.listdir(directory):
        file_path = os.path.join(directory, filename)
        if os.path.isfile(file_path):
            file_creation_time = os.path.getctime(file_path)
            if file_creation_time < cutoff:
                os.remove(file_path)
                logger.info(f'Removed old token file: {file_path}')



def take_token_for_chatmanager():
    formatted_date = get_today_date()
    token_directory = fr'{path_to_main}\all_json\token_for_chatmanager'
    clean_old_tokens(token_directory)  # Clean old token files
    if not check_file_exists(fr'{token_directory}\{formatted_date}.json'):
    # if not check_file_exists(fr'{path_to_main}\all_json\token_for_chatmanager\{formatted_date}.json'):
        url = "https://chatmanager-babdt.dev.akbars.ru/api/Login"

        payload = json.dumps({
            "username": "DitAdmin"
        })
        headers = {
            'Content-Type': 'application/json',
            'Cookie': '030869a29f6c9ecff006feb08d19d914=603b1b189e1147710771bb7e9ebeff4a'
        }
        response = requests.request("POST", url, headers=headers, data=payload)
        logger.info(response.text)

        with open(fr'{path_to_main}\all_json\token_for_chatmanager\{formatted_date}.json', 'w') as f:
            # print(response.json())
            json.dump(response.json(), f)
    with open(fr'{path_to_main}\all_json\token_for_chatmanager\{formatted_date}.json', 'r') as f:
        api_key = json.load(f)
    return api_key

# take_token_for_chatmanager()


def take_telegram_id(fio_with_email):
    logger.debug(fio_with_email)
    api_key = take_token_for_chatmanager()
    # url = f"https://chatmanager-babdt.dev.akbars.ru/api/EmailByTelegramId/{fio_with_email}"
    url = f"https://chatmanager-babdt.live-int.akbars.ru/api/TelegramIdByEmail/{fio_with_email}"

    payload = {}
    headers = {
        'Authorization': f'Bearer {api_key["token"]}',
        'Cookie': '030869a29f6c9ecff006feb08d19d914=603b1b189e1147710771bb7e9ebeff4a'
    }
    response = requests.request("GET", url, headers=headers, data=payload)

    # response = requests.request("GET", url, headers=headers, data=payload)
    logger.debug(response)
    telegram_id = response.json()['result']

    return telegram_id


# check_eligible_user(512578806, 'jira', service='Среда для разработки и автоматизации внутренних решений ДИТ')
# print(take_telegram_id('AleksandrovMV@akbars.ru'))

if __name__ == '__main__':
    # print(take_telegram_id('NasibullinAL@akbars.ru'))
    # print(take_telegram_id('AleksandrovMV@akbars.ru'))
    # take_token_for_chatmanager()
    # print(take_telegram_id('ZaripovNFA@akbarsdigital.ru'))
    # print(take_telegram_id('ZaripovNFA@abdgtl.com'))
    print(take_token_for_chatmanager())
    ''''ZaripovNFA@akbarsdigital.ru
ZaripovNFA@abdgtl.com'''
    '''5299989528'''
    # print(take_telegram_id(1437674559))
# print(take_token_for_chatmanager())


