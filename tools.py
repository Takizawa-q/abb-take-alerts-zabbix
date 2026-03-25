from datetime import date, datetime, timedelta
import re
import re
import jira_api
import time

from aiogram import Bot

from modules.log import logger


def parse_zni_link(zni_text: str) -> str:
    """
    Извлекает CM-номер из текста zni_link и преобразует в markdown-ссылку.
    Например: 'CM-10545 Сервисы: ...' -> '[CM-10545](https://team.akbars.ru/browse/CM-10545)'
    """
    if not zni_text:
        return ""

    # Ищем первое вхождение CM-номера (CM-XXXXX)
    match = re.search(r'CM-\d+', zni_text)
    if match:
        cm_number = match.group(0)
        cm = jira_api.jira.get_cm(cm_number)
        resolution = (cm.raw.get('fields') or {}).get('resolution')
        if resolution and resolution.get('description') == 'Отклонено':
            return ""
        return zni_text
    return ""


def take_tags(i):
    tags = set()
    for tag in i['tags']:
        if tag['tag'].lower() == 'role':
            tags.add(tag['value'])
        if ((tag['tag'].lower() == 'application' and tag['value'].lower() == 'monitoring agent') or
                (tag['tag'].lower() == 'application' and tag['value'].lower() == 'filesystem /var') or
                (tag['tag'].lower() == 'application' and tag['value'].lower() == 'cpu') or
                (tag['tag'].lower() == 'application' and tag['value'].lower() == 'service.info') or
                (tag['tag'].lower() == 'application' and tag['value'].lower() == 'memory') or
                (tag['tag'].lower() == 'application' and tag['value'].lower() == 'locks') or
                (tag['tag'].lower() == 'server' and tag['value'].lower() == 'app') or
                (tag['tag'].lower() == 'template_light')):
            tags.add('SA')

    if 'SA' in tags and 'DA' in tags:
        tags = set()
    return tags


def take_now_hour(hours=1):
    time_now = datetime.now()
    # time_now = int(time.mktime(time_now.timetuple()))
    time_from = time_now - timedelta(hours=hours)  # relativedelta(hours=1)
    time_from = int(time.mktime(time_from.timetuple()))
    logger.info(time_from)
    return time_from


def check_params(time_from, time_till):
    if not time_from:
        today = date.today()
        time_from = int(time.mktime(today.timetuple()))
        print(time_from)
    if not time_till:
        params = {
            "time_from": time_from,  # 1682888400, # 1.05.2023
            # "eventids":  [740691464],
            # "time_till": time_till, #1688158800, # 1.07.2023
            "output": "extend",
            "select_acknowledges": "extend",
            "selectSuppressionData": "extend",  # select_suppression_data
            # select_relatedObject
            "selectRelatedObject": ["triggerid", "description", "priority", "lastchange"],
            "severities": [4, 5],
            "selectHosts": "extend",
            # "filterCustomTime": "extend",
            "selectTags": "extend"

        }
    else:
        params = {
            "time_from": time_from,  # 1682888400, # 1.05.2023
            # "eventids": 385287521,
            # "eventids": [485760166, 485744631],
            "time_till": time_till,  # 1688158800, # 1.07.2023
            "output": "extend",
            "select_acknowledges": "extend",
            "selectSuppressionData": "extend",  # select_suppression_data
            #
            "selectRelatedObject": ["triggerid", "description", "priority", "lastchange"],
            "severities": [4, 5],
            "selectHosts": "extend",
            # "filterCustomTime": "extend"
        }
    return params


async def send_message_tg(problem_id, created, host, problem):  # (
    token_test_bot = '6884868674:AAHHRZPHYSC31CwZTT99_78zbyoZDyGxN5E'
    proxy_url = 'http://mcafee.abb-win.akbars.ru:8080'
    bot = Bot(token=token_test_bot, messageroxy=proxy_url)  #
    # bot = telebot.TeleBot(token_test_bot)
    text = f'ID:{problem_id}\n' \
        f'Дата начала: {created}\n' \
        f'Хост: {host}\n' \
        f'Описаине: {problem}'

    await bot.send_message(512578806, text, parse_mode='HTML', )
    logger.info(f'Send message: {problem_id}')
