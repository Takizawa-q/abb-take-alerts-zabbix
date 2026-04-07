from datetime import timedelta, date, datetime
import time
import re
from sql_requests import insert_problem, select_count_message, delete_message, insert_message_info, update_problem, \
    check_zni, take_service_name, check_service_in_new_table
from modules.log import logger


class ZabbixTools:
    time_till = None
    time_from = 1
    time_from_minus_hours = 1
    eventids = None
    def __init__(self, name):
        self.name = name

    def check_insert_message(self, i):
        count_message = select_count_message(i['eventid'], self.name)

        zero_message = 0
        if 'acknowledges' not in i:
            return None
        if len(i['acknowledges']) != count_message and i['hosts']:
            add_message = []
        # if i['acknowledged'] != count_message and i['hosts']:

            for j in i['acknowledges']:
                time_message = j['clock']
                message = j['message']
                if not message:
                    zero_message += 1
                    continue
                # print(j)
                try:
                    user_name = f'{j["username"]} {j["name"]} {j["surname"]}'
                except Exception as e:
                    print(e)

                    user_name = None
                # if zero_message + count_message == len():

                # insert_message_info(i['eventid'], message, time_message, user_name, self.name)
                add_message.append((i['eventid'], message, time_message, user_name, self.name))
            if zero_message + count_message != len(i['acknowledges']) and add_message:
                delete_message(i['eventid'], self.name)
                logger.info('delete_message: %s, %s', i['eventid'], self.name)
                for i in add_message:
                    insert_message_info(i[0], i[1], i[2], i[3], i[4])

    def check_insert_info(self, i, dict_problem_ids_db):
        description_trigger = None
        url = None

        if str(i['eventid']) not in dict_problem_ids_db:
            if i['hosts']:
                host = i['hosts'][0]['host']
            else:
                host = None
            if i['suppressed']:
                visible = i['suppressed']
            else:
                visible = None
            if i['severity']:
                status = int(i['severity'])
            else:
                status = None
            if i['opdata']:
                opdata = i['opdata']
            else:
                opdata = None
            if i['objectid']:
                id_trigger = i['objectid']
            else:
                id_trigger = None
            tags = set()
            tags_zabbix = None
            if i['tags']:
                tags, tags_zabbix = self.take_tags(i)
            try:
                if i['hosts'][0]['name']:
                    host_name = i['hosts'][0]['name']
                else:
                    host_name = host
            except Exception as e:
                logger.error(f'host_name_error {e}')
                host_name = host
            try:
                if i['relatedObject']['comments']:
                    description_trigger = i['relatedObject']['comments']
            except Exception as e:
                logger.error(e)
            try:
                if i['relatedObject']['url']:
                    url = i['relatedObject']['url']
            except Exception as e:
                logger.error(e)
            logger.info(f"{i['eventid']}, dict_problem_ids_db: {dict_problem_ids_db}")
            logger.info(f"New alert in zabbix_{self.name}: {i['eventid']}, {i['clock']}, {host}, {i['name']}, {visible}, {status}, "
                        f"{host_name}, {opdata}, {id_trigger}, {tags}, {tags_zabbix}")
            insert_problem(problem_id=int(i['eventid']), created=int(i['clock']), host=host, problem=i['name'],
                           visible=visible, status=status, host_name=host_name, opdata=opdata, id_trigger=int(id_trigger),
                           tags=tags,  tags_zabbix=tags_zabbix, name=self.name)

            service = check_service_in_new_table(host, i['name'], id_trigger)
            if not service:
                try:
                    dict_all_service = take_service_name(self.name)
                    service = dict_all_service[host][0]

                except Exception as e:
                    print(e)
            logger.info(f'SERVICE: {service}')
            zni = check_zni(service[0], host, self.name)
            logger.info(f'ZNI: {zni}') #CM-4041
            try:
                if hasattr(self, 'send_comment'):
                    if zni:
                        self.send_comment(int(i['eventid']), f'Возможные ЗНИ {zni}')
                    if description_trigger and re.search(r'[а-яА-ЯёЁ]', description_trigger):
                        self.send_comment(int(i['eventid']), description_trigger)
                    if url and re.search(r'[а-яА-ЯёЁ]', description_trigger):
                        self.send_comment(int(i['eventid']), url)
            except Exception as e:
                logger.error(e)

    def check_params(self):
        if not self.time_from:
            today = date.today()
            self.time_from_unix = int(today.timetuple())
        if not self.time_till:
            params = {
                "time_from": self.time_from_unix,  # 1682888400, # 1.05.2023
                # "eventids":  [740691464],
                # "time_till": time_till, #1688158800, # 1.07.2023
                "output": "extend",
                "select_acknowledges": "extend",
                "selectSuppressionData": "extend",  # select_suppression_data
                "selectRelatedObject": ["triggerid", "description", "priority", "lastchange", "url", "comments"],
                # select_relatedObject
                "severities": [4, 5],
                "selectHosts": "extend",
                # "filterCustomTime": "extend",
                "selectTags": "extend"

            }
        else:
            params = {
                "time_from": self.time_from_unix,  # 1682888400, # 1.05.2023
                # "eventids": 385287521,
                # "eventids": [485760166, 485744631],
                "time_till": self.time_till,  # 1688158800, # 1.07.2023
                "output": "extend",
                "select_acknowledges": "extend",
                "selectSuppressionData": "extend",  # select_suppression_data
                "selectRelatedObject": ["triggerid", "description", "priority", "lastchange"],
                # select_relatedObject
                "severities": [4, 5],
                "selectHosts": "extend",
                # "filterCustomTime": "extend",
                "selectTags": "extend"
            }
        if self.eventids:
            params = {
                "eventids": self.eventids,
                "output": "extend",
                "select_acknowledges": "extend",
                "selectSuppressionData": "extend",  # select_suppression_data
                "selectRelatedObject": ["triggerid", "description", "priority", "lastchange"],
                # select_relatedObject
                "severities": [4, 5],
                "selectHosts": "extend",
                # "filterCustomTime": "extend",
                "selectTags": "extend"
            }
        logger.debug(f'tut: {params}')
        self.params = params

    def take_time_from(self):
        time_now = datetime.now().replace(microsecond=0)
        self.time_from = time_now - timedelta(hours=self.time_from_minus_hours)
        # self.time_from = str(time_from)
        self.time_from_unix = int(self.time_from.timestamp())
        logger.info('%s', self.time_from_unix)
        logger.info('%s', self.time_from)

    @staticmethod
    def take_tags(i):
        print("TAKE_TAGS, ARS", i)
        tags = set()
        take_all_tags = ''
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
            take_all_tags += tag['tag'] + ": " + tag['value'] + '; '
        # print('gfdgdfgdfgfdgdf', tags)
        if 'SA' in tags and 'DA' in tags:
            tags = set()
        if take_all_tags == '':
            take_all_tags = None
        print("TAKE_TAGS, ARS", tags, take_all_tags)
        return tags, take_all_tags


    def check_success_problem(self, result_end_date, dict_id_end_date, dict_all_end_date):
        for i in result_end_date['result']:
            if i['eventid'] in dict_id_end_date:
                id_problem = dict_id_end_date[i['eventid']]
                duration = int(i['clock']) - int(dict_all_end_date[id_problem][0])
                # print(duration)
                dict_all_end_date[id_problem] = [dict_all_end_date[id_problem][0], i['clock'], duration]
                update_problem(id_problem, i['clock'], duration, name=self.name)

