from datetime import datetime

from pyzabbix.api import ZabbixAPI

from env import params_zabbix
from modules.log import logger
from modules.zabbix_tools import ZabbixTools
from sql_requests import select_problem_id, select_all_open_ids, update_problem, update_delete_info, \
    update_visible, select_visible, select_id_today, select_time_begin

zabbix_infura_name = params_zabbix['zabbix_infura_name']
zabbix_infura_password = params_zabbix['zabbix_infura_password']

zabbix_inside_name = params_zabbix['zabbix_inside_name']
zabbix_inside_password = params_zabbix['zabbix_inside_password']


class ZabbixClient(ZabbixTools):
    def __init__(self, name: str, eventids: list[int] = None, time_from_minus_hours: int = 1, time_till: int = None):
        super().__init__(name)
        self.params = None
        self.time_from_unix, self.time_from_minus_hours, self.time_till = None, time_from_minus_hours, time_till
        self.eventids = eventids
        if self.name == 'infura':
            self._login = zabbix_infura_name
            self.__password = zabbix_infura_password
            self.url = 'https://m-zabxm-zbx01.abb-win.akbars.ru/'
        else:
            self._login = zabbix_inside_name
            self.__password = zabbix_inside_password
            self.url = 'https://zabbix/'
            # self.url = 'https://10.149.83.208/'

    def take_top100_message(self) -> None:
        list_ids = [i[0] for i in select_id_today(self.name)]
        params = {
            # "time_from": 1682888400, #1682888400, # 1.05.2023
            "eventids": list_ids,
            # "time_till": time_till, #1688158800, # 1.07.2023
            "output": "extend",
            "select_acknowledges": "extend",
            "selectSuppressionData": "extend",  # select_suppression_data
            "selectRelatedObject": ["triggerid", "description", "priority", "lastchange"],  # select_relatedObject
            "severities": [4, 5],
            "selectHosts": "extend",
            # "filterCustomTime": "extend"

        }
        try:
            with ZabbixAPI(url=self.url, user=self._login, password=self.__password) as zapi:
                result_end_date2 = zapi.do_request("event.get", params=params)
                for i in result_end_date2['result']:
                    # print(i)
                    self.check_insert_message(i)
        except Exception as e:
            logger.error(e)

    def parsing_info(self):
        self.take_time_from()  # time_now - 1 hour
        # if not self.params:
        self.check_params()

        with ZabbixAPI(url=self.url, user=self._login, password=self.__password) as zapi:
            result_pars_info = zapi.do_request("event.get", params=self.params)

            dict_all_end_date = {}
            list_all_end_date = []
            list_all_problem_id = select_problem_id(date_from=self.time_from, name=self.name)
            # list_all_problem_id = select_all_open_ids(name=self.name)
            # print('$$$$$$$$$$$$$$$$$$$$$$$$$$$$')
            # print(result_pars_info)
            logger.debug(result_pars_info)
            for i in result_pars_info['result']:
                logger.debug(i)
            for i in result_pars_info['result']:
                self.check_insert_info(i, list_all_problem_id)
                if i['r_eventid'] != '0':
                    dict_all_end_date[i['eventid']] = [int(i['clock']), i['r_eventid']]
                    list_all_end_date.append(i['r_eventid'])
                # print(i)
                self.check_insert_message(i)
            # end_date_is_null_dict = select_problem_id(date_from=self.time_from, name=self.name)
            end_date_is_null_dict = select_all_open_ids(name=self.name)
            for i in end_date_is_null_dict:
                if i not in list_all_end_date:
                    list_all_end_date.append(i)
            # print(dict_all_end_date)
            if len(list_all_end_date) > 800:
                list_all_end_date = list_all_end_date[0:900]
            # print(dict_all_end_date)
            # print('$$$$$$$$$$$$$$$$$$$$$$$$$')
            # print(dict_problem_ids_db)
            dict_id_end_date = {}
            dict_problem_ids_db = select_problem_id(date_from=self.time_from, name=self.name)
            for i in dict_problem_ids_db:
                # print(i)
                if i in dict_all_end_date:
                    dict_id_end_date[dict_all_end_date[i][1]] = i
            result_end_date = zapi.do_request("event.get", params={
                "eventids": list_all_end_date
            })
            # print(result_end_date['result'])
            self.check_success_problem(result_end_date, dict_id_end_date, dict_all_end_date)
            for i in result_end_date['result']:
                # print(i['eventid'])
                # print(dict_id_end_date)
                self.check_insert_message(i)
                # if i['eventid'] in dict_id_end_date:
                #     id_problem = dict_id_end_date[i['eventid']]
                #     duration = int(i['clock']) - int(dict_all_end_date[id_problem][0])
                #     # print(duration)
                #     dict_all_end_date[id_problem] = [dict_all_end_date[id_problem][0], i['clock'], duration]
                #     update_problem(id_problem, i['clock'], duration, name=self.name)
                #     logger.info('update_problem1: %s, %s, %s', id_problem, i['clock'], duration)
        # result_end_date2 = zapi.do_request("event.get", params={
        #     "eventids": end_date_is_null_list
        # })

    def take_end_date(self):
        dict_problem_ids_without_end_date = select_time_begin(self.name)
        logger.info(dict_problem_ids_without_end_date)
        # print(len(dict_problem_ids_without_end_date))

        list_problem_ids_whithout_end_date = []
        params = {
            # "time_from": 1682888400, #1682888400, # 1.05.2023
            "eventids": list_problem_ids_whithout_end_date,
            # "time_till": time_till, #1688158800, # 1.07.2023
            "output": "extend",
            "select_acknowledges": "extend",
            "selectSuppressionData": "extend",  # select_suppression_data
            "selectRelatedObject": ["triggerid", "description", "priority", "lastchange"],  #
            "severities": [4, 5],
            "selectHosts": "extend",
            # "filterCustomTime": "extend"
        }
        list_r_event = []
        reverse_dict = {}
        for i in dict_problem_ids_without_end_date:
            list_problem_ids_whithout_end_date.append(i)
        id_visible = select_visible(name=self.name)
        with ZabbixAPI(url=self.url, user=self._login, password=self.__password) as zapi:
            result_end_date2 = zapi.do_request("event.get", params=params)
            list_triggerids = []
            dict_triggerids = {}
            list_ids_zabbix = []
            for i in result_end_date2['result']:
                # print(i)
                logger.debug(i)
                self.check_insert_message(i)
                list_ids_zabbix.append(i['eventid'])
                if i['eventid'] in dict_problem_ids_without_end_date:
                    # print(i['r_eventid'])
                    dict_problem_ids_without_end_date[i['eventid']] = [i['r_eventid'], i['clock']]
                    dict_triggerids[i['objectid']] = i['eventid']
                    list_triggerids.append(i['objectid'])
                    if i['r_eventid'] != '0':
                        list_r_event.append(i['r_eventid'])
                    reverse_dict[i['r_eventid']] = i['eventid']
                if 'hosts' not in i or not i['hosts'] or i['hosts'][0]['status'] == '1':
                    logger.debug(f'hosts: {i}')
                    update_delete_info(i['eventid'], i['clock'], name=self.name)
                # if i['suppressed'] != id_visible[i['eventid']] and str(id_visible[i['eventid']]) != '1':
                #     logger.info(f'update_visible: {i}')
                #     update_visible(i['eventid'], i['suppressed'], name=self.name)
            # print(list_r_event)
            # ['490408435', '489357238', '489357268', '489357275', '489357470', '489260307', '490377074', '490967764', '490967720', '491021663', '489496796', '489496751', '488931707', '488935952', '489509806', '488963348', '489028413', '489029214', '489035494', '489197575', '489082470', '489082565', '489348327', '489354271', '489148234', '489354987', '489137407', '489187827', '489338266', '489397392', '489200948', '489208291', '489230605', '489243321', '489237646', '489243659', '489344064', '489789052', '489356756', '489443605', '489510136', '489483529', '492078208', '492078240', '492078258', '492078518', '489503769', '489523812', '489513636', '489509584', '489557199', '489636080', '489633550', '490008370', '490105465', '489853290', '490105467', '489676695', '489695974', '489775341', '489705773', '489848611', '489848340', '489964432', '490034305', '490296043', '490296044', '490310854', '490301479', '490296027', '490352359', '490251791', '490251796', '490293609', '490119285', '490132393', '490167899', '490296994', '492075986', '490285203', '490279429', '490570543', '490360519', '491663385', '490440135', '490458207', '490458231', '490945027', '490947738', '491602975', '492199615', '491579353', '490885060', '490885847', '490574026', '490547145', '490987194', '492138892', '492191273', '492103595', '492176672', '492467853', '492467918', '492467964', '492576842', '492576860', '492576885', '492577047', '492230442', '492244370', '492244510', '492283148', '492225259']
            # print(dict_problem_ids_whitout_end_date)
            result_end_date3 = zapi.do_request("event.get", params={"eventids": list_r_event})
            result_status_triggerids = zapi.do_request('trigger.get', params={'triggerids': list_triggerids})
        # print('ngjdfkgdf')
        for i in result_end_date3['result']:
            logger.debug(i)
            if i['eventid'] in reverse_dict:
                # print(i)
                # print(reverse_dict[i['eventid']])
                # print(dict_problem_ids_whitout_end_date[reverse_dict[i['eventid']]])
                id_problem = reverse_dict[i['eventid']]
                duration = int(i['clock']) - int(dict_problem_ids_without_end_date[reverse_dict[i['eventid']]][1])
                dict_problem_ids_without_end_date[reverse_dict[i['eventid']]] = [i['clock'],
                                                                                 dict_problem_ids_without_end_date[
                                                                                     reverse_dict[i['eventid']]][1],
                                                                                 duration]
                update_problem(id_problem, i['clock'], duration, name=self.name)
                logger.info('update_problem2: %s %s %s', id_problem, i['clock'], duration)
        # print(dict_problem_ids_whitout_end_date)
        # print('We are here')
        for i in result_status_triggerids['result']:
            trigger_id = i['triggerid']
            if i['status'] == '1':
                logger.info(f'update_delete_info: {trigger_id}')
                update_delete_info(dict_triggerids[trigger_id], datetime.now().replace(microsecond=0), name=self.name)

        delete_info = []
        for i in list_problem_ids_whithout_end_date:
            if i not in list_ids_zabbix:
                delete_info.append(i)
                update_delete_info(i, datetime.now().replace(microsecond=0), name=self.name)
                logger.info(f'Delete_info: {i}')

    def send_comment(self, id_alert, text):
        logger.info('SEND_COMMENT')
        params = {
            "eventids": id_alert,
            "action": 4,
            "message": text,
        }
        try:
            with ZabbixAPI(url=self.url, user=self._login, password=self.__password) as zapi:
                result1 = zapi.do_request("event.acknowledge", params=params)
            logger.info(result1)
        except Exception as e:
            logger.error(e)
