import time
from modules.zabbix_client import ZabbixClient
from modules.log import logger


def process_zabbix_client(zabbix_client):
    zabbix_client.parsing_info()
    zabbix_client.take_top100_message()
    zabbix_client.take_end_date()


def main():
    zabbix_infura = ZabbixClient('infura', time_from_minus_hours=1)
    zabbix_inside = ZabbixClient('inside', time_from_minus_hours=1)

    while True:
        logger.info('Processing Zabbix Infura')
        process_zabbix_client(zabbix_infura)
        
        logger.info('Processing Zabbix Inside')
        process_zabbix_client(zabbix_inside)
        
        logger.info('Cycle complete, sleeping for 60 seconds')
        time.sleep(60)
        # break


if __name__ == '__main__':
    main()
