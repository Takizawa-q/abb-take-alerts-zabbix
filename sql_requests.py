import asyncio
import time
import uuid

import pyodbc

from chatmanager_take_tg_id import take_telegram_id
from env import params_zabbix
from datetime import datetime
from datetime import date

from modules.log import logger
import textwrap

from tools import send_message_tg

SERVER_SCSM = params_zabbix['SERVER_SCSM']
DB_GRAFANA = params_zabbix['DB_GRAFANA']
DRIVER = params_zabbix['DRIVER']
USER_GRAFANA = params_zabbix['USER_GRAFANA']
PASSWORD_GRAFANA = params_zabbix['PASSWORD_GRAFANA']

SERVER_GLSHP = params_zabbix['SERVER_GLSHP']
DB_MONITORING = params_zabbix['DB_MONITORING']
USER_MONITORING = params_zabbix['USER_MONITORING']
PASSWORD_MONITORING = params_zabbix['PASSWORD_MONITORING']
DB_MASTER = params_zabbix['DB_MASTER']

SQL_SERVER_OKTEL = params_zabbix['SQL_SERVER_OKTEL']
DATABASE_OKTEL = params_zabbix['DATABASE_OKTEL']
USER_OKTEL = params_zabbix['USER_OKTEL']
PASSWORD_OKTEL = params_zabbix['PASSWORD_OKTEL']


def take_name_phone(info):
    dict_name_phone = {}
    for i in info:
        dict_name_phone[i[4]] = i[5]
    return dict_name_phone


def convert_dict(lst):
    dict_problem = {}
    for i in lst:
        dict_problem[str(i[0])] = i[1]
    # print(dict_problem)
    return dict_problem


def convert_dict2(lst):
    dict_problem = {}
    for i in lst:
        dict_problem[str(i[0])] = [i[1], i[2]]
    # print(dict_problem)
    return dict_problem


def select_problem_id(date_from='2023-01-01', name='inside'):
    conn_string = f'SERVER={SERVER_GLSHP};DATABASE={DB_MONITORING};DRIVER={DRIVER};UID={USER_MONITORING};PWD={PASSWORD_MONITORING}'
    sql_query = f'''SELECT [id]
        ,[end_date]
    FROM [zabbix_{name}] where start_date >= '{date_from}' '''
    
    try:
        with pyodbc.connect(conn_string) as conn:
            with conn.cursor() as cur:
                info = cur.execute(sql_query).fetchall()
                info = convert_dict(info)
                logger.debug(info)
    except Exception as e:
        logger.error(e)
        return []
    
    return info


def select_all_open_ids(name='inside'):
    conn_string = f'SERVER={SERVER_GLSHP};DATABASE={DB_MONITORING};DRIVER={DRIVER};UID={USER_MONITORING};PWD={PASSWORD_MONITORING}'
    sql_query = f'''SELECT [id], [end_date] FROM [zabbix_{name}] WHERE end_date IS NULL'''
    
    try:
        with pyodbc.connect(conn_string) as conn:
            with conn.cursor() as cur:
                info = cur.execute(sql_query).fetchall()
                info = convert_dict(info)
                logger.debug(info)
    except Exception as e:
        logger.error(e)
        return []
    
    return info


def take_service_name(name='inside'):
    dict_service = {}
    sql_query = f'''
        set nocount ON
        DROP TABLE IF EXISTS #tt
    DROP TABLE IF EXISTS #ttt
    DROP TABLE IF EXISTS #Maxim 
    SELECT 
           t1.[id]
    	   ,[host]
    	  ,case 
         when CHARINDEX('-', host, CHARINDEX('-', host) + 1) - CHARINDEX('-', host) - 1 > 0 
          then SUBSTRING(host, CHARINDEX('-', host) + 1, CHARINDEX('-', host, CHARINDEX('-', host) + 1) - CHARINDEX('-', host) - 1)
          end as pref
          ,[problem]
          ,[start_date]
          ,[end_date]
          ,[duration]
          ,[status]
          ,[visible]
    	  INTO #tt
      FROM [Monitoring].[dbo].[zabbix_{name}] AS t1
     --WHERE [start_date] BETWEEN '2024-01-01 00:00:00.00' AND '2024-01-26 00:00:00.00'
          --where [host] = 'sql12db4'
        WHERE [end_date] is NULL
        --WHERE [start_date] > '2024-03-01'
    		--Select * FROM #tt
    	select 
    	   #tt.[id] 
    	  ,[host]
    	  ,case 
                when [problem] LIKE '%Отв.: Админы EQ%' then 'Misys Equation'
                when [host] = 'notification' then 'Среда для разработки и автоматизации внутренних решений ДИТ'
                when [host] = 'common-api-pdf-export' then 'Банковские Гарантии онлайн'
    			when [host] = 'RiskTechDR' then 'Терминальные серверы пользователей домена ABB-WIN'
    			when [host] = 'eq-overview-nso-transaction-correqts.live-int.akbars.ru' then 'ДБО ЮЛ Correqts (BSS)'
                when [host] = 'gateway-mortgageloan.msp.akbars.ru' then 'Ak Bars Factory (Автоматизированный сервис подачи заявки на получение ипотеки)'
    			when [host] = 'eqweb webchecks' then 'EQ-Web-Adapter'			
    			when [host] = 'antifraud-antifraud.live-int.akbars.ru' then 'Антифрод ДБО ФЛ'
    			when [host] = 'Mysis Equation' then 'Misys Equation'			
    			when [host] = 'ulcrqag' or pref = 'ULCRQ4' or pref = 'ULCRQ6' then 'ДБО ЮЛ Correqts (BSS)'
    			when [host] = 'Diasoft_efrsb' then 'Diasoft Flextera ЕФРСБ'
    			when [host] LIKE 'pc-ta-way%' or [host] LIKE 'PC-TU-WAY%' or [host] LIKE 'PC-TPU-WAY%' or [host] LIKE 'PC-TU-%' then 'Система терминального доступа к приложениям домен card.akbars.ru'
    			when [host] LIKE '%my.akbars.ru%' or [host] LIKE '%corporateportal%' then 'Корпоративный портал (платформа Orchard Core)'
    			when [host] = 'P-ULCRQ-DB04' AND ([problem] = 'Кол-во Заявлений на досрочное погашение в статусе Выгружен > 0' OR [problem] = 'Кол-во ПП в статусе Ошибка выгрузки во FRAUD >0' OR [problem] = 'Кол-во ПП в статусе Не принят АБС- Update program YIK..AUR >0' OR [problem] = 'Задача, Мобильный клиент. Подпись документов с помощью payControl в статусе выполняется более 5 минут' OR [problem] = 'Выписки TODK значение меньше 60') then 'ДБО ЮЛ Correqts (BSS)'
    			when [host] = 'P-ULCRQ-DB02' AND [problem] = 'Количество смс в статусе Отправлено более 10 минут увеличилось на 10 сообщений' then 'ДБО ЮЛ Correqts (BSS)' 
    			when [host] LIKE '%correqts%' then 'ДБО ЮЛ Correqts (BSS)'
    			when [host] LIKE '%prognozadapter%.akbars.ru' or [host] = 'cre-bankguarantee.msp.akbars.ru' then 'Платформа для взаимодействия с партнерами по БГ'
    			when [host] LIKE 'EDOMMVB' then 'Шлюз торговых площадок ММВБ'
    			when [host] LIKE 'EGARLIMIT-APP' OR [host] LIKE 'FOCUS-APP' then 'EGAR'
    			when [host] LIKE 'rabbitmq%' then 'RabbitMQ Брокер сообщений'
    			when [host] LIKE '%SMSSEND%' then 'Ozeki'
    			when [host] = '1CServer11' then '1C:Управление и учёт хозяйственной деятельностью банка'
    			when [host] = '1CSERVER21' then '1C Эксперт: Зарплата и управление персоналом в кредитных организациях'
    			when [host] = 'packages-factorypackages.live-int.akbars.ru' then 'Ak Bars Factory (Автоматизированный сервис по управлению пакетами услуг)'
    			when [host] = 'searchpayment-bankguarantee.live-int.akbars.ru' then 'Платформа для взаимодействия с партнерами по БГ'
    			when [host] LIKE '%Dionis%' or [host] = 'c2960-48' or [host] LIKE '%Dion-%' then 'Сопровождение WAN ЦОД'
    			when [host] = 'departmentcharges-abo.live-int.akbars.ru' then 'Ак Барс онлайн (АБО 3.0)'
    			when [host] = 'biztalkloangate' or [host] = 'biztalkloangw2' then 'ABB Loan'
    			when [host] = 'common-api-drd-evaluate' then 'Банковские Гарантии онлайн'
    			when [host] = 'crmwebchecks' then 'CRM Siebel'
    			when [host] = 'balancer01.abb-win.akbars.ru' or [host] = 'balancer02.abb-win.akbars.ru' then 'Балансировщики Haproxy'
    			when [host] = 'QUIKGATENEW' then 'Quik'
    			when [host] = 'w4-db' AND [problem] = 'Кол-во операций по СБП на 50% ниже типовых значений (Если не закрылся в течении 2-х минут, звонить дежурному)' then 'Ак Барс онлайн (АБО 3.0)'
    			when [host] = 'w4-db' AND ([problem] = 'Нет операций по Транспортной карте за последние 10 минут' or [problem] = 'Проблема с транзакциями по VIP клиентам' or [problem] = '1 - количество сессий с блокировкой более 2х минут. Сообщать в  чат "Мониторинг Way4" и дублировать в чат "СУБД Алерты" и Гильдееву Т.П.' or [problem] like 'БД WAY4 блокирует пользователь - "%') then 'Way4 (Lekton Classic)'
    			when [host] LIKE '%naucc%' then 'Naumen Contact Center'
    			when [host] = 'REPORTS' then 'Система отчетности Reports 2016'
    			when [host] = 'sbl-db-01.abb-win.akbars.ru' AND ([problem] = 'CRM Siebel - Мониторинг импорта. Импорт неуспешен. (возможно инцидент)' or [problem] = 'CRM Siebel - Мониторинг ошибок интеграции с BizTalk (DUL WF). СЕРВИС ID-Банк. Количество записей за последние 15 мин. превышает допустимое значение (>50)') then 'CRM Siebel' 
    			when [host] = 'ZABBIX-TERM' AND [problem] = 'Failed url "https://bi.akbars.ru/". Код страницы не равен 200 (Оповестить ответственного за сервис BI Qlik)' then 'Система для бизнес-анализа BI Qlik Sence'
    			when [host] = 'SFS' then 'CRM Siebel'
    			when [host] = 'sancm webcheckes' then 'Diasoft - контроль платежей и проверка клиентской базы'
    			when [host] = 'sbl-db-01.abb-win.akbars.ru' then 'Сопровождение СУБД Oracle'
    			when [host] = 'searchclient-440p.live-int.akbars.ru' then 'Взаимодействие с ФНС (440-П)'
    			when [host] LIKE 'haproxy-res%' then 'Балансировщики Haproxy'
    			when [host] LIKE 'application-factorypackages.live-int..akbars.ru%' then 'Ak Bars Factory (Автоматизированный сервис по управлению пакетами услуг)'
    			when [host] LIKE 'app-factorycards.live-int.akbars.ru%' then 'Ak Bars Factory (Автоматизированный сервис подачи заявок на выпуск дебетовой карты)'
    			when [pref] = 'epc' then 'ПК ЕПЦ (АББ)'
    			when [host] LIKE 'BT13-DBO-%' then 'Microsoft BizTalk (АБО 3.0)'
    			when [host] LIKE 'BT13ONLINE%' then 'Microsoft BizTalk (Платежные сервисы)'
    			when [host] LIKE 'BT13PREPROD%' or [host] LIKE 'BT13PPPROC%' or [host] LIKE 'BT13PPSEND%' then 'Microsoft BizTalk (Мультисервисный)'
    			when [host] LIKE 'application-mortgageloan.msp.akbars.ru' then 'Ak Bars Factory (Автоматизированный сервис подачи заявки на получение ипотеки)'
    			when [host] LIKE 'arm-factoryrevizor.live-int.akbars.ru' then 'Ak Bars Factory (Автоматизированное рабочее место сотрудника миддл-офиса)'
    			when [host] LIKE 'Applications-invest-abol.live-int.akbars.ru' then 'Инвест-БЭК'
    			when [host] LIKE 'etpwebchecks' then 'Система межведомственного электронного взаимодействия (СМЭВ)'
    			when [host] LIKE 'Ak Bars Factory webchecks 2' then 'Ak Bars Factory (Электронный архив ФЛ)'
    			when [host] LIKE 'retail-siebel-adapter-super.live-int.akbars.ru' then 'Агрегатор партнерских продуктов'
    			when [host] LIKE 'Web-checking sites Ak Bars Factory CRBRC' then 'Ak Bars Factory (Кредит наличными по брокерской схеме)'
    			when [host] LIKE 'retail-core-super.live-int.akbars.ru' then 'Агрегатор партнерских продуктов'
    			when [host] LIKE 'retail-sla-service-super.live-int.akbars.ru' then 'Агрегатор партнерских продуктов'
    			when [host] = 'migrator-factoryrevizor.live-int.akbars.ru' or [host] = 'camundapledge-factoryrevizor.live-int.akbars.ru' then 'Ak Bars Factory (Автоматизированное рабочее место сотрудника миддл-офиса)'
    			when [host] LIKE 'stagepartnerservice-mortgageloan.msp.akbars.ru' then 'Ak Bars Factory (Автоматизированный сервис подачи заявки на получение ипотеки)'
    			when [host] LIKE 'taskservices-factorycards.live-int.akbars.ru' then 'Ak Bars Factory (Автоматизированный сервис подачи заявок на выпуск дебетовой карты)'
    			when [host] LIKE 'downloaderrorcrerutdf-creditregistry.live-int.akbars.ru' then 'Credit Registry'
    			when [host] LIKE 'abo-hp-0%' then 'Балансировщики Haproxy'
    			when [host] LIKE 'adapter-citizencard-epc.live-int.akbars.ru' then 'Ak Bars Factory (Автоматизированный сервис подачи заявок на выпуск дебетовой карты)'
    			when [host] LIKE 'gw-factorycards.live-int.akbars.ru' then 'Ak Bars Factory (Автоматизированный сервис подачи заявок на выпуск дебетовой карты)'
    			when [host] LIKE 'gw-factorycards.live-int.akbars.ru' then 'Ak Bars Factory (Автоматизированный сервис подачи заявки на получение ипотеки)'
    			when [host] LIKE 'retail-mts-adapter-super.live-int.akbars.ru' then 'Агрегатор партнерских продуктов'
    			when [host] LIKE 'stagecrmservice-mortgageloan.msp.akbars.ru' then 'Ak Bars Factory (Автоматизированный сервис подачи заявки на получение ипотеки)'
    			when [host] LIKE 'identity-sbp.live-int.akbars.ru' then 'Модуль интеграции с Системой Быстрых Платежей (СБП)'
    			when [host] LIKE 'crmadapter-creditregistry.live-int.akbars.ru' then 'Credit Registry'
    			when [host] LIKE '%-abo.live-int.akbars.ru' then 'Ак Барс онлайн (АБО 3.0)'
    			when [host] LIKE '%-digq.live-int.akbars.ru' then 'Система Diasoft Digital.Q Продуктовый каталог (учет пакетов, Биллинговый центр)'
    			when [host] LIKE '%-fns440p.live-int.akbars.ru' then 'Взаимодействие с ФНС (440-П)'
    			when [host] LIKE '%-epc.live-int.akbars.ru' then 'ПК ЕПЦ (АББ)'
    			when [host] LIKE '%-440p.live-int.akbars.ru' then 'Система обработки запросов получения "Единой справки"'
    			when [host] LIKE 'abbm.akbars.ru' then 'Ак Барс бизнес менеджмент'
    			when [host] LIKE 'searchclient-uniteinquire.msp.akbars.ru' then 'Система обработки запросов получения "Единой справки"'
    			when [host] LIKE 'instantforms-uniteinquire.live-int.akbars.ru' then 'Система обработки запросов получения "Единой справки"'
    			when [host] LIKE '%-corpcard.dev.akbars.ru' then 'Корпоративная карта'
    			when [host] = 'trixtrixbox.abb-win.akbars.ru' then 'Asterisk'
    			when [host] = 'check4-app' then 'Check4Trick'
    			when [host] LIKE '%-kfmb.live-int.akbars.ru' then 'Кредитная фабрика МБ'
    			when [host] LIKE '%-prognoz.live-int.akbars.ru' then 'Прогноз'
    			when [host] LIKE '%Web services Invest-BEK checking from zabbix-abc' then 'Инвест-БЭК'
    			when ([host] LIKE '%-bankguarantee.live-int.akbars.ru' or [host] = 'callbackonsearchpayment.msp.akbars.ru') then 'Платформа для взаимодействия с партнерами по БГ'
    			when [host] = 'common-api-process-integration' then 'Банковские Гарантии онлайн'
    			when [host] = 'ExtCheckPorts_IBM_BPM' then 'IBM BPM'
    			when [host] = 'web_team.akbars.ru' then 'Atlassian Jira Software'
    			when [host] = 'SchoolCard webchecks' then 'Microsoft BizTalk (Платежные сервисы)'
    			when [host] = 'gisgmp-balancer' then 'IDБанк-СМЭВ'
    			when [host] = 'EFRSB1.abb-win.akbars.ru' then 'Diasoft Flextera ЕФРСБ'
    			when [host] = 'Elastic Search cluster' or [host] = 'common-api-user-profile' or [host] = 'bg-api-cre' or [host] = 'common-api-doc-template' 
    			or [host] = 'common-api-rbac-management' then 'Банковские Гарантии онлайн'
    			when [host] = 'transfer.akbars.ru' then 'Платформа лояльности GoPoints'
    			when [host] = 'bankok.akbars.ru creditapplications' then 'Ак Барс онлайн (АБО 3.0)'
    			when [host] like 'ABO-C1-%' then 'Ак Барс онлайн (АБО 3.0)'
    			when [host] like '%-factorycards.live-int.akbars.ru' then 'Ak Bars Factory (Автоматизированный сервис подачи заявок на выпуск дебетовой карты)'
    			when [host] like 'BANKOK%' then 'Ак Барс онлайн (АБО 3.0)'
    			when [host] like 'bg-api-%' or [host] like 'common-api-%' then 'Банковские Гарантии онлайн'
    			when [host] like 'haproxy-%.abb-win.akbars.ru' then 'Балансировщики Haproxy'
    			when [host] = 'currencyrates' or [host] = 'currencyratesadapter-smartpanel.msp.akbars.ru' then 'РМК'
    			when [host] = 'w4-db' then 'Way4 (Lekton Classic)'
    			when [host] = 'sql12db4' and ([problem] like '%ОРИОН%' or [problem] like '%Orion%') then 'ОРИОН'
    			when [problem] like N'%[А-Яа-я]%' then [name]
    			when [host] LIKE '%-%-PDB%' then 'Сопровождение СУБД PostgreSQL'
    			when [host] LIKE '%-%-DB%' or [host] LIKE '%-%-CDB%' then 'Сопровождение СУБД MSSQL'
    			when [name] is null and pref like '%way%' then 'Way4 (Lekton Classic)'
    			when [host] LIKE 'SQLWIN%' OR [host] LIKE 'sql12%' then 'Сопровождение СУБД MSSQL'
                else [name] 
            end [name]
    	  ,[status_name]
    	  ,pref
          ,[problem]
          ,[start_date]
          ,[end_date]
          ,[duration]
          ,[status]
          ,[visible]
    	   INTO #ttt
    From #tt
    LEFT JOIN  [dw-it4it-prod].[DataWarehouse].[dbo].[Target_Abdt_Abbm_ITService] AS t2 ON #tt.pref = t2.prefix COLLATE Cyrillic_General_CI_AS

    Select #ttt.*, nomer into #Maxim 
    FROM #ttt 
    LEFT JOIN  [SCSM-DWM].[GRAFANA].[dbo].[ZUIN_alert] AS t3 ON #ttt.host = t3.host COLLATE Cyrillic_General_CI_AS 
    AND #ttt.problem = t3.problem COLLATE Cyrillic_General_CI_AS 
    --WHERE [name] is NULL
    ORDER BY [start_date] DESC

    Select distinct id, host, problem, name, start_date, end_date, duration, status, 
    stuff((
        select concat(', ',nomer) 
        from #Maxim
        where id=m.id 
        order by name
        for XML path('')
        ),1,1,'') Insidenti
    From #Maxim m
    order by [start_date] DESC



    --collate Cyrillic_General_CI_AS

    --Select Distinct[host], [name] FROM #ttt where name is NULL '''

    conn = None
    try:
        # conn_string = f'SERVER={SERVER_SCSM};DATABASE={DB_MASTER};DRIVER={DRIVER};UID={USER_GRAFANA};PWD={PASSWORD_GRAFANA}'
        conn_string = f'SERVER={SERVER_GLSHP};DATABASE={DB_MASTER};DRIVER={DRIVER};UID={USER_MONITORING};PWD={PASSWORD_MONITORING}'
        conn = pyodbc.connect(conn_string)
        cur = conn.cursor()
        info = cur.execute(sql_query)
        info = info.fetchall()
        for i in info:
            # print(i)
            dict_service[i[1]] = i[3], i[8]

    except Exception as e:
        logger.error(e)
    if conn:
        conn.close()
    return dict_service


def take_email_from_host(host, send_role=None, name='inside'):
    dict_all_service = take_service_name(name)
    # print(dict_all_service)
    try:
        service = dict_all_service[host][0]
    except Exception:
        return None, None

    sql_query = '''SELECT 
	t1.employee_fullname,
    t1.employee_email,
    t1.type_name,
    t2.post
FROM 
    [dw-it4it-prod].[DataWarehouse].[dbo].[Target_Abdt_Abbm_ITServiceResponsible] t1
LEFT JOIN 
    [DW-IT4IT-PROD].[DataWarehouse].[dbo].[Target_Abdt_Abbm_Employee] t2 ON t1.employee_email = t2.email  -- t1.service_name = t2.service_name
WHERE 
	t1.responsible_date_to_ch is NULL and t1.type_name in ('Менеджер IT-сервиса', 'Ответственный за сопровождение ИС (SA)',
                                 'Ответственный за сопровождение данных (DA)') and t1.service_name = ? '''

    conn_string = f'SERVER={SERVER_GLSHP};DATABASE={DB_MONITORING};DRIVER={DRIVER};UID={USER_MONITORING};PWD={PASSWORD_MONITORING}'
    logger.info(f'{sql_query}, {service}')
    fio_emails_dolznost = None
    try:
        with pyodbc.connect(conn_string) as conn:
            with conn.cursor() as cur:
                fio_emails_dolznost = cur.execute(sql_query, (service, )).fetchall()
                logger.info(fio_emails_dolznost)
    except Exception as e:
        logger.error(f'{host}: {e}')
    
    logger.info(f'{host}: {fio_emails_dolznost}')
    if send_role:
        send_role = send_role.split(';')
        result_fio_emails_dolznost = []
        logger.info(fio_emails_dolznost)
        # fio_emails_dolznost = list(fio_emails_dolznost)
        list_fio_emails_dolznost = []
        for i in fio_emails_dolznost:
            list_fio_emails_dolznost.append(list(i))
        set_send = set()
        try:
            for i in send_role:
                for j in list_fio_emails_dolznost:
                    if i in j[2]:
                        fio = list(j)
                        fio.append('Отправлять')
                        result_fio_emails_dolznost.append(fio)
                        # list_fio_emails_dolznost.remove(j)
                        set_send.add(j[0])
            # for i in result_fio_emails_dolznost:
            logger.info(f'after remove: {list_fio_emails_dolznost}')
            logger.info(f'result_fio_emails_dolznost: {result_fio_emails_dolznost}')
            for j in list_fio_emails_dolznost:
                if j[0] not in set_send:
                    fio = list(j)
                    fio.append('Не отправлять')
                    result_fio_emails_dolznost.append(fio)
        except Exception as e:
            logger.error(e)
            for fio in list_fio_emails_dolznost:
                result_fio_emails_dolznost.append(fio)
        return result_fio_emails_dolznost, service
    if not fio_emails_dolznost:
        logger.info(f'Not email for {service}')

    logger.info(f'fio_emails_dolznostgfgfg {fio_emails_dolznost}')
    return fio_emails_dolznost, service


def not_find_service(key, host, problem, created):
    sql_query = ''' INSERT INTO [send_zabbix_tg] (id, host, problem, start_date, status_tg) 
    VALUES(?, ?, ?, ?, 'Не найден сервис')'''
    conn_string = f'SERVER={SERVER_SCSM};DATABASE={DB_GRAFANA};DRIVER={DRIVER};UID={USER_GRAFANA};PWD={PASSWORD_GRAFANA}'

    try:
        with pyodbc.connect(conn_string) as conn:
            with conn.cursor() as cur:
                cur.execute(sql_query, (key, host, problem, created))
                conn.commit()
                logger.info(f'Not find service: {key, host, problem, created}')
    except Exception as e:
        logger.error(f'{e}, {(key, host, problem, created)}')


def take_service_from_id(id_service):
    # logger.info('take_service_from_id')
    # conn_string = f'SERVER={SERVER_SCSM};DATABASE={DB_MASTER};DRIVER={DRIVER};UID={USER_GRAFANA};PWD={PASSWORD_GRAFANA}'
    conn_string = f'SERVER={SERVER_GLSHP};DATABASE={DB_MONITORING};DRIVER={DRIVER};UID={USER_MONITORING};PWD={PASSWORD_MONITORING}'
    sql_query = '''SELECT [id],[code],[name],[prefix] 
    FROM [dw-it4it-prod].[DataWarehouse].[dbo].[Target_Abdt_Abbm_ITService]
        where id = ?'''
    res = (id_service, )
    info = None
    try:
        with pyodbc.connect(conn_string) as conn:
            with conn.cursor() as cur:
                info = cur.execute(sql_query, res).fetchone()
                logger.info(info)
    except Exception as e:
        logger.error(f'id_service: {id_service}, {e}')
    return info


def check_service_in_new_table(host, problem, id_trigger=None):
    conn_string = f'SERVER={SERVER_GLSHP};DATABASE={DB_MONITORING};DRIVER={DRIVER};UID={USER_MONITORING};PWD={PASSWORD_MONITORING}'
    send_role = ''
    query_params = (f'{host};{problem}', )
    service = None
    try:
        with pyodbc.connect(conn_string) as conn:
            with conn.cursor() as cur:
                if id_trigger:
                    sql_query = f'''SELECT
                              [id_service], [role]
                          FROM [Monitoring].[dbo].[monitoring_app_rightservice] where [id_trigger] = ? '''
                    service = cur.execute(sql_query, (id_trigger, )).fetchone()
                    if service:
                        service_id, send_role = service[0], service[1]
                        service = take_service_from_id(service_id)[2]
                        logger.info(f'{service, send_role}')
                        return service, send_role
                sql_query = f'''SELECT
                      [id_service], [role]
                  FROM [Monitoring].[dbo].[monitoring_app_rightservice] where [host_description] = ? '''
                service = cur.execute(sql_query, query_params).fetchone()
                logger.info(service)
                if service:
                    service_id, send_role = service[0], service[1]
                    service = take_service_from_id(service_id)[2]
                    logger.info(f'{service, send_role}')
                    return service, send_role
                sql_query = f'''SELECT
                [id_service], [role]
                FROM [Monitoring].[dbo].[monitoring_app_rightservice] where [host_description] = ? '''
                service = cur.execute(sql_query, (host, )).fetchone()
                if service:
                    service_id, send_role = service[0], service[1]
                    service = take_service_from_id(service_id)[2]
                    logger.info(f'{service, send_role}')
    except Exception as e:
        logger.error(e)
    return service, send_role


def take_status_service_from_service(service):
    sql_query = '''SELECT status_name
        FROM [dw-it4it-prod].[DataWarehouse].[dbo].[Target_Abdt_Abbm_ITService] where name = ? '''
    conn_string = f'SERVER={SERVER_GLSHP};DATABASE={DB_MONITORING};DRIVER={DRIVER};UID={USER_MONITORING};PWD={PASSWORD_MONITORING}'

    try:
        with pyodbc.connect(conn_string) as conn:
            with conn.cursor() as cur:
                status_name = cur.execute(sql_query, (service, )).fetchone()
                logger.info(status_name)
                return status_name[0]
    except Exception as e:
        logger.error(e)


def insert_monitoring(problem_id, host, problem, created, status, host_name=None, opdata=None, id_trigger=None,
                      tags_zabbix=None, name='inside'):
    service, send_role = check_service_in_new_table(host, problem, id_trigger)
    last_uin = ''
    version_zabbix = 'Z1'
    if name == 'infura':
        version_zabbix = 'Z2'
    if not service:
        dict_all_service = take_service_name(name)
        try:
            service, last_uin = dict_all_service[host][0], dict_all_service[host][1]
            set_last_uin = set(last_uin.split(', '))
            last_uin = ', '.join(map(str, set_last_uin))
        except Exception:
            service, last_uin = None, ''

    # else:
    #     service, send_role = service[0], service[1]
    sql_query = f'''INSERT INTO [Monitoring].[dbo].[monitoring_app_alert]
                    (id_alert, host, host_name, service, description, begin_date, priority, incident, shozhie_incident,
                    opdata, id_trigger, version_zabbix, zni_link, tags_zabbix) 
                    VALUES(?, ?, ?, ?, ?, ?, ?, '', ?, ?, ?, ?, ?, ?)'''
    zni = take_info_zni(service, host, name)
    status_name = take_status_service_from_service(service)
    if status_name:
        service_and_status = f'{service} ({status_name})'
    else:
        service_and_status = service
    try:
        conn_string = f'SERVER={SERVER_GLSHP};DATABASE={DB_MONITORING};DRIVER={DRIVER};;UID={USER_MONITORING};PWD={PASSWORD_MONITORING}'
        with pyodbc.connect(conn_string) as conn:
            with conn.cursor() as cur:
                res = (problem_id, host, host_name, service_and_status, problem, created, status, last_uin, opdata,
                       id_trigger, version_zabbix, zni, tags_zabbix)
                logger.info(res)
                cur.execute(sql_query, res)
                conn.commit()

    except Exception as e:
        logger.error(e)


def take_info_zni(service, host, name='inside'):
    service1 = None
    if 'db' in host or "DB" in host:
        service1 = take_new_service_without_db(host, name)
    if service1:
        service = service1
    conn_string = f'SERVER={SERVER_SCSM};DATABASE={DB_GRAFANA};DRIVER={DRIVER};UID={USER_GRAFANA};PWD={PASSWORD_GRAFANA}'
    conn_string = f'SERVER=p-glshp-db01;DATABASE=master;DRIVER=ODBC Driver 17 for SQL server;UID=s-ITSupport;PWD=Ca6oQoPyDTKuHqNsMlCv'
    sql_query = f'''SELECT [Number], Service, Affected_service, Date_begin, Date_end, Stop_service, Description
        FROM [Express].[dbo].[zni_cm_jira] where 
        [Service] = '{service}' or [Affected_service] LIKE '%{service}%' 
        and (Date_begin < GETDATE() and [Date_end] > GETDATE())
        and (Status NOT IN ('Закрыт', 'Отклонено')) '''
    result_zni = ''
    count_zni = 0
    try:
        with pyodbc.connect(conn_string) as conn:
            with conn.cursor() as cur:
                zni = cur.execute(sql_query).fetchall()
                logger.info(zni)
                for i in zni:
                    fields = 0
                    if count_zni < 5:
                        result_zni += f'{i[0]}\n'
                        for j in range(1, len(i)):
                            if type(i[j]) is str:
                                i[j] = i[j].replace(';', ',')
                            if fields == 0:
                                i[j] = 'Сервисы: ' + str(i[j])
                            elif fields == 1:
                                i[j] = 'Затронутые: ' + str(i[j])
                            elif fields == 2:
                                i[j] = 'Время начала: ' + str(i[j].strftime("%d.%m.%Y %H:%M")) # %H:%M:%S
                            elif fields == 3:
                                i[j] = 'Время окончания: ' + str(i[j].strftime("%d.%m.%Y %H:%M")) # %H:%M:%S
                            elif fields == 4:
                                i[j] = 'Остановка сервиса: ' + str(i[j])
                            elif fields == 5:
                                i[j] = 'Описание: ' + str(i[j])
                            result = textwrap.fill(i[j], width=50)
                            result_zni += f'{result}\n'
                            fields += 1
                        result_zni += ';'
                    else:
                        result_zni += f'{i[0]};'
                    count_zni += 1
                return result_zni[:3750]
    except Exception as e:
        logger.error(e)


def take_new_service_without_db(host, name='inside'):
    host = host.replace('db', '').replace('DB', '')
    dict_all_service = take_service_name(name)
    try:
        service = dict_all_service[host][0]
        logger.info(service)
        return service
    except Exception:
        try:
            all_info_service = select_service_id(host)
            service = all_info_service[0][2]
            return service
        except Exception:
            pass
        return None


def check_zni(service, host='', name='inside'):
    if 'db' in host or "DB" in host:
        service = take_new_service_without_db(host, name)

    conn_string = f'SERVER={SERVER_SCSM};DATABASE={DB_GRAFANA};DRIVER={DRIVER};UID={USER_GRAFANA};PWD={PASSWORD_GRAFANA}'
    conn_string = f'SERVER=p-glshp-db01;DATABASE=master;DRIVER=ODBC Driver 17 for SQL server;UID=s-ITSupport;PWD=Ca6oQoPyDTKuHqNsMlCv'
    sql_query = f'''SELECT [Number]
    FROM [Express].[dbo].[zni_cm_jira] where (Date_begin < GETDATE() and [Date_end] > GETDATE()) and 
    ([Service] = '{service}' or [Affected_service] LIKE '%{service}%') and (Status NOT IN ('Закрыт', 'Отклонено'))'''
    result_zni = ''
    try:
        with pyodbc.connect(conn_string) as conn:
            with conn.cursor() as cur:
                zni = cur.execute(sql_query).fetchall()
                logger.info(zni)
                for i in zni:
                    result_zni += f'{i[0]};'
                return result_zni
    except Exception as e:
        logger.error(service)
        logger.error(e)


def insert_problem(problem_id, created, host, problem,  visible, status, host_name=None, opdata=None,
                         id_trigger=None, tags=None,  tags_zabbix=None, name='inside'):
    # logger.info('TRY INSERT')
    created = 10800 + int(created)
    created = datetime.utcfromtimestamp(created).strftime('%Y-%m-%d %H:%M:%S')
    # created = datetime.fromtimestamp(created)  # Unix Time
    # created = str(date.today()) + ' ' + created
    logger.info(f'{problem_id, created, host, problem, visible}')
    # created = datetime.strptime(created, '%Y-%m-%d %H:%M:%S')
    res = (problem_id, created, host, problem, visible, status)
    conn_string = f'SERVER={SERVER_GLSHP};DATABASE={DB_MONITORING};DRIVER={DRIVER};UID={USER_MONITORING};PWD={PASSWORD_MONITORING}'

    # list_reason = []
    sql_query = f'''INSERT INTO [zabbix_{name}] (id, start_date, host, problem, visible, status) VALUES(?, ?, ?, ?, ?, ?)'''
    try:
        with pyodbc.connect(conn_string) as conn:
            with conn.cursor() as cur:
                cur.execute(sql_query, res)
                conn.commit()
    except Exception as e:
        logger.error(e)

        return None
    if visible in [1, '1']:
        return None
    try:
        # print('gdfgdfgdf')
        # if problem_id != 863527665:
        insert_send_zabbix(problem_id, host, problem, created, status, opdata, id_trigger=id_trigger, tags=tags,
                           host_name=host_name, name=name)
    except Exception as e:
        logger.error(e)

    try:
        insert_monitoring(problem_id, host, problem, created, status, host_name, opdata, id_trigger=id_trigger,
                          tags_zabbix=tags_zabbix, name=name)
    except Exception as e:
        logger.error(e)

def add_phone_in_oktell(alert_number: int, alert_description: str, host: str, name: str, number: str) -> None:
    text = f'В хосте {host} возник алерт {alert_description}'
    # number = refactoring_number(number)
    res = (uuid.uuid4(), alert_number, text, name, number, datetime.now(), 0, 0)
    # 89027153617
    logger.info(f'{res}')
    try:
        conn_string = f'SERVER={SQL_SERVER_OKTEL};DATABASE={DATABASE_OKTEL};DRIVER={DRIVER};UID={USER_OKTEL};PWD={PASSWORD_OKTEL}'

        with pyodbc.connect(conn_string, driver='{SQL server}') as conn:
            sql_query = f'''INSERT INTO [oktell].[dbo].[tab_recall_allert_SituationCentr]  ([Alert_Number], [Alert_ID] 
                ,[Alert_description] ,[Name] ,[Number] ,[DateTime] ,[Counter] ,[status])VALUES(?, ?, ?, ?, ?, ?, ?, ?) '''
            with conn.cursor() as cur:
                cur.execute(sql_query, res)
                conn.commit()
    except Exception as e:
        logger.error(e)


def take_fio_emails_from_service(service, dolznost):
    fio_emails_dolznost = None
    if dolznost:
        dolznost = dolznost.replace('Manager', 'Менеджер IT-сервиса')
        dolznost = dolznost.split(';')
    logger.info(dolznost)
    sql_query = '''SELECT 
	t1.employee_fullname,
    t1.employee_email,
    t1.type_name,
    t2.post
FROM 
    [dw-it4it-prod].[DataWarehouse].[dbo].[Target_Abdt_Abbm_ITServiceResponsible] t1
LEFT JOIN 
    [DW-IT4IT-PROD].[DataWarehouse].[dbo].[Target_Abdt_Abbm_Employee] t2 ON t1.employee_email = t2.email  -- t1.service_name = t2.service_name
WHERE 
	t1.responsible_date_to_ch is NULL and t1.type_name in ('Менеджер IT-сервиса', 'Ответственный за сопровождение ИС (SA)',
                                 'Ответственный за сопровождение данных (DA)') and t1.service_name = ? '''
    conn_string = f'SERVER={SERVER_GLSHP};DATABASE={DB_MONITORING};DRIVER={DRIVER};UID={USER_MONITORING};PWD={PASSWORD_MONITORING}'
    logger.info(f'{sql_query, service}')
    try:
        with pyodbc.connect(conn_string) as conn:
            with conn.cursor() as cur:
                fio_emails_dolznost = cur.execute(sql_query, (service,)).fetchall()
                logger.info(fio_emails_dolznost)
    except Exception as e:
        logger.error(e)
    
    result_fio_emails_dolznost = []
    logger.info(f'{fio_emails_dolznost}')

    # fio_emails_dolznost = list(fio_emails_dolznost)
    list_fio_emails_dolznost = []
    for i in fio_emails_dolznost:
        list_fio_emails_dolznost.append(list(i))
    set_send = set()
    try:
        for i in dolznost:
            for j in list_fio_emails_dolznost:
                if i in j[2]:
                    fio = list(j)
                    fio.append('Отправлять')
                    result_fio_emails_dolznost.append(fio)
                    # list_fio_emails_dolznost.remove(j)
                    set_send.add(j[0])
        # for i in result_fio_emails_dolznost:
        # print('after remove', list_fio_emails_dolznost)
        logger.info(f'after remove: {list_fio_emails_dolznost}')
        logger.info(f'result_fio_emails_dolznost: {result_fio_emails_dolznost}')
        for j in list_fio_emails_dolznost:
            if j[0] not in set_send:
                fio = list(j)
                fio.append('Не отправлять')
                result_fio_emails_dolznost.append(fio)
    except Exception as e:
        logger.warning(e)
        for fio in list_fio_emails_dolznost:
            result_fio_emails_dolznost.append(fio)
    logger.info(result_fio_emails_dolznost)
    if not fio_emails_dolznost:
        logger.info(f'Not email for {service}')

    return result_fio_emails_dolznost


def insert_send_zabbix(problem_id, host, problem, created, status, opdata=None, id_trigger=None, tags=None,
                       host_name=None, name='inside'):
    sql_query2 = '''INSERT INTO [send_zabbix_tg] (id, prioryti, id_tg, host, problem, start_date, status_tg, fio,
            email, service, dolzhnost, opdata, host_name, resend_date, zni_link, post) 
            VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, GETDATE(), ?, ?)'''
    service = check_service_in_new_table(host, problem, id_trigger)
    send_role = None
    logger.info(f'Service: {service}, {problem_id, host, problem, created, status, opdata, id_trigger, tags, host_name}')
    if not service[0]:
        if tags:
            send_role = ''.join(tags) + ';Менеджер'
            fio_emails_dolzhnost, service = take_email_from_host(host, send_role, name=name)
        else:
            fio_emails_dolzhnost, service = take_email_from_host(host, name=name)
    else:
        service, send_role = service[0], service[1]
        # fio_emails = take_fio_emails_from_service(service[0])
        fio_emails_dolzhnost = take_fio_emails_from_service(service, send_role)
    # print(fio_emails_dolzhnost)
    # return None
    logger.info(f'{fio_emails_dolzhnost, service}')

    new_fio_emails_tgid = []
    try:
        if send_role:
            for i in fio_emails_dolzhnost:
                logger.info(i)
                fio_emails_tgid = [i[0], i[1], take_telegram_id(i[1]), i[2], i[3], i[4]]
                new_fio_emails_tgid.append(fio_emails_tgid)
        else:
            for i in fio_emails_dolzhnost:
                fio_emails_tgid = [i[0], i[1], take_telegram_id(i[1]), i[2], i[3]]
                new_fio_emails_tgid.append(fio_emails_tgid)
    except Exception as e:
        logger.error(f'{e}\n {fio_emails_dolzhnost, send_role}')
    logger.info(f'{fio_emails_dolzhnost, service}')
    if not fio_emails_dolzhnost or not service:
        logger.info(f"not_find_service: {problem_id, host, problem, created, fio_emails_dolzhnost, service, send_role}")
        not_find_service(problem_id, host, problem, created)
        return None

    result = []
    i = 0
    logger.info(f'Send message {problem_id}: {new_fio_emails_tgid}')
    zni = check_zni(service, host, name)
    for fio_emails_tgid in new_fio_emails_tgid:
        logger.debug(f'ggjndflgdfjgdfj{fio_emails_tgid}')
        try:
            if fio_emails_tgid[5] == 'Не отправлять':
                status_tg = 'Не отправлять'
            else:
                status_tg = 'Не отправлен в тг'
            result.append((problem_id, status, fio_emails_tgid[2], host, problem, created, status_tg,
                           fio_emails_tgid[0], fio_emails_tgid[1], service, f'{fio_emails_tgid[3]};{fio_emails_tgid[5]}',
                           opdata, host_name, zni, fio_emails_tgid[4][0:130]))
        except Exception as e:
            if fio_emails_tgid[4]:
                result.append((problem_id, status, fio_emails_tgid[2], host, problem, created, 'Не отправлен в тг',
                               fio_emails_tgid[0], fio_emails_tgid[1], service, fio_emails_tgid[3], opdata, host_name, zni,
                               fio_emails_tgid[4][0:130]))
        i += 1
    logger.info(f'{result}')

    conn_string = f'SERVER={SERVER_SCSM};DATABASE={DB_GRAFANA};DRIVER={DRIVER};UID={USER_GRAFANA};PWD={PASSWORD_GRAFANA}'
    try:
        with pyodbc.connect(conn_string) as conn:
            with conn.cursor() as cur:
                for i in result:
                    logger.info(i)
                    cur.execute(sql_query2, i)
                conn.commit()
    except Exception as e:
        logger.error(e)


def update_problem(problem_id, end_date, duration, name='inside'):
    try:
        end_date = 10800 + int(end_date)
        end_date = datetime.utcfromtimestamp(int(end_date)).strftime('%Y-%m-%d %H:%M:%S')
    except (TypeError, ValueError):
        pass
    except Exception as e:
        logger.error(f'{problem_id}: {e}')
    logger.debug('%s %s %s', str(problem_id), str(end_date), str(duration))
    conn_string = f'SERVER={SERVER_GLSHP};DATABASE={DB_MONITORING};DRIVER={DRIVER};UID={USER_MONITORING};PWD={PASSWORD_MONITORING}'
    sql_query1 = f'''UPDATE [zabbix_{name}]
                   SET end_date = ?, duration = ?
                   WHERE id = ? '''
    sql_query2 = f'''UPDATE [monitoring_app_alert]
                   SET [end_date] = ?, duration = ?
                   WHERE id_alert = ?'''
    try:
        with pyodbc.connect(conn_string) as conn:
            with conn.cursor() as cur:
                cur.execute(sql_query1, (end_date, duration, problem_id))
                cur.execute(sql_query2, (end_date, duration, problem_id))
                conn.commit()

        conn_string = f'SERVER={SERVER_SCSM};DATABASE={DB_GRAFANA};DRIVER={DRIVER};UID={USER_GRAFANA};PWD={PASSWORD_GRAFANA}'
        sql_query3 = f''' UPDATE [scsm-dwm].[Grafana].[dbo].[send_zabbix_tg]
                            SET [end_date] = ?
                            WHERE id = ? '''
        with pyodbc.connect(conn_string) as conn:
            with conn.cursor() as cur:
                cur.execute(sql_query3, (end_date, problem_id))
                conn.commit()

    except Exception as e:
        logger.error(e)
        logger.error(f'{sql_query1}')
        time.sleep(50)
        update_problem(problem_id, end_date, duration, name=name)


def select_count_message(problem_id, name):
    conn_string = f'SERVER={SERVER_GLSHP};DATABASE={DB_MONITORING};DRIVER={DRIVER};UID={USER_MONITORING};PWD={PASSWORD_MONITORING}'
    res = (problem_id, )
    sql_query = f'''SELECT COUNT(*) FROM [zabbix_{name}_message] with (nolock) WHERE id = ?'''
    try:
        with pyodbc.connect(conn_string) as conn:
            with conn.cursor() as cur:
                count_message = cur.execute(sql_query, res).fetchone()[0]
                logger.debug('%s: %s', str(problem_id), str(count_message))
                return count_message
    except Exception as e:
        logger.error(e)
        return 0


def insert_message_info(problem_id, message_text, message_date, message_creator, name='inside'):
    message_date = 10800 + int(message_date)
    message_date = datetime.utcfromtimestamp(int(message_date)).strftime('%Y-%m-%d %H:%M:%S')
    try:

        message_text = message_text.strip()
        if message_creator:
            message_creator = message_creator.strip()
        else:
            message_creator = ''
        res = problem_id, message_text.strip(), message_date, message_creator
        logger.info('%s %s %s %s', problem_id, message_text, message_date, message_creator)
        conn_string = f'SERVER={SERVER_GLSHP};DATABASE={DB_MONITORING};DRIVER={DRIVER};UID={USER_MONITORING};PWD={PASSWORD_MONITORING}'
        sql_query = f'''INSERT INTO [zabbix_{name}_message] (id, message, time, name_user) VALUES(?, ?, ?, ?)'''
        with pyodbc.connect(conn_string) as conn:
            with conn.cursor() as cur:
                cur.execute(sql_query, res)
                conn.commit()
    except Exception as e:
        logger.error(e)
        logger.error(f'{problem_id}, {message_text}, {message_date}, {message_creator}')


def delete_message(problem_id, name):
    res = (problem_id, )
    conn_string = f'SERVER={SERVER_GLSHP};DATABASE={DB_MONITORING};DRIVER={DRIVER};UID={USER_MONITORING};PWD={PASSWORD_MONITORING}'
    sql_query = f''' DELETE FROM [zabbix_{name}_message] WHERE id = ?'''
    try:
        with pyodbc.connect(conn_string) as conn:
            with conn.cursor() as cur:
                cur.execute(sql_query, res)
                conn.commit()
                logger.debug(f'Delete message: {problem_id} in table zabbix_{name}')
    except Exception as e:
        logger.error(e)


def update_delete_info_monitoring(problem_id, end_date):
    try:
        conn_string = f'SERVER={SERVER_GLSHP};DATABASE={DB_MONITORING};DRIVER={DRIVER};UID={USER_MONITORING};PWD={PASSWORD_MONITORING}'

        with pyodbc.connect(conn_string) as conn:
            with conn.cursor() as cur:
                sql_query = f'''UPDATE [monitoring_app_alert]
                                   SET [end_date] = '{end_date}', duration = NULL
                                   WHERE id_alert = '{problem_id}' '''
                logger.info(sql_query)
                cur.execute(sql_query)
                conn.commit()
                logger.info(f'Delete: {problem_id} in table monitoring_app_alert')

    except Exception as e:
        logger.error(e)


def update_delete_info_send_zabbix(problem_id, end_date):
    try:
        conn_string = f'SERVER={SERVER_SCSM};DATABASE={DB_GRAFANA};DRIVER={DRIVER};UID={USER_GRAFANA};PWD={PASSWORD_GRAFANA}'
        with pyodbc.connect(conn_string) as conn:
            with conn.cursor() as cur:
                sql_query = f'''UPDATE [send_zabbix_tg]
                               SET [end_date] = '{end_date}'
                               WHERE id = '{problem_id}' '''
                logger.info(sql_query)
                cur.execute(sql_query)
                conn.commit()
    except Exception as e:
        logger.error(e)


def update_delete_info(problem_id, end_date, name='inside'):
    if type(end_date) == str or type(end_date) == int:
        end_date = 10800 + int(end_date)
        end_date = datetime.utcfromtimestamp(int(end_date)).strftime('%Y-%m-%d %H:%M:%S')
    conn_string = f'SERVER={SERVER_GLSHP};DATABASE={DB_MONITORING};DRIVER={DRIVER};UID={USER_MONITORING};PWD={PASSWORD_MONITORING}'
    sql_query = f'''UPDATE [zabbix_{name}]
                   SET end_date = '{end_date}' WHERE id = '{problem_id}' '''
    logger.info(sql_query)
    try:
        with pyodbc.connect(conn_string) as conn:
            with conn.cursor() as cur:
                cur.execute(sql_query)
                conn.commit()
        logger.info(f'Delete: {problem_id} in table zabbix_{name}')
        update_delete_info_monitoring(problem_id, end_date)
        update_delete_info_send_zabbix(problem_id, end_date)
    except Exception as e:
        logger.error(e)


def select_visible(name='inside'):
    conn_string = f'SERVER={SERVER_GLSHP};DATABASE={DB_MONITORING};DRIVER={DRIVER};UID={USER_MONITORING};PWD={PASSWORD_MONITORING}'
    sql_query = f'''SELECT [id]
          ,[visible]
        FROM [zabbix_{name}] with (nolock)''' # where start_date >= '2023-08-29' AND end_date is NULL'''  # AND host IS NOT NULL'''
    # FROM [Grafana].[dbo].[zabbix] where start_date >= '2023-01-01' AND end_date is NULL AND host IS NOT NULL''' # AND id = 485774486 or id = 485781473

    try:
        with pyodbc.connect(conn_string) as conn:
            with conn.cursor() as cur:
                info = cur.execute(sql_query).fetchall()
                info = convert_dict(info)
                return info
    except Exception as e:
        logger.error(e)
        info = []
    return info


def update_visible(problem_id, visible, name='inside'):
    print('update_problem')

    # prinT((problem_id, message_date, message_creator, message_text))
    print(problem_id, visible)

    #    conn_string = f'SERVER={SERVER_SCSM};DATABASE={DB_GRAFANA};DRIVER={DRIVER};UID={USER_GRAFANA};PWD={PASSWORD_GRAFANA}'

    # conn = pyodbc.connect(conn_string)
    # sql_query = f'''UPDATE [zabbix]
    #                SET visible = '{visible}' WHERE id = '{problem_id}' '''
    # # print(sql_query)
    # cur = conn.cursor()
    # cur.execute(sql_query)
    # conn.commit()
    # conn.close()


def select_id_today(name='inside'):
    conn_string = f'SERVER={SERVER_GLSHP};DATABASE={DB_MONITORING};DRIVER={DRIVER};UID={USER_MONITORING};PWD={PASSWORD_MONITORING}'

    # conn = pyodbc.connect(conn_string)

    # if len(str(time)) < 2:
    #     time = '0'+str(time)
    today = datetime.today()
    today = today.strftime('%Y') + '-' + today.strftime('%m') + '-' + today.strftime('%d')
    sql_query = f'''SELECT TOP (100) [id]
       
       FROM [zabbix_{name}] with (nolock) where start_date >= '{today}' ORDER BY start_date DESC'''  # start_date >= '2023-08-29' AND '''# AND host IS NOT NULL'''

    try:
        with pyodbc.connect(conn_string) as conn:
            with conn.cursor() as cur:
                info = cur.execute(sql_query).fetchall()
                return info
    except Exception as e:
        logger.error(e)
        return []


def update_delete_info_monitoring_all(info):
    if not info:
        return None
    conn_string = f'SERVER={SERVER_GLSHP};DATABASE={DB_MONITORING};DRIVER={DRIVER};UID={USER_MONITORING};PWD={PASSWORD_MONITORING}'
    try:
        with pyodbc.connect(conn_string) as conn:
            with conn.cursor() as cur:
                for i in info:
                    logger.info(i)
                    problem_id, end_date = i[0], i[1]
                    sql_query = f'''UPDATE [monitoring_app_alert]
                                       SET [end_date] = {end_date}, duration = NULL
                                       WHERE id_alert = '{problem_id}' '''

                    cur.execute(sql_query)
                conn.commit()
    except Exception as e:
        logger.error(e)


def update_deleted_monitoring(name='inside'):
    # conn_string = f'SERVER={SERVER_SCSM};DATABASE={DB_GRAFANA};DRIVER={DRIVER};UID={USER_GRAFANA};PWD={PASSWORD_GRAFANA}'
    conn_string = f'SERVER={SERVER_GLSHP};DATABASE={DB_MONITORING};DRIVER={DRIVER};UID={USER_MONITORING};PWD={PASSWORD_MONITORING}'
    sql_query = f'''SELECT  [id]
      ,[end_date]
  FROM [zabbix_{name}] where [duration] is Null AND end_date is not NULL order by start_date desc '''
    try:
        with pyodbc.connect(conn_string) as conn:
            with conn.cursor() as cur:
                info = cur.execute(sql_query).fetchall()
                logger.info(info)
                update_delete_info_monitoring_all(info)
    except Exception as e:
        logger.error(e)


def select_service_id(host):
    if host.count('-') == 2:
        logger.info(host)
        host = host[host.find('-')+1:host.rfind('-')]
    res = (host, )
    logger.info(res)
    conn_string = f'SERVER={SERVER_GLSHP};DATABASE={DB_MONITORING};DRIVER={DRIVER};UID={USER_MONITORING};PWD={PASSWORD_MONITORING}'
    sql_query = '''SELECT [id],[code],[name],[prefix] 
    FROM [dw-it4it-prod].[DataWarehouse].[dbo].[Target_Abdt_Abbm_ITService]
        where prefix = ?
                   '''
    try:
        with pyodbc.connect(conn_string) as conn:
            with conn.cursor() as cur:
                info = cur.execute(sql_query, res).fetchall()
                logger.info(info)
                service_id = (info[0][0], )
                sql_query = '''SELECT [service_id]
                  ,[service_code]
                  ,[service_name]
                  ,[type_name]
                  ,[employee_fullname]
                  ,[mobile_phone]
              FROM [dw-it4it-prod].[DataWarehouse].[dbo].[Target_Abdt_Abbm_ITServiceResponsible]
            where service_id = ?
              and [responsible_active] = 1'''
                info = cur.execute(sql_query, service_id).fetchall()
                logger.info(info)
                dict_name_phone = take_name_phone(info)
                logger.info(dict_name_phone)
                return info
    except Exception as e:
        logger.error(e)
        return []


def select_time_begin(name):
    conn_string = f'SERVER={SERVER_GLSHP};DATABASE={DB_MONITORING};DRIVER={DRIVER};UID={USER_MONITORING};PWD={PASSWORD_MONITORING}'
    # if len(str(time)) < 2:
    #     time = '0'+str(time)
    sql_query = f'''SELECT [id]
      ,[end_date]
      ,[start_date]
    FROM [zabbix_{name}] where end_date is NULL '''#start_date >= '2023-08-29' AND '''# AND host IS NOT NULL'''
    try:
        with pyodbc.connect(conn_string) as conn:
            with conn.cursor() as cur:
                info = cur.execute(sql_query).fetchall()
                info = convert_dict2(info)
                return info
    except Exception as e:
        logger.error(e)
        return {}


def test_update():
    a =   '''CM-4198
Сервисы: Виртуализация ESXi
Затронутые:
Время начала: 08:30:00
Время окончания: 17:30:00
Остановка сервиса: Нет
Описание: 1.Вывод гипервизора в обслуживание.
Вывод из кластера. Отключение луна BootFromSAN на
схд NetApp.2.Установка esxi на локальный
диск.3.Настройка esxi, ввод в кластер. Ожидание
миграции на него ВМ.4.После другой гипер по
пунктам 1 - 3.
;CM-4211
Сервисы: Виртуализация ESXi
Затронутые:
Время начала: 08:31:00
Время окончания: 23:19:00
Остановка сервиса: Нет
Описание: 1.Вывод гипервизора в обслуживание.
Вывод из кластера. Отключение луна BootFromSAN на
схд NetApp.2.Установка esxi на локальный
диск.3.Настройка esxi, ввод в кластер. Ожидание
миграции на него ВМ.4.После другой гипер по
пунктам 1 - 3.
;'''
    sql_query = '''update monitoring_app_alert set zni_link = ? where id_alert = 842780304'''
    try:
        conn_string = f'SERVER={SERVER_GLSHP};DATABASE={DB_MONITORING};DRIVER={DRIVER};;UID={USER_MONITORING};PWD={PASSWORD_MONITORING}'
        with pyodbc.connect(conn_string) as conn:
            with conn.cursor() as cur:

                cur.execute(sql_query, (a, ))
                sql_query = '''update monitoring_app_alert set zni_link = ? where id_alert = 842780304'''
                cur.execute(sql_query, (a,))
                conn.commit()

    except Exception as e:
        logger.error(e)



if __name__ == '__main__':
    print(check_zni('Аналитический слой данных Блока Риски'))
    # print(take_status_service_from_service('Diasoft Flextera - AML1'))
    # print(check_zni('Сопровождение СУБД MSSQL (Промышленный)', 'P-CBREV-DB01'))
    print(take_new_service_without_db('P-RSKDT-DB01'))
    # print(take_new_service_without_db('pc-abo-c403'))
    host = 'pc-abo-c403'
    problem = 'Недоступен порт 1433 на pc-abo-list01.card.akbars.ru (10.150.36.101)'
    # print(check_service_in_new_table(host, problem))
    # dict_all_service = take_service_name()
    # print(dict_all_service[host][0])
    # print(take_email_from_host(host))
    # service = 'Антифрод Smart Fraud Detection'
    # tags = {'SA'}
    # send_role = ''.join(tags) + ';Менеджер'
    # host = 'P-cardj-mil01'
    # # print(take_fio_emails_from_service(service, send_role))
    # # print(check_service_in_new_table())
    # fio_emails_dolzhnost, service = take_email_from_host(host, send_role)
    # print(fio_emails_dolzhnost)
    # print(select_problem_id(date_from='2025-03-03', name='inside'))



    # print(check_zni('Way4 (Lekton Classic)'))
    # test_update()
    # a = take_info_zni('Виртуализация ESXi')
    # print(a)
    # c = a.split(';')
    # print(c)
    # for i in c:
    #     # i.split('\n')
    #     print(i.split('\n')[0])
    # # print(take_email_from_host('p-astx-nfs01'))
    # print(len('CM-4198\nВиртуализация ESXi\n\n2025-04-14 08:30:00\n2025-04-19 17:30:00\nНет\n1.Вывод гипервизора в обслуживание. Вывод из кластера. Отключение луна BootFromSAN на схд NetApp.2.Установка esxi на локальный диск.3.Настройка esxi, ввод в кластер. Ожидание миграции на него ВМ.4.После другой гипер по пунктам 1 - 3.\n;CM-4211\nВиртуализация ESXi\n\n2025-04-14 08:31:00\n2025-04-19 23:19:00\nНет\n1.Вывод гипервизора в обслуживание. Вывод из кластера. Отключение луна BootFromSAN на схд NetApp.2.Установка esxi на локальный диск.3.Настройка esxi, ввод в кластер. Ожидание миграции на него ВМ.4.После другой гипер по пунктам 1 - 3.\n'))
