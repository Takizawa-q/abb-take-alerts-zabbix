import datetime

from config import config
from jira import JIRA, Issue

class NoIssueException(Exception):

    def __init__(self, message="Не существует такого UIN"):
        self.message = message
        super().__init__(self.message)


import json


def parse_datetime_time(now):
    return f"{now.day}-{now.month}-{now.year} {now.hour}:{now.minute}"


class JiraAPI:

    def __init__(self):
        self.jira = JIRA("https://team.akbars.ru",
                         basic_auth=("s-monitoringHD",
                                     "M0nit0ring!_123"))
    def get_cm(self, cm: str | int) -> Issue | None:
        try:
            iss: Issue = self.jira.issue(f"{cm}")
            return iss
        except Exception as e:
            return None
    def get_uin(self, uin: str | int) -> Issue | None:
        try:
            iss: Issue = self.jira.issue(f"{uin}")
            return iss
        except Exception as e:
            return None

    def get_date_end_and_priority(self, uin: str | int):

        u = self.get_uin(uin)
        wait_time = u.get_field("customfield_12410")
        priority = u.get_field("customfield_12402")
        description = u.get_field("customfield_12204")
        date_end = u.get_field("customfield_12409")

        title = u.raw["fields"]["summary"]
        wait_time = datetime.datetime.fromisoformat(wait_time).replace(tzinfo=None) if wait_time else "None"
        date_end = datetime.datetime.fromisoformat(date_end).replace(tzinfo=None) if date_end else "None"
        priority = priority if priority else "None"

        return wait_time, date_end, priority, title, description



def get_jira_obj():
    return JiraAPI()


jira = get_jira_obj()