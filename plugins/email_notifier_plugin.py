from airflow.plugins_manager import AirflowPlugin

import email_notifier_listeners

class EmailNotifierPlugin(AirflowPlugin):
    name = "email_notifier_plugin"
    listeners = [email_notifier_listeners]