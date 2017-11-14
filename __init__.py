from airflow.plugins_manager import AirflowPlugin
from bamboo_hr_plugin.hooks.bamboo_hr_hook import BambooHRHook
from bamboo_hr_plugin.operators.bamboo_hr_to_s3_operator import BambooHRToS3Operator


class BambooHRPlugin(AirflowPlugin):
    name = "bamboohr_plugin"
    operators = [BambooHRToS3Operator]
    hooks = [BambooHRHook]
