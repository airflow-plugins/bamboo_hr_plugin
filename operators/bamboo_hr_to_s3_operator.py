from bamboo_hr_plugin.hooks.bamboo_hr_hook import BambooHRHook
from airflow.utils.decorators import apply_defaults
from airflow.models import BaseOperator
from airflow.hooks import S3Hook
from os import path
import boa
import json


class BambooHRToS3Operator(BaseOperator):
    """
      S3 To Redshift Operator
          :param bamboo_conn_id:          The destination redshift connection id.
          :type bamboo_conn_id:           string

          :param company name:            The company data is being requested for
                                          - goes before the endpoint in the URL.
          :type redshift_schema:          string

          :param method:                  The endpoint from which data is being
                                          requested.
          :type table:                    string

          :param s3_conn_id:              The source s3 connection id.
          :type s3_conn_id:               string

          :param s3_bucket:               The source s3 bucket.
          :type s3_bucket:                string

          :param s3_key:                  The source s3 key.
          :type s3_key:                   string

          :param payload:                 Additional parameters you're making in
                                          the request.
          :type payload:                  string

      """
    template_fields = ('s3_key')

    @apply_defaults
    def __init__(self,
                 bamboo_conn_id,
                 company_name,
                 method,
                 s3_conn_id,
                 s3_bucket,
                 s3_key,
                 payload=None,
                 *args,
                 **kwargs):
        super().__init__(*args, **kwargs)
        self.bamboo_conn_id = bamboo_conn_id
        self.company_name = company_name
        self.method = method
        self.s3_conn_id = s3_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.payload = payload

    def methodMapping(self, method):
        mapping = {
            "getJobInfo": "employees/all/tables/jobInfo/",
            "getEmployeeDirectory": "employees/directory/",
            "getEmploymentStatus":
            "employees/all/tables/employmentStatus/"
        }

        return mapping[method]

    def schemaMapping(self, fields):
        schema = {}
        for field in fields:
            if field['type'] == 'bool':
                schema[boa.constrict(field['id'])] = 'bool'
            else:
                schema[boa.constrict(field['id'])] = 'varchar'
            schema['id'] = 'int'

        return schema

    def execute(self, context):
        response = (BambooHRHook(self.bamboo_conn_id)
                    .run(self.company_name,
                         self.methodMapping(self.method),
                         self.payload)).text

        results = json.loads(response)
        s3 = S3Hook(s3_conn_id=self.s3_conn_id)

        if self.s3_key.endswith('.json'):
            split = path.splitext(self.s3_key)
            schema_key = '{0}_schema{1}'.format(split[0], split[1])
"""
  S3 To Redshift Operator
      :param bamboo_conn_id:          The destination redshift connection id.
      :type bamboo_conn_id:           string

      :param company name:            The company data is being requested for
                                      - goes before the endpoint in the URL.
      :type redshift_schema:          string

      :param method:                  The endpoint from which data is being
                                      requested.
      :type table:                    string

      :param s3_conn_id:              The source s3 connection id.
      :type s3_conn_id:               string

      :param s3_bucket:               The source s3 bucket.
      :type s3_bucket:                string

      :param s3_key:                  The source s3 key.
      :type s3_key:                   string

      :param payload:                 Additional parameters you're making in
                                      the request.
      :type payload:                  string

  """

        if self.method == 'getEmployeeDirectory':
            schema = self.schemaMapping(results.get('fields', []))
            results = results.get('employees')

        elif self.method == 'getJobInfo':
            schema = {
                "id": "int",
                "employee_id": "int",
                "date": "date",
                "location": "varchar",
                "division": "varchar",
                "department": "varchar",
                "job_title": "varchar",
                "reports_to": "varchar",
            }

        elif self.method == 'getEmploymentStatus':
            schema = {
                "id": "int",
                "employee_id": "int",
                "date": "date",
                "employment_status": "varchar",
                "benetrac_status": "varchar",
                "gusto": "varchar",
            }

        results = [dict([boa.constrict(k), v]
                        for k, v in i.items()) for i in results]
        results = '\n'.join([json.dumps(i) for i in results])

        s3.load_string(
            string_data=json.dumps(schema),
            bucket_name=self.s3_bucket,
            key=schema_key,
            replace=True
        )

        s3.load_string(
            string_data=results,
            bucket_name=self.s3_bucket,
            key=self.s3_key,
            replace=True
        )

        s3.connection.close()
