# from google.cloud import bigquery
import os
import pandas_gbq
import time


while True:
    time_now = time.strftime("%H:%M:%S", time.localtime())  # 刷新
    if time_now == "00:30:00":  # 此处设置每天定时的时间

        os.environ[
            "GOOGLE_APPLICATION_CREDENTIALS"
        ] = "/Users/shoplazza/Downloads/**.json"
        # 此处填写json存放的地址

        # gcp_project = "admin-account-293313"
        # bq_dataset = "DIM"
        #
        # client = bigquery.Client(project=gcp_project)
        # dataset_ref = client.dataset(bq_dataset)
        #
        # def gcp2df(sql):
        #     query = client.query(sql)
        #     results = query.result()
        #     return results.to_dataframe()
        #
        # query = """SELECT *
        #             FROM admin-account-293313.DIM.currenc_exchange_rate_zm
        #             LIMIT 1000"""
        #
        # df = gcp2df(query)

        project_id = "admin-account-293313"

        # 填写project 的名称

        sql = """SELECT *
                 FROM admin-account-293313.DIM.***
                 LIMIT 1000"""

        # 用sql语法写想读取的数据

        df = pandas_gbq.read_gbq(sql, project_id=project_id, progress_bar_type=None)

        # 将sql写到DataFrame格式，接下来可以根据自己需求用python处理数据

        # 自定义处理数据处

        pandas_gbq.to_gbq(
            df,
            "my_dataset.my_table",
            project_id=projectid,
            if_exists="replace",
        )

        # 填写dataset, table, project的名称，如果是替换之前的表格if_exists填写'replace'，
        # 如果是新的表的填写''replace'', 如果是添加数据填写'append'。

        time.sleep(2)  # 因为以秒定时，所以暂停2秒，使之不会在1秒内执行多次
