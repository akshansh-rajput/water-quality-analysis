"""Aproval interface for the Deployment Jobs"""

# Web framework

from flask import Blueprint, request, render_template, redirect
from flask_appbuilder import ModelView, expose, BaseView as AppBuilderBaseView

# airflow plugins

from airflow.www.views import AirflowBaseView
from airflow.plugins_manager import AirflowPlugin
import logging, os
from confluent_kafka.admin import AdminClient


approval_bp = Blueprint(
    "kafka_control", 
    __name__,
    template_folder='templates',
    # static_folder='static/kafka',
    # static_url_path='/admin/kafka/static',
    url_prefix='/kafka/kafka_control',
)
  
class KafkaView(AppBuilderBaseView):
    default_view = "kafka"

    route_base = "/kafka/list"

    @expose("/")
    def kafka(self):
        conf = {'bootstrap.servers':'broker:29092'}
        admin = AdminClient(conf)
        topic_dict = admin.list_topics().topics
        topics = []

        for topic_name in topic_dict:
           topic_partition = len(topic_dict[topic_name].partitions)
           topic_dic = {
               'name': topic_name,
               'partition': topic_partition
           }
           topics.append(topic_dic)

        return self.render_template(
            "deployments.html", 
            topics=topics
        )

my_kafka_view = KafkaView()



kafka_view_package = {
    "name": "kafka",
    "category": "kafka",
    "view": my_kafka_view,
}


class AirflowTestPlugin(AirflowPlugin):
    name = "Deployment UI"
  
    flask_blueprints = [ approval_bp ]
    appbuilder_views = [ kafka_view_package ]

