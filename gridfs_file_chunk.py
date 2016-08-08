#-*- coding: utf-8 -*-

import datetime
from mongoengine import document, fields, connect
from pymongo import Connection

class ReportFile(document.DynamicDocument):
    """
    报告文件模型
    """

    # 报告的文件名
    filename = fields.StringField(primary_key=True)
    # 报告的生成日期：
    date = fields.DateTimeField()
    # 报告文件
    report_file = fields.FileField(required=True)

    meta = {
        'collection': 'TestFile'
    }

# 连接本地数据库
conn = connect('test', host='localhost', port=27017)

# 使用mongoengine GridFS存文件
filename = 'a pic'
report = ReportFile(filename=filename, datetime=datetime.datetime.now())
fh = open('qinghai_lake.jpg', 'rb')
report.report_file.put(fh, content_type='img/jpeg')
report.save()

# 读取文件
report = ReportFile.objects(filename=filename).first()
report_file = report.report_file.read()
with open('retr_img.jpg', 'w') as retr_fh:
    retr_fh.write(report_file)
conn.disconnect()

conn = Connection('localhost', 27017)
db = conn.test
for each_file in db.fs.files.find():
    print each_file
print db.fs.chunks.find().count()
db.drop_collection('TestFile')
db.drop_collection('fs.files')
db.drop_collection('fs.chunks')
