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
with open('qinghai_lake.jpg', 'rb') as fh:
    report.report_file.put(fh, content_type='img/jpeg')
report.save()

# 读取文件
report = ReportFile.objects(filename=filename).first()
if not report:
    print 'Report file not found in the DB!'
    exit(1)
# 对应的fs file id
fid = report.report_file._id
report_file = report.report_file.read()
with open('retr_img.jpg', 'w') as retr_fh:
    retr_fh.write(report_file)
conn.disconnect()

# 使用pymongo来查询对应的fs files和fs chunks
conn = Connection('localhost', 27017)
db = conn.test
fs_file = db.fs.files.find_one({'_id': fid})
if not fs_file:
    print 'The fs file does not exist in collection fs.files!'
    exit(1)
print 'The fs file id is \'%s\'' % fs_file['_id']
chunk_count = db.fs.chunks.find({'files_id': fid}).count()
if chunk_count == 0:
    print 'There is no any fs chunk for the stored large file!'
    exit(1)
print 'There are %d fs chunks for the stored large file.' % chunk_count

# drop collection
db.drop_collection('TestFile')
db.drop_collection('fs.files')
db.drop_collection('fs.chunks')
