import json

__author__ = 'patmue'
import beanstalkc



# Connection
beanstalk = beanstalkc.Connection(host='localhost', port=11300)
beanstalk.use('url_list')
jsonobject = json.dumps({"domain": "http://www.x303.de/"})
beanstalk.put(jsonobject)

