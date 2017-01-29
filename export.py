#!/usr/bin/env python

import datetime
import json
import pprint
import os
import boto3
import requests
from jinja2 import Template
from Queue import Queue
from threading import Thread

BUCKET_NAME = 'drop.nullis.net'

f = open('files.json', 'r')

drops = json.loads(f.read())

output_dir = 'output'

pp = pprint.PrettyPrinter(indent=4)


def worker():
    session = boto3.session.Session()
    s3 = session.resource('s3',aws_access_key_id='<access key>', aws_secret_access_key='<secret key>')

    template_data = open('template.html', 'r').read()
    template = Template(template_data)

    while True:
        item = q.get()
        process_drop(item, s3, template)
        q.task_done()

def process_drop(drop, s3, template):
    result = {}
    result['title'] = drop['title']
    result['index_path'] = '%s/%s' % (drop['code'], drop['password'])
    result['url'] = 'https://%s/%s' % ('drop.nullis.net', result['index_path'])
    result['file_size'] = drop['size']
    result['unixdate'] = drop['created_at']/1000.0
    result['date'] = datetime.datetime.fromtimestamp(result['unixdate']).strftime('%d %B %Y at %H:%M:%S')
    result['dl'] = drop['full_src']

    if drop['is_note']:
        result['preview'] = ''
        result['note'] = True
        result['title'] = 'Saved Note'
        result['file_path'] = '%s/%s/%s' % (drop['code'], drop['password'], 'note.html')
        result['file_url'] = 'https://%s/%s' % ('drop.nullis.net', result['file_path'])
        result['file_relative'] = './%s/%s' % (drop['password'], 'note.html')
        result['content_type'] = 'text/html'
        result['file'] = '<html><body><pre>%s</pre></body></html>' % drop['content']

        #pp.pprint(result)
        #pp.pprint(drop)
    elif drop['type'] == 'link':
        result['preview'] = ''
        result['link'] = drop['content']
    else:
        result['file_path'] = '%s/%s/%s' % (drop['code'], drop['password'], result['title'])
        result['file_url'] = 'https://%s/%s' % ('drop.nullis.net', result['file_path'])
        result['file_relative'] = './%s/%s' % (drop['password'], result['title'])

        if not drop['type'] == 'file':
            result['preview'] = '<div class="imagePreview"><img src="%s" alt="Preview" class="img-thumbnail" /></div>' % result['file_relative']

        print result['dl']
        r = requests.get(result['dl'], stream=True)
        r.raw.decode_content = True
        result['file']  = r.raw.read()
        result['content_type'] = r.headers['Content-Type']


    if not drop['type'] == 'link':
        s3.Object(BUCKET_NAME, result['file_path']).put(Body=result['file'], ContentType=result['content_type'], CacheControl='max-age=31536000')
        output = template.render(**result)
        s3.Object(BUCKET_NAME, result['index_path']).put(Body=output, ContentType='text/html', CacheControl='max-age=31536000')
    else:
        s3.Object(BUCKET_NAME, result['index_path']).put(WebsiteRedirectLocation=result['link'])

q = Queue()
for i in range(100):
     t = Thread(target=worker)
     t.daemon = True
     t.start()

for drop in drops:
    q.put(drop)

q.join()       # block until all tasks are done

